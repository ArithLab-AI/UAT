import logging
import os

from fastapi import APIRouter, Body, Depends, Request
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.schemas.csv_dataset_schema import (
    CsvDatasetListSuccessResponse,
    CsvMergedDatasetSuccessResponse,
    CsvUploadedDatasetListSuccessResponse,
    MergeCsvDatasetsRequest,
    MergeSourceDatasetsRequest,
    MergeSuggestionsSuccessResponse,
    PreviewMergeRequest,
    PreviewMergeSuccessResponse,
    SelectExcelSheetRequest,
)
from app.schemas.common_schema import MessageSuccessResponse
from app.services.csv_service import (
    build_dataset_name,
    count_user_active_datasets,
    create_uploaded_dataset,
    delete_merged_dataset,
    delete_uploaded_dataset,
    ParsedUpload,
    PendingSheetSelection,
    parse_csv_upload,
)
from app.services.excel_sheet_service import process_temporary_upload_selection
from app.services.merge_service import (
    get_ordered_uploaded_datasets,
    merge_uploaded_datasets,
)
from app.services.preview_service import preview_uploaded_dataset_merge
from app.services.suggestion_service import suggest_join_columns
from app.services.file_retention_service import (
    retention_dataset_for_user,
    set_dataset_retention_expiry,
)
from app.services.subscription_service import get_user_plan_capabilities
from app.services.temporary_upload_service import (
    delete_temporary_upload,
    validate_temporary_upload,
)
from app.utils.responses import error_response, success_response

UPLOAD_MULTIPLE_OPENAPI = {
    "requestBody": {
        "required": True,
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "required": ["files"],
                    "properties": {
                        "files": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "format": "binary",
                            },
                        }
                    },
                }
            }
        },
    }
}

router = APIRouter(prefix="/csv-datasets", tags=["CSV Datasets"])
logger = logging.getLogger(__name__)


def _get_file_size(file) -> int | None:
    if not hasattr(file, "file"):
        return None

    current_position = file.file.tell()
    file.file.seek(0, os.SEEK_END)
    file_size = file.file.tell()
    file.file.seek(current_position)
    return file_size


def _format_file_size_limit(limit_bytes: int | None) -> str:
    if limit_bytes is None:
        return "unlimited"
    return f"{limit_bytes // (1024 * 1024)} MB"


def _serialize_merged_dataset(merged_dataset, source_dataset_map=None):
    metadata = merged_dataset.source_datasets_metadata or []
    source_dataset_map = source_dataset_map or {}
    seen_source_ids = set()

    source_datasets = []
    for item in metadata:
        source_id = item["id"]
        if source_id in seen_source_ids:
            continue

        seen_source_ids.add(source_id)
        src = source_dataset_map.get(source_id)
        source_datasets.append(
            {
                "id": source_id,
                "file_name": src.file_name if src else item["file_name"],
                "sheet_name": src.sheet_name if src else item.get("sheet_name"),
            }
        )

    return {
        "id": merged_dataset.id,
        "name": merged_dataset.name,
        "table_name": merged_dataset.table_name,
        "storage_key": merged_dataset.storage_key,
        "file_url": merged_dataset.file_url,
        "file_size": merged_dataset.file_size,
        "total_rows": merged_dataset.total_rows,
        "columns": merged_dataset.columns,
        "created_at": merged_dataset.created_at,
        "source_datasets": source_datasets,
    }


@router.post(
    "/upload-multiple",
    response_model=CsvUploadedDatasetListSuccessResponse,
    response_model_exclude_none=True,
    status_code=201,
    openapi_extra=UPLOAD_MULTIPLE_OPENAPI,
)
async def upload_multiple_csv_datasets(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    form = await request.form()
    files = form.getlist("files")

    if not files:
        raise error_response(
            status_code=400,
            detail="At least one CSV, XLSX, or XLS file is required",
        )

    plan_capabilities = get_user_plan_capabilities(db, current_user)
    active_dataset_count = count_user_active_datasets(db, current_user.id)
    max_active_datasets = plan_capabilities["max_active_datasets"]

    if (
        max_active_datasets is not None
        and active_dataset_count + len(files) > max_active_datasets
    ):
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows up to {max_active_datasets} active datasets. "
                "Please delete an existing dataset or upgrade your plan."
            ),
        )

    parsed_uploads: list[ParsedUpload] = []
    pending_sheet_selections: list[PendingSheetSelection] = []
    for file in files:
        if not hasattr(file, "filename") or not hasattr(file, "read"):
            raise error_response(status_code=400, detail="Invalid file input")

        file_size = _get_file_size(file)
        max_file_size_bytes = plan_capabilities["max_file_size_bytes"]
        if (
            file_size is not None
            and max_file_size_bytes is not None
            and file_size > max_file_size_bytes
        ):
            raise error_response(
                status_code=400,
                detail=(
                    f"{file.filename} exceeds your current plan file size limit of "
                    f"{_format_file_size_limit(max_file_size_bytes)}. Please upgrade your plan."
                ),
            )

        parsed_upload = await parse_csv_upload(file, user_id=current_user.id)
        if isinstance(parsed_upload, PendingSheetSelection):
            pending_sheet_selections.append(parsed_upload)
            continue
        parsed_uploads.append(parsed_upload)

    created_datasets = []
    for parsed_upload in parsed_uploads:
        dataset_name = build_dataset_name(None, parsed_upload.file_name)
        dataset = create_uploaded_dataset(
            db,
            dataset_name=dataset_name,
            file_name=parsed_upload.file_name,
            sheet_name=parsed_upload.sheet_name,
            file_size=parsed_upload.file_size,
            columns=parsed_upload.columns,
            internal_columns=parsed_upload.internal_columns,
            rows=parsed_upload.rows,
            user_id=current_user.id,
        )
        set_dataset_retention_expiry(
            db=db,
            dataset=dataset,
            user_id=current_user.id,
        )
        created_datasets.append(dataset)

    db.commit()

    for dataset in created_datasets:
        db.refresh(dataset)

    if pending_sheet_selections:
        pending_files = [
            {
                "requires_sheet_selection": pending_upload.requires_sheet_selection,
                "file_token": pending_upload.file_token,
                "file_name": pending_upload.file_name,
                "available_sheets": pending_upload.available_sheets,
                "sheet_count": pending_upload.sheet_count,
                "preview_row_count": pending_upload.preview_row_count,
            }
            for pending_upload in pending_sheet_selections
        ]
        response_data = {
            "requires_sheet_selection": True,
            "pending_files": pending_files,
            "uploaded_datasets": created_datasets,
        }

        return success_response(
            "Sheet selection is required before upload can continue",
            data=response_data,
        )

    logger.info(
        "Created %s uploaded datasets for user_id=%s",
        len(created_datasets),
        current_user.id,
    )
    return success_response(
        "Uploaded datasets created successfully",
        status_code=201,
        data=created_datasets,
    )


@router.post(
    "/upload/select-sheet",
    response_model=CsvUploadedDatasetListSuccessResponse,
    status_code=201,
)
def select_excel_sheet_for_upload(
    payload: list[SelectExcelSheetRequest] = Body(
        ...,
        examples=[
            [
                {
                    "file_token": "lgNA820xPWbtX9EWXsxdTSjywBDQC93BmkJOgKWAFdU",
                    "sheet_name": "Sheet1",
                },
                {
                    "file_token": "4DIlvNYnvXolrdFcgoFnTk0H0d_k3Pcp3u9dcUjM9yQ",
                    "sheet_name": "Sheet2",
                },
            ]
        ],
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not payload:
        raise error_response(
            status_code=400,
            detail="At least one sheet selection is required",
        )

    plan_capabilities = get_user_plan_capabilities(db, current_user)
    active_dataset_count = count_user_active_datasets(db, current_user.id)
    max_active_datasets = plan_capabilities["max_active_datasets"]
    if (
        max_active_datasets is not None
        and active_dataset_count + len(payload) > max_active_datasets
    ):
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows up to {max_active_datasets} active datasets. "
                "Please delete an existing dataset or upgrade your plan."
            ),
        )

    created_datasets = []
    for selection in payload:
        temporary_upload = validate_temporary_upload(
            token=selection.file_token,
            user_id=current_user.id,
        )
        (
            file_name,
            file_size,
            columns,
            internal_columns,
            rows,
            sheet_name,
        ) = process_temporary_upload_selection(
            upload=temporary_upload,
            sheet_name=selection.sheet_name,
        )
        dataset = create_uploaded_dataset(
            db,
            dataset_name=build_dataset_name(None, file_name),
            file_name=file_name,
            sheet_name=sheet_name,
            file_size=file_size,
            columns=columns,
            internal_columns=internal_columns,
            rows=rows,
            user_id=current_user.id,
        )
        set_dataset_retention_expiry(
            db=db,
            dataset=dataset,
            user_id=current_user.id,
        )
        created_datasets.append(dataset)

    db.commit()

    for dataset in created_datasets:
        db.refresh(dataset)

    for selection in payload:
        delete_temporary_upload(selection.file_token)

    return success_response(
        "Uploaded datasets created successfully",
        status_code=201,
        data=created_datasets,
    )


@router.post("/merge/suggestions", response_model=MergeSuggestionsSuccessResponse)
def suggest_csv_dataset_merge(
    payload: MergeSourceDatasetsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plan_capabilities = get_user_plan_capabilities(db, current_user)
    if not plan_capabilities["can_merge"]:
        raise error_response(
            status_code=400,
            detail="Data merging is not available on your current plan. Please upgrade your plan.",
        )

    source_datasets = get_ordered_uploaded_datasets(
        db,
        source_ids=payload.source_dataset_ids,
        user_id=current_user.id,
    )
    return success_response(
        "Merge suggestions generated successfully",
        data=suggest_join_columns(source_datasets=source_datasets),
    )


@router.post("/merge/preview", response_model=PreviewMergeSuccessResponse)
def preview_csv_dataset_merge(
    payload: PreviewMergeRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plan_capabilities = get_user_plan_capabilities(db, current_user)
    if not plan_capabilities["can_merge"]:
        raise error_response(
            status_code=400,
            detail="Data merging is not available on your current plan. Please upgrade your plan.",
        )

    source_datasets = get_ordered_uploaded_datasets(
        db,
        source_ids=payload.source_dataset_ids,
        user_id=current_user.id,
    )
    return success_response(
        "Merge preview generated successfully",
        data=preview_uploaded_dataset_merge(
            source_datasets=source_datasets,
            merge_type=payload.merge_type,
            join_columns=payload.join_columns,
        ),
    )


@router.post("/merge", response_model=CsvMergedDatasetSuccessResponse, status_code=201)
def merge_csv_datasets(
    payload: MergeCsvDatasetsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plan_capabilities = get_user_plan_capabilities(db, current_user)
    if not plan_capabilities["can_merge"]:
        raise error_response(
            status_code=400,
            detail="Data merging is not available on your current plan. Please upgrade your plan.",
        )

    max_merge_sources = plan_capabilities["max_merge_sources"]
    if max_merge_sources is not None and len(payload.source_dataset_ids) > max_merge_sources:
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows merging up to {max_merge_sources} source datasets at a time. "
                "Please upgrade your plan."
            ),
        )

    active_dataset_count = count_user_active_datasets(db, current_user.id)
    max_active_datasets = plan_capabilities["max_active_datasets"]
    if max_active_datasets is not None and active_dataset_count + 1 > max_active_datasets:
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows up to {max_active_datasets} active datasets. "
                "Please delete an existing dataset or upgrade your plan."
            ),
        )

    source_datasets = get_ordered_uploaded_datasets(
        db,
        source_ids=payload.source_dataset_ids,
        user_id=current_user.id,
    )
    source_dataset_map = {dataset.id: dataset for dataset in source_datasets}

    merged_dataset = merge_uploaded_datasets(
        db,
        merged_name=payload.merged_name,
        source_datasets=source_datasets,
        user_id=current_user.id,
        merge_type=payload.merge_type,
        join_columns=payload.join_columns,
    )
    db.commit()
    db.refresh(merged_dataset)

    logger.info(
        "Merged %s uploaded datasets into merged_dataset_id=%s for user_id=%s",
        len(source_datasets),
        merged_dataset.id,
        current_user.id,
    )
    return success_response(
        "Datasets merged successfully",
        status_code=201,
        data=_serialize_merged_dataset(merged_dataset, source_dataset_map),
    )

@router.get("", response_model=CsvDatasetListSuccessResponse)
def list_csv_datasets(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    uploaded_datasets = (
        db.query(CsvUploadedDataset)
        .filter(CsvUploadedDataset.created_by_user_id == current_user.id)
        .order_by(CsvUploadedDataset.id.desc())
        .all()
    )
    merged_datasets = (
        db.query(CsvMergedDataset)
        .filter(CsvMergedDataset.created_by_user_id == current_user.id)
        .order_by(CsvMergedDataset.id.desc())
        .all()
    )
    source_dataset_ids = sorted(
        {
            item["id"]
            for merged_dataset in merged_datasets
            for item in (merged_dataset.source_datasets_metadata or [])
        }
    )
    source_datasets = (
        db.query(CsvUploadedDataset)
        .filter(
            CsvUploadedDataset.created_by_user_id == current_user.id,
            CsvUploadedDataset.id.in_(source_dataset_ids),
        )
        .all()
        if source_dataset_ids
        else []
    )
    source_dataset_map = {dataset.id: dataset for dataset in source_datasets}

    return success_response(
        "Datasets fetched successfully",
        data={
            "uploaded_datasets": uploaded_datasets,
            "merged_datasets": [
                _serialize_merged_dataset(merged_dataset, source_dataset_map)
                for merged_dataset in merged_datasets
            ],
        },
    )


@router.post("/uploaded/{dataset_id}/retention", response_model=MessageSuccessResponse)
def retention_csv_uploaded_dataset(
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = (
        db.query(CsvUploadedDataset)
        .filter(
            CsvUploadedDataset.id == dataset_id,
            CsvUploadedDataset.created_by_user_id == current_user.id,
        )
        .first()
    )

    if not dataset:
        raise error_response(status_code=404, detail="Uploaded dataset not found")

    retention_until = retention_dataset_for_user(
        db=db,
        dataset=dataset,
        user_id=current_user.id,
    )
    db.commit()
    db.refresh(dataset)

    logger.info(
        "retention uploaded dataset_id=%s for user_id=%s until=%s",
        dataset_id,
        current_user.id,
        retention_until,
    )
    return success_response(
        "Uploaded dataset retention expiry updated successfully",
        data={
            "dataset_id": dataset.id,
            "is_retention": dataset.is_retention,
            "retention_until": dataset.retention_until,
            "retention_at": dataset.retention_at,
        },
    )


@router.delete("/uploaded/{dataset_id}", response_model=MessageSuccessResponse)
def delete_csv_uploaded_dataset(
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = (
        db.query(CsvUploadedDataset)
        .filter(
            CsvUploadedDataset.id == dataset_id,
            CsvUploadedDataset.created_by_user_id == current_user.id,
        )
        .first()
    )

    if not dataset:
        raise error_response(status_code=404, detail="Uploaded dataset not found")

    delete_uploaded_dataset(dataset=dataset)
    db.commit()

    logger.info(
        "Deleted uploaded dataset_id=%s for user_id=%s",
        dataset_id,
        current_user.id,
    )
    return success_response("Uploaded dataset deleted successfully", data=None)


@router.delete("/merged/{dataset_id}", response_model=MessageSuccessResponse)
def delete_csv_merged_dataset(
    dataset_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    dataset = (
        db.query(CsvMergedDataset)
        .filter(
            CsvMergedDataset.id == dataset_id,
            CsvMergedDataset.created_by_user_id == current_user.id,
        )
        .first()
    )

    if not dataset:
        raise error_response(status_code=404, detail="Merged dataset not found")

    delete_merged_dataset(dataset=dataset)
    db.commit()

    logger.info(
        "Deleted merged dataset_id=%s for user_id=%s",
        dataset_id,
        current_user.id,
    )
    return success_response("Merged dataset deleted successfully", data=None)
