import logging
import os

from fastapi import APIRouter, Depends, Request
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
)
from app.schemas.common_schema import MessageSuccessResponse
from app.services.csv_service import (
    build_dataset_name,
    count_user_active_datasets,
    create_uploaded_dataset,
    delete_uploaded_dataset,
    merge_uploaded_datasets,
    parse_csv_upload,
)
from app.services.subscription_service import get_user_plan_capabilities
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


def _serialize_merged_dataset(
    merged_dataset: CsvMergedDataset,
    source_dataset_map: dict[int, CsvUploadedDataset],
):
    return {
        "id": merged_dataset.id,
        "name": merged_dataset.name,
        "table_name": merged_dataset.table_name,
        "storage_key": merged_dataset.storage_key,
        "file_url": merged_dataset.file_url,
        "total_rows": merged_dataset.total_rows,
        "columns": merged_dataset.columns,
        "created_at": merged_dataset.created_at,
        "source_dataset_ids": merged_dataset.source_dataset_ids,
        "source_datasets": [
            {
                "id": source_id,
                "file_name": source_dataset_map[source_id].file_name,
            }
            for source_id in merged_dataset.source_dataset_ids
            if source_id in source_dataset_map
        ],
    }


@router.post(
    "/upload-multiple",
    response_model=CsvUploadedDatasetListSuccessResponse,
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

    created_datasets = []
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

        file_name, file_size, columns, internal_columns, rows = await parse_csv_upload(file)
        dataset_name = build_dataset_name(None, file_name)
        dataset = create_uploaded_dataset(
            db,
            dataset_name=dataset_name,
            file_name=file_name,
            file_size=file_size,
            columns=columns,
            internal_columns=internal_columns,
            rows=rows,
            user_id=current_user.id,
        )
        created_datasets.append(dataset)

    db.commit()

    for dataset in created_datasets:
        db.refresh(dataset)

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

    source_ids = list(dict.fromkeys(payload.source_dataset_ids))
    max_merge_sources = plan_capabilities["max_merge_sources"]
    if max_merge_sources is not None and len(source_ids) > max_merge_sources:
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

    fetched_datasets = (
        db.query(CsvUploadedDataset)
        .filter(
            CsvUploadedDataset.created_by_user_id == current_user.id,
            CsvUploadedDataset.id.in_(source_ids),
        )
        .all()
    )

    if len(fetched_datasets) != len(source_ids):
        raise error_response(
            status_code=404,
            detail="One or more source dataset IDs were not found",
        )

    source_dataset_map = {dataset.id: dataset for dataset in fetched_datasets}
    source_datasets = [source_dataset_map[source_id] for source_id in source_ids]

    merged_dataset = merge_uploaded_datasets(
        db,
        merged_name=payload.merged_name,
        source_datasets=source_datasets,
        user_id=current_user.id,
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
            source_id
            for merged_dataset in merged_datasets
            for source_id in merged_dataset.source_dataset_ids
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
