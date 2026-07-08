import logging
import os
from math import ceil
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, Query, Request
from sqlalchemy import String, or_
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.ai_cleaning_models import AICleaningJobDetail
from app.models.auth_models import User
from app.models.cleaning_models import CleaningJob
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.schemas.csv_dataset_schema import (
    CsvDatasetListSuccessResponse,
    CsvMergedDatasetSuccessResponse,
    CsvUploadedDatasetListSuccessResponse,
    MergeCsvDatasetsRequest,
    MergeSuggestionsRequest,
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
from app.services.subscription_service import (
    ensure_upload_storage_available,
    get_recorded_upload_count,
    get_user_plan_capabilities,
    record_upload_storage_usage,
)
from app.services.temporary_upload_service import (
    delete_temporary_upload,
    validate_temporary_upload,
)
from app.utils.object_storage import get_object_storage_service
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
DEFAULT_DATASET_PAGE_SIZE = 10
MAX_DATASET_PAGE_SIZE = 100

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


def _build_uploaded_dataset_name(file_name: str, sheet_name: str | None = None) -> str:
    dataset_name = build_dataset_name(None, file_name)
    if sheet_name:
        return f"{dataset_name} ({sheet_name})"
    return dataset_name


def _build_uploaded_file_name(file_name: str, sheet_name: str | None = None) -> str:
    if sheet_name:
        return f"{file_name} ({sheet_name})"
    return file_name


def _normalize_sort_timestamp(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _clean_file_url(clean_state: dict) -> str | None:
    """Return the cleaned file as a presigned, directly-downloadable HTTPS URL.

    Falls back to the raw ``s3://`` URI if signing is unavailable (e.g. storage off)."""
    raw_url = clean_state.get("clean_file_url")
    if not raw_url:
        return None
    return get_object_storage_service().presigned_download_url(raw_url) or raw_url


def _serialize_uploaded_dataset(uploaded_dataset, clean_state=None):
    clean_state = clean_state or {}
    return {
        "id": uploaded_dataset.id,
        "name": uploaded_dataset.name,
        "file_name": uploaded_dataset.file_name,
        "sheet_name": uploaded_dataset.sheet_name,
        "table_name": uploaded_dataset.table_name,
        "storage_key": uploaded_dataset.storage_key,
        "file_url": uploaded_dataset.file_url,
        "is_clean": bool(clean_state.get("is_clean", False)),
        "clean_file_url": _clean_file_url(clean_state),
        "file_size": uploaded_dataset.file_size,
        "total_rows": uploaded_dataset.total_rows,
        "columns": uploaded_dataset.columns,
        "is_retention": uploaded_dataset.is_retention,
        "retention_until": uploaded_dataset.retention_until,
        "retention_at": uploaded_dataset.retention_at,
        "created_at": uploaded_dataset.created_at,
    }


def _serialize_merged_dataset(merged_dataset, source_dataset_map=None, clean_state=None):
    metadata = merged_dataset.source_datasets_metadata or []
    source_dataset_map = source_dataset_map or {}
    clean_state = clean_state or {}
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
        "is_clean": bool(clean_state.get("is_clean", False)),
        "clean_file_url": _clean_file_url(clean_state),
        "file_size": merged_dataset.file_size,
        "total_rows": merged_dataset.total_rows,
        "columns": merged_dataset.columns,
        "created_at": merged_dataset.created_at,
        "source_datasets": source_datasets,
    }


def _update_dataset_clean_state(
    clean_state_map: dict[tuple[str, int], dict[str, Any]],
    dataset_key: tuple[str, int],
    *,
    clean_file_url: str | None,
    cleaned_at: datetime | None,
):
    if not clean_file_url or dataset_key not in clean_state_map:
        return

    candidate_sort_key = _normalize_sort_timestamp(cleaned_at)
    current_state = clean_state_map[dataset_key]
    if candidate_sort_key < current_state["_sort_key"]:
        return

    current_state["is_clean"] = True
    current_state["clean_file_url"] = clean_file_url
    current_state["_sort_key"] = candidate_sort_key


def _build_dataset_clean_state_map(
    db: Session,
    *,
    current_user: User,
    uploaded_datasets: list[CsvUploadedDataset],
    merged_datasets: list[CsvMergedDataset],
) -> dict[tuple[str, int], dict[str, Any]]:
    clean_state_map: dict[tuple[str, int], dict[str, Any]] = {
        ("uploaded", dataset.id): {
            "is_clean": False,
            "clean_file_url": None,
            "_sort_key": float("-inf"),
        }
        for dataset in uploaded_datasets
    }
    clean_state_map.update(
        {
            ("merged", dataset.id): {
                "is_clean": False,
                "clean_file_url": None,
                "_sort_key": float("-inf"),
            }
            for dataset in merged_datasets
        }
    )

    dataset_ids = sorted(
        {dataset.id for dataset in uploaded_datasets}
        | {dataset.id for dataset in merged_datasets}
    )
    if not dataset_ids:
        return clean_state_map

    uploaded_name_map = {
        (dataset.id, dataset.file_name): ("uploaded", dataset.id)
        for dataset in uploaded_datasets
    }
    merged_name_map = {
        (dataset.id, f"{dataset.name}.csv"): ("merged", dataset.id)
        for dataset in merged_datasets
    }

    ai_records = (
        db.query(CleaningJob, AICleaningJobDetail)
        .join(AICleaningJobDetail, AICleaningJobDetail.job_id == CleaningJob.id)
        .filter(
            CleaningJob.ai_cleaning_type.is_(True),
            CleaningJob.status == "completed",
            AICleaningJobDetail.created_by_user_id == current_user.id,
            AICleaningJobDetail.source_dataset_id.in_(dataset_ids),
            AICleaningJobDetail.cleaned_file_path.isnot(None),
        )
        .order_by(AICleaningJobDetail.updated_at.desc(), AICleaningJobDetail.created_at.desc())
        .all()
    )
    for job, detail in ai_records:
        dataset_type = (detail.source_dataset_type or "").strip()
        dataset_key = (dataset_type, detail.source_dataset_id)
        _update_dataset_clean_state(
            clean_state_map,
            dataset_key,
            clean_file_url=detail.cleaned_file_path,
            cleaned_at=detail.updated_at or detail.created_at or job.completed_at or job.created_at,
        )

    manual_records = (
        db.query(CleaningJob)
        .filter(
            CleaningJob.ai_cleaning_type.is_not(True),
            CleaningJob.status == "completed",
            CleaningJob.source_dataset_id.in_(dataset_ids),
            CleaningJob.s3_cleaned_url.isnot(None),
        )
        .order_by(CleaningJob.completed_at.desc(), CleaningJob.created_at.desc())
        .all()
    )
    for job in manual_records:
        candidate_dataset_keys = []
        uploaded_key = uploaded_name_map.get((job.source_dataset_id, job.original_filename))
        if uploaded_key is not None:
            candidate_dataset_keys.append(uploaded_key)

        merged_key = merged_name_map.get((job.source_dataset_id, job.original_filename))
        if merged_key is not None:
            candidate_dataset_keys.append(merged_key)

        for dataset_key in candidate_dataset_keys:
            _update_dataset_clean_state(
                clean_state_map,
                dataset_key,
                clean_file_url=job.s3_cleaned_url,
                cleaned_at=job.completed_at or job.created_at,
            )

    return clean_state_map

def _normalize_search_query(search: str | None) -> str | None:
    if search is None:
        return None

    normalized_search = search.strip()
    return normalized_search or None

def _resolve_search_query(*search_terms: str | None) -> str | None:
    for search_term in search_terms:
        normalized_search = _normalize_search_query(search_term)
        if normalized_search:
            return normalized_search
    return None


def _pagination_meta(total: int, page: int, page_size: int) -> dict[str, int]:
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": ceil(total / page_size) if total else 0,
    }


def _paginate_dataset_query(query, *, page: int, page_size: int):
    total = query.order_by(None).count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()
    return items, _pagination_meta(total, page, page_size)


def _selected_sheet_names_for_route(selection: SelectExcelSheetRequest) -> list[str]:
    sheet_names = [sheet_name.strip() for sheet_name in selection.selected_sheet_names()]
    if len(set(sheet_names)) != len(sheet_names):
        raise error_response(
            status_code=400,
            detail="Sheet names must be unique for each file token",
        )
    return sheet_names


def _ensure_merge_allowed(plan_capabilities: dict, source_count: int) -> None:
    if not plan_capabilities["can_merge"]:
        raise error_response(
            status_code=400,
            detail="Data merging is not available on your current plan. Please upgrade your plan.",
        )

    max_merge_sources = plan_capabilities["max_merge_sources"]
    if max_merge_sources is not None and source_count > max_merge_sources:
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows merging up to {max_merge_sources} source datasets at a time. "
                "Please upgrade your plan."
            ),
        )


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
    recorded_upload_count = get_recorded_upload_count(db, current_user.id)
    max_active_datasets = plan_capabilities["max_active_datasets"]

    if (
        max_active_datasets is not None
        and recorded_upload_count + len(files) > max_active_datasets
    ):
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows up to {max_active_datasets} uploaded files. "
                "You have reached your upload limit. Please upgrade your plan."
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
    total_upload_size = sum(parsed_upload.file_size for parsed_upload in parsed_uploads)
    ensure_upload_storage_available(
        db,
        user_id=current_user.id,
        plan_capabilities=plan_capabilities,
        upload_size_bytes=total_upload_size,
    )
    for parsed_upload in parsed_uploads:
        dataset_name = _build_uploaded_dataset_name(parsed_upload.file_name, parsed_upload.sheet_name)
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

    db.flush()
    for dataset in created_datasets:
        record_upload_storage_usage(db, dataset=dataset, user_id=current_user.id)

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
            "uploaded_datasets": [
                _serialize_uploaded_dataset(dataset)
                for dataset in created_datasets
            ],
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
        data=[
            _serialize_uploaded_dataset(dataset)
            for dataset in created_datasets
        ],
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
                    "sheet_names": ["Sheet1", "Sheet2"],
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

    selected_sheet_names_by_selection = [
        (selection, _selected_sheet_names_for_route(selection))
        for selection in payload
    ]
    selected_sheet_count = sum(
        len(sheet_names) for _, sheet_names in selected_sheet_names_by_selection
    )
    plan_capabilities = get_user_plan_capabilities(db, current_user)
    recorded_upload_count = get_recorded_upload_count(db, current_user.id)
    max_active_datasets = plan_capabilities["max_active_datasets"]
    if (
        max_active_datasets is not None
        and recorded_upload_count + selected_sheet_count > max_active_datasets
    ):
        raise error_response(
            status_code=400,
            detail=(
                f"Your current plan allows up to {max_active_datasets} uploaded files. "
                "You have reached your upload limit. Please upgrade your plan."
            ),
        )

    selected_uploads = []
    total_selected_upload_size = 0
    seen_file_sheet_selections: set[tuple[str, str]] = set()
    for selection, selected_sheet_names in selected_sheet_names_by_selection:
        temporary_upload = validate_temporary_upload(
            token=selection.file_token,
            user_id=current_user.id,
        )
        for selected_sheet_name in selected_sheet_names:
            file_sheet_key = (selection.file_token, selected_sheet_name)
            if file_sheet_key in seen_file_sheet_selections:
                raise error_response(
                    status_code=400,
                    detail="Each sheet can only be selected once per uploaded file",
                )
            seen_file_sheet_selections.add(file_sheet_key)
            (
                file_name,
                file_size,
                columns,
                internal_columns,
                rows,
                sheet_name,
            ) = process_temporary_upload_selection(
                upload=temporary_upload,
                sheet_name=selected_sheet_name,
            )
            total_selected_upload_size += file_size
            ensure_upload_storage_available(
                db,
                user_id=current_user.id,
                plan_capabilities=plan_capabilities,
                upload_size_bytes=total_selected_upload_size,
            )
            selected_uploads.append(
                {
                    "file_name": file_name,
                    "display_file_name": _build_uploaded_file_name(file_name, sheet_name),
                    "file_size": file_size,
                    "columns": columns,
                    "internal_columns": internal_columns,
                    "rows": rows,
                    "sheet_name": sheet_name,
                }
            )

    created_datasets = []
    for selected_upload in selected_uploads:
        dataset = create_uploaded_dataset(
            db,
            dataset_name=_build_uploaded_dataset_name(
                selected_upload["file_name"],
                selected_upload["sheet_name"],
            ),
            file_name=selected_upload["display_file_name"],
            sheet_name=selected_upload["sheet_name"],
            file_size=selected_upload["file_size"],
            columns=selected_upload["columns"],
            internal_columns=selected_upload["internal_columns"],
            rows=selected_upload["rows"],
            user_id=current_user.id,
        )
        set_dataset_retention_expiry(
            db=db,
            dataset=dataset,
            user_id=current_user.id,
        )
        created_datasets.append(dataset)

    db.flush()
    for dataset in created_datasets:
        record_upload_storage_usage(db, dataset=dataset, user_id=current_user.id)

    db.commit()

    for dataset in created_datasets:
        db.refresh(dataset)

    for file_token in {selection.file_token for selection in payload}:
        delete_temporary_upload(file_token)

    return success_response(
        "Uploaded datasets created successfully",
        status_code=201,
        data=[
            _serialize_uploaded_dataset(dataset)
            for dataset in created_datasets
        ],
    )


@router.post("/merge/suggestions", response_model=MergeSuggestionsSuccessResponse)
def suggest_csv_dataset_merge(
    payload: MergeSuggestionsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    plan_capabilities = get_user_plan_capabilities(db, current_user)
    _ensure_merge_allowed(plan_capabilities, len(payload.source_dataset_ids))

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
    _ensure_merge_allowed(plan_capabilities, len(payload.source_dataset_ids))

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
    _ensure_merge_allowed(plan_capabilities, len(payload.source_dataset_ids))

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
    search: str | None = Query(
        default=None,
        description="Search term applied to uploaded and merged datasets unless a list-specific search is provided.",
    ),
    uploaded_search: str | None = Query(
        default=None,
        description="Search term for uploaded datasets.",
    ),
    merged_search: str | None = Query(
        default=None,
        description="Search term for merged datasets.",
    ),
    uploaded_page: int = Query(default=1, ge=1),
    uploaded_page_size: int = Query(
        default=DEFAULT_DATASET_PAGE_SIZE,
        ge=1,
        le=MAX_DATASET_PAGE_SIZE,
    ),
    merged_page: int = Query(default=1, ge=1),
    merged_page_size: int = Query(
        default=DEFAULT_DATASET_PAGE_SIZE,
        ge=1,
        le=MAX_DATASET_PAGE_SIZE,
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    uploaded_specific_search = _normalize_search_query(uploaded_search)
    merged_specific_search = _normalize_search_query(merged_search)
    shared_search = _normalize_search_query(search)
    has_specific_search = (
        uploaded_specific_search is not None or merged_specific_search is not None
    )
    uploaded_search_term = uploaded_specific_search or (
        shared_search if not has_specific_search else None
    )
    merged_search_term = merged_specific_search or (
        shared_search if not has_specific_search else None
    )
    include_uploaded_datasets = uploaded_search_term is not None or not has_specific_search
    include_merged_datasets = merged_search_term is not None or not has_specific_search

    uploaded_query = db.query(CsvUploadedDataset).filter(
        CsvUploadedDataset.created_by_user_id == current_user.id
    )
    if uploaded_search_term:
        uploaded_search_pattern = f"%{uploaded_search_term}%"
        uploaded_query = uploaded_query.filter(
            or_(
                CsvUploadedDataset.name.ilike(uploaded_search_pattern),
                CsvUploadedDataset.file_name.ilike(uploaded_search_pattern),
                CsvUploadedDataset.sheet_name.ilike(uploaded_search_pattern),
                CsvUploadedDataset.table_name.ilike(uploaded_search_pattern),
            )
        )
    uploaded_query = uploaded_query.order_by(CsvUploadedDataset.id.desc())
    if include_uploaded_datasets:
        uploaded_datasets, uploaded_pagination = _paginate_dataset_query(
            uploaded_query,
            page=uploaded_page,
            page_size=uploaded_page_size,
        )
    else:
        uploaded_datasets = []
        uploaded_pagination = _pagination_meta(0, uploaded_page, uploaded_page_size)

    merged_query = db.query(CsvMergedDataset).filter(
        CsvMergedDataset.created_by_user_id == current_user.id
    )
    if merged_search_term:
        merged_search_pattern = f"%{merged_search_term}%"
        merged_query = merged_query.filter(
            or_(
                CsvMergedDataset.name.ilike(merged_search_pattern),
                CsvMergedDataset.table_name.ilike(merged_search_pattern),
                CsvMergedDataset.source_datasets_metadata.cast(String).ilike(
                    merged_search_pattern
                ),
            )
        )
    merged_query = merged_query.order_by(CsvMergedDataset.id.desc())
    if include_merged_datasets:
        merged_datasets, merged_pagination = _paginate_dataset_query(
            merged_query,
            page=merged_page,
            page_size=merged_page_size,
        )
    else:
        merged_datasets = []
        merged_pagination = _pagination_meta(0, merged_page, merged_page_size)
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

    clean_state_map = _build_dataset_clean_state_map(
        db,
        current_user=current_user,
        uploaded_datasets=uploaded_datasets,
        merged_datasets=merged_datasets,
    )

    return success_response(
        "Datasets fetched successfully",
        data={
            "uploaded_datasets": [
                _serialize_uploaded_dataset(
                    uploaded_dataset,
                    clean_state_map.get(("uploaded", uploaded_dataset.id)),
                )
                for uploaded_dataset in uploaded_datasets
            ],
            "uploaded_pagination": uploaded_pagination,
            "merged_datasets": [
                _serialize_merged_dataset(
                    merged_dataset,
                    source_dataset_map,
                    clean_state_map.get(("merged", merged_dataset.id)),
                )
                for merged_dataset in merged_datasets
            ],
            "merged_pagination": merged_pagination,
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

    record_upload_storage_usage(db, dataset=dataset, user_id=current_user.id)
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
