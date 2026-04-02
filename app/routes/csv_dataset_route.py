import logging

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
from app.services.csv_service import (
    build_dataset_name,
    create_uploaded_dataset,
    merge_uploaded_datasets,
    parse_csv_upload,
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

    created_datasets = []
    for file in files:
        if not hasattr(file, "filename") or not hasattr(file, "read"):
            raise error_response(status_code=400, detail="Invalid file input")
        file_name, columns, internal_columns, rows = await parse_csv_upload(file)
        dataset_name = build_dataset_name(None, file_name)
        dataset = create_uploaded_dataset(
            db,
            dataset_name=dataset_name,
            file_name=file_name,
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
    source_ids = list(dict.fromkeys(payload.source_dataset_ids))
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
        data=merged_dataset,
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

    return success_response(
        "Datasets fetched successfully",
        data={
            "uploaded_datasets": uploaded_datasets,
            "merged_datasets": merged_datasets,
        },
    )
