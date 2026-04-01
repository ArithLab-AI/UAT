import logging

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.schemas.csv_dataset_schema import (
    CsvDatasetListResponse,
    CsvMergedDatasetResponse,
    CsvUploadedDatasetResponse,
    MergeCsvDatasetsRequest,
)
from app.services.csv_service import (
    build_dataset_name,
    create_uploaded_dataset,
    merge_uploaded_datasets,
    parse_csv_upload,
)

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
    response_model=list[CsvUploadedDatasetResponse],
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
        raise HTTPException(
            status_code=400,
            detail="At least one CSV, XLSX, or XLS file is required",
        )

    created_datasets = []
    for file in files:
        if not hasattr(file, "filename") or not hasattr(file, "read"):
            raise HTTPException(status_code=400, detail="Invalid file input")
        file_name, columns, rows = await parse_csv_upload(file)
        dataset_name = build_dataset_name(None, file_name)
        dataset = create_uploaded_dataset(
            db,
            dataset_name=dataset_name,
            file_name=file_name,
            columns=columns,
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
    return created_datasets

@router.post("/merge", response_model=CsvMergedDatasetResponse, status_code=201)
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
        raise HTTPException(
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
    return merged_dataset

@router.get("", response_model=CsvDatasetListResponse)
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

    return {
        "uploaded_datasets": uploaded_datasets,
        "merged_datasets": merged_datasets,
    }
