"""
POST   /clean                  → Start background cleaning job from an uploaded/merged dataset
GET    /clean/status/{job_id}  → Poll progress (status, %, current step, results when done)
GET    /clean/download/{job_id}→ Stream cleaned file directly from S3
"""
import uuid
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.models.cleaning_models import CleaningJob
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.services.cleaning_service import start_cleaning_job

router = APIRouter(tags=["Clean"])


class CleanRequest(BaseModel):
    dataset_id: int
    dataset_type: str  # "uploaded" or "merged"


@router.post("/clean", summary="Start background cleaning from an uploaded or merged dataset")
async def clean_endpoint(
    payload: CleanRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if payload.dataset_type == "uploaded":
        dataset = (
            db.query(CsvUploadedDataset)
            .filter(
                CsvUploadedDataset.id == payload.dataset_id,
                CsvUploadedDataset.created_by_user_id == current_user.id,
            )
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Uploaded dataset not found")
        file_name = dataset.file_name
        storage_key = dataset.storage_key

    elif payload.dataset_type == "merged":
        dataset = (
            db.query(CsvMergedDataset)
            .filter(
                CsvMergedDataset.id == payload.dataset_id,
                CsvMergedDataset.created_by_user_id == current_user.id,
            )
            .first()
        )
        if not dataset:
            raise HTTPException(status_code=404, detail="Merged dataset not found")
        file_name = f"{dataset.name}.csv"
        storage_key = dataset.storage_key

    else:
        raise HTTPException(status_code=400, detail="dataset_type must be 'uploaded' or 'merged'")

    job_id = str(uuid.uuid4())
    try:
        job = start_cleaning_job(
            storage_key=storage_key,
            file_name=file_name,
            job_id=job_id,
            db=db,
            source_dataset_id=payload.dataset_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {str(e)}")

    return {
        "job_id": job.id,
        "status": job.status,
        "message": "Cleaning started. Poll /clean/status/{job_id} for progress.",
        "status_url": f"/clean/status/{job.id}",
        "download_url": f"/clean/download/{job.id}",
    }


@router.get("/clean/status/{job_id}", summary="Poll job progress")
async def get_status(job_id: str, db: Session = Depends(get_db)):
    job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    response = {
        "job_id": job.id,
        "status": job.status,
        "progress_pct": job.progress_pct,
        "current_step": job.current_step,
        "total_steps": job.total_steps,
        "current_step_name": job.current_step_name,
        "original_filename": job.original_filename,
        "file_type": job.file_type,
        "file_size_bytes": job.file_size_bytes,
    }

    if job.status == "completed":
        response.update({
            "rows_before": job.rows_before,
            "rows_after": job.rows_after,
            "columns_before": job.columns_before,
            "columns_after": job.columns_after,
            "columns_after_names": job.columns_after_names,
            "cleaning_summary": job.cleaning_summary,
            "steps_applied": job.steps_applied,
            "duration_seconds": job.duration_seconds,
            "output_filename": job.output_filename,
            "download_url": job.download_url,
            "s3_upload_url": job.s3_upload_url,
            "s3_cleaned_url": job.s3_cleaned_url,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        })

    if job.status == "failed":
        response["error_message"] = job.error_message

    return response


@router.get("/clean/download/{job_id}", summary="Stream cleaned file from S3")
async def download(job_id: str, db: Session = Depends(get_db)):
    from app.utils.object_storage import get_object_storage_service

    job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    if job.status == "failed":
        raise HTTPException(status_code=500, detail=f"Job failed: {job.error_message}")
    if job.status != "completed":
        return {"job_id": job.id, "status": job.status, "progress_pct": job.progress_pct,
                "message": "Still processing. Poll /clean/status/{job_id}."}
    if not job.output_filename:
        raise HTTPException(status_code=500, detail="Cleaned file not available.")

    s3_key = f"cleaned/{job_id}/{job.output_filename}"
    storage = get_object_storage_service()
    client = storage._get_client()

    try:
        s3_object = client.get_object(Bucket=storage.bucket_name, Key=s3_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve file from storage: {e}")

    return StreamingResponse(
        s3_object["Body"].iter_chunks(chunk_size=65536),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{job.output_filename}"'},
    )
