"""
POST   /clean                  → Upload file, start background job → returns job_id instantly
GET    /clean/status/{job_id}  → Poll progress (status, %, current step, results when done)
GET    /clean/download/{job_id}→ Redirect to S3 presigned URL
"""
import uuid
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.cleaning_models import CleaningJob
from app.services.cleaning_service import start_cleaning_job

router = APIRouter(tags=["Clean"])


@router.post("/clean", summary="Upload file → start background cleaning → returns job_id instantly")
async def clean_endpoint(
    file: UploadFile = File(..., description="CSV, XLSX, JSON, PDF, Parquet, TSV"),
    db: Session = Depends(get_db),
):
    job_id = str(uuid.uuid4())
    try:
        job = start_cleaning_job(file=file, job_id=job_id, db=db)
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


@router.get("/clean/download/{job_id}", summary="Redirect to S3 download")
async def download(job_id: str, db: Session = Depends(get_db)):
    job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    if job.status == "failed":
        raise HTTPException(status_code=500, detail=f"Job failed: {job.error_message}")
    if job.status != "completed":
        return {"job_id": job.id, "status": job.status, "progress_pct": job.progress_pct,
                "message": "Still processing. Poll /clean/status/{job_id}."}
    if not job.download_url:
        raise HTTPException(status_code=500, detail="Download URL not available.")
    return RedirectResponse(url=job.download_url)
