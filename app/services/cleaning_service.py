"""
Background cleaning service.
Uses project's existing:
  - app.db.database (SessionLocal)
  - app.utils.object_storage (ObjectStorageService for S3)
  - app.utils.file_io (read/write any file)
"""
import os
import time
import threading
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.db.database import SessionLocal
from app.models.cleaning_models import CleaningJob
from app.services.cleaning_engine import run_cleaning_pipeline
from app.utils.file_io import read_file_to_dataframe, write_dataframe_to_file, SUPPORTED_EXTENSIONS
from app.utils.object_storage import get_object_storage_service

# Temp dir for processing
TEMP_DIR = "./temp"
os.makedirs(TEMP_DIR, exist_ok=True)

# ── ALL 28 STEPS ──────────────────────────────────────────────────────────

ALL_STEPS = [
    {"step": "use_first_row_as_header"},
    {"step": "standardize_headers"},
    {"step": "remove_duplicates"},
    {"step": "remove_constant_columns"},
    {"step": "strip_whitespace"},
    {"step": "fix_encoding"},
    {"step": "clean_null_strings"},
    {"step": "clean_currency"},
    {"step": "standardize_dates"},
    {"step": "fix_data_types"},
    {"step": "normalize_text_case", "strategy": "lower"},
    {"step": "standardize_booleans"},
    {"step": "standardize_categories"},
    {"step": "replace_value"},
    {"step": "handle_outliers", "strategy": "clip"},
    {"step": "validate_numeric_range"},
    {"step": "scaling_normalization", "strategy": "minmax"},
    {"step": "binning"},
    {"step": "validate_emails"},
    {"step": "normalize_phones"},
    {"step": "validate_string_length"},
    {"step": "cross_column_check"},
    {"step": "whitelist_validation"},
    {"step": "extract_from_text"},
    {"step": "mask_pii"},
    {"step": "standardize_units"},
    {"step": "feature_engineering_dates"},
    {"step": "handle_missing_values", "strategy": "fill_median"},
]


def start_cleaning_job(storage_key: str, file_name: str, job_id: str, db: Session) -> CleaningJob:
    """Download file from object storage by storage_key, create DB record, launch background thread."""
    ext = Path(file_name).suffix.lower().lstrip(".")
    if not ext:
        ext = "csv"
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported: '.{ext}'. Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}")

    temp_path = os.path.join(TEMP_DIR, f"{job_id}.{ext}")
    storage = get_object_storage_service()
    success = storage.download_file(storage_key, temp_path)
    if not success:
        raise ValueError(f"Could not download file from storage (key: {storage_key})")

    file_size = os.path.getsize(temp_path)

    job = CleaningJob(
        id=job_id,
        original_filename=file_name,
        file_type=ext,
        file_size_bytes=file_size,
        status="pending",
        total_steps=len(ALL_STEPS),
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    thread = threading.Thread(
        target=_background_clean,
        args=(job_id, temp_path, ext, file_name),
        daemon=True,
    )
    thread.start()
    return job


def _update_job(job_id: str, **kwargs):
    """Update job in DB from background thread (uses its own session)."""
    db = SessionLocal()
    try:
        job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
        if job:
            for k, v in kwargs.items():
                setattr(job, k, v)
            db.commit()
    finally:
        db.close()


def _background_clean(job_id: str, temp_path: str, ext: str, original_name: str):
    """Background worker: read → clean → write → upload S3 → update DB."""
    storage = get_object_storage_service()
    start = time.time()
    print(f"\n{'='*60}", flush=True)
    print(f"[{job_id[:8]}] Starting job: {original_name}", flush=True)

    try:
        # 1. Upload original to S3
        _update_job(job_id, status="uploading", current_step_name="Uploading original to S3")
        s3_upload_key = f"uploads/{job_id}/{original_name}"
        s3_upload_url = storage.upload_file(temp_path, s3_upload_key)
        _update_job(job_id, s3_upload_url=s3_upload_url)
        print(f"[{job_id[:8]}] Uploaded original to S3", flush=True)

        # 2. Read file
        _update_job(job_id, status="reading", current_step_name="Reading file")
        t0 = time.time()
        df = read_file_to_dataframe(temp_path, file_type=ext)
        print(f"[{job_id[:8]}] Read: {time.time()-t0:.1f}s | {len(df)} rows x {len(df.columns)} cols", flush=True)
        _update_job(job_id, rows_before=len(df), columns_before=len(df.columns))

        # 3. Clean
        _update_job(job_id, status="cleaning", current_step_name="Starting cleaning")
        print(f"[{job_id[:8]}] Running {len(ALL_STEPS)} steps...", flush=True)

        def on_progress(step_idx, total, step_name, elapsed):
            pct = int((step_idx / total) * 100)
            _update_job(job_id, current_step=step_idx, current_step_name=step_name, progress_pct=pct)

        cleaned_df, step_results = run_cleaning_pipeline(df, ALL_STEPS, on_progress=on_progress)
        _update_job(job_id, rows_after=len(cleaned_df), columns_after=len(cleaned_df.columns), steps_applied=step_results)

        # 4. Write cleaned file locally
        _update_job(job_id, status="writing", current_step_name="Writing cleaned file")
        out_fmt = ext if ext != "pdf" else "csv"
        output_filename = f"cleaned_{Path(original_name).stem}.{out_fmt}"
        temp_output = os.path.join(TEMP_DIR, f"{job_id}_{output_filename}")
        t0 = time.time()
        write_dataframe_to_file(cleaned_df, temp_output, out_fmt)
        print(f"[{job_id[:8]}] Written: {time.time()-t0:.1f}s", flush=True)

        # 5. Upload cleaned to S3
        _update_job(job_id, status="uploading", current_step_name="Uploading cleaned file to S3")
        s3_output_key = f"cleaned/{job_id}/{output_filename}"
        s3_cleaned_url = storage.upload_file(temp_output, s3_output_key)

        # Generate presigned download URL
        client = storage._get_client()
        download_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": storage.bucket_name, "Key": s3_output_key},
            ExpiresIn=3600,
        )
        print(f"[{job_id[:8]}] Uploaded cleaned to S3", flush=True)

        # 6. Done
        duration = round(time.time() - start, 3)
        summary = {
            "total_steps": len(ALL_STEPS),
            "applied": sum(1 for r in step_results if r.get("status") == "applied"),
            "skipped": sum(1 for r in step_results if r.get("status") == "skipped"),
            "errors": sum(1 for r in step_results if r.get("status") == "error"),
        }
        _update_job(
            job_id, status="completed", progress_pct=100,
            current_step_name="Done",
            s3_cleaned_url=s3_cleaned_url, download_url=download_url,
            output_filename=output_filename, cleaning_summary=summary,
            duration_seconds=duration, completed_at=datetime.now(timezone.utc),
        )
        print(f"[{job_id[:8]}] ✅ Done in {duration}s", flush=True)
        print(f"{'='*60}\n", flush=True)

    except Exception as e:
        _update_job(job_id, status="failed", error_message=str(e),
                    duration_seconds=round(time.time() - start, 3))
        print(f"[{job_id[:8]}] ❌ Failed: {e}", flush=True)

    finally:
        for f in os.listdir(TEMP_DIR):
            if f.startswith(job_id):
                try:
                    os.remove(os.path.join(TEMP_DIR, f))
                except Exception:
                    pass
