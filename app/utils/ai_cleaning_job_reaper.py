"""Recovery for AI cleaning jobs orphaned by a worker/process restart.

AI cleaning runs in an in-process daemon thread. If the process restarts (deploy,
scale event, or ``--reload`` in dev), that thread dies and its job is left stuck in
``pending``/``processing`` forever, because nothing ever moves it to a terminal
state. This reaper marks such stale jobs as ``failed`` so the client stops polling a
job that will never finish. It is time-based so a genuinely in-flight job is not
touched.
"""
import logging
from datetime import datetime, timedelta

from app.models.cleaning_models import CleaningJob

logger = logging.getLogger(__name__)

STALE_JOB_MINUTES = 15


def reap_stale_ai_cleaning_jobs(db, *, older_than_minutes: int = STALE_JOB_MINUTES) -> int:
    """Mark AI cleaning jobs stuck in pending/processing past the cutoff as failed."""
    cutoff = datetime.utcnow() - timedelta(minutes=older_than_minutes)
    stale_jobs = (
        db.query(CleaningJob)
        .filter(
            CleaningJob.ai_cleaning_type.is_(True),
            CleaningJob.status.in_(["pending", "processing"]),
            CleaningJob.created_at < cutoff,
        )
        .all()
    )
    if not stale_jobs:
        return 0

    now = datetime.utcnow()
    for job in stale_jobs:
        job.status = "failed"
        job.error_message = (
            "Cleaning did not complete (worker was interrupted or the job timed out). "
            "Please run the cleaning again."
        )
        job.completed_at = now
    db.commit()
    logger.warning("Reaped %d stale AI cleaning job(s) stuck in pending/processing", len(stale_jobs))
    return len(stale_jobs)
