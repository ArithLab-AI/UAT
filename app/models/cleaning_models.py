import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, JSON, String, Text

from app.db.database import Base


class CleaningJob(Base):
    __tablename__ = "cleaning_jobs"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    original_filename = Column(String(255), nullable=False)
    file_type = Column(String(20), nullable=False)
    file_size_bytes = Column(Integer, nullable=False)

    # S3 URLs only — no files in DB
    s3_upload_url = Column(Text, nullable=True)
    s3_cleaned_url = Column(Text, nullable=True)
    download_url = Column(Text, nullable=True)
    output_filename = Column(String(255), nullable=True)

    # Progress
    status = Column(String(20), default="pending")
    progress_pct = Column(Integer, default=0)
    current_step = Column(Integer, default=0)
    total_steps = Column(Integer, default=0)
    current_step_name = Column(String(100), default="")

    # Results
    rows_before = Column(Integer, nullable=True)
    rows_after = Column(Integer, nullable=True)
    columns_before = Column(Integer, nullable=True)
    columns_after = Column(Integer, nullable=True)
    source_dataset_id = Column(Integer, nullable=True, index=True)
    ai_cleaning_type = Column(Boolean, nullable=False, default=False)
    steps_applied = Column(JSON, nullable=True)
    cleaning_summary = Column(JSON, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)
