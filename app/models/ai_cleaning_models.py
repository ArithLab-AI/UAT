import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import relationship

from app.db.database import Base


class AICleaningJobDetail(Base):
    __tablename__ = "ai_cleaning_job_details"

    job_id = Column(
        String(36),
        ForeignKey("cleaning_jobs.id", ondelete="CASCADE"),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=True, index=True)
    source_dataset_id = Column(Integer, nullable=True, index=True)
    source_dataset_type = Column(String(20), nullable=True, index=True)
    source_dataset_name = Column(String(255), nullable=True)
    source_file_name = Column(String(255), nullable=True)
    source_storage_key = Column(String(255), nullable=True)
    source_file_url = Column(Text, nullable=True)
    source_suggestion_id = Column(String(36), nullable=True, index=True)
    source_suggestion_priority = Column(String(20), nullable=True)
    source_ai_job_id = Column(String(36), nullable=True, index=True)
    source_type = Column(String(20), nullable=True, default="raw")
    prompt = Column(Text, nullable=True)
    cleaning_strategy = Column(String(100), nullable=True)
    llm_used = Column(Boolean, nullable=True, default=False)
    target_columns = Column(JSON, nullable=True)
    cleaned_storage_key = Column(String(255), nullable=True)
    cleaned_file_path = Column(Text, nullable=True)
    cleaned_data = Column(JSON, nullable=True)
    cleaned_rows = Column(Integer, nullable=True)
    preview_rows_returned = Column(Integer, nullable=True)
    preview_limited = Column(Boolean, nullable=True, default=False)
    changes_detected = Column(Boolean, nullable=True, default=False)
    quality_score = Column(Integer, nullable=True)
    analysis = Column(JSON, nullable=True)
    suggestion_source = Column(String(20), nullable=True)
    llm_provider = Column(String(50), nullable=True)
    llm_model = Column(String(255), nullable=True)
    message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    created_by = relationship("User")
    job = relationship("CleaningJob")
