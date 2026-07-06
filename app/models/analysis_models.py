import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import relationship

from app.db.database import Base


class DatasetAnalysis(Base):
    __tablename__ = "dataset_analyses"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    dataset_name = Column(String(255), nullable=False)
    file_name = Column(String(255), nullable=False)
    quality_score = Column(Integer, nullable=False)
    llm_used = Column(Boolean, nullable=False, default=False)
    llm_provider = Column(String(50), nullable=True)
    llm_model = Column(String(255), nullable=True)
    suggestion_source = Column(String(20), nullable=False, default="rule_based")
    message = Column(Text, nullable=True)
    dataset_profile = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    created_by = relationship("User")


class AnalysisSuggestion(Base):
    __tablename__ = "analysis_suggestions"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    analysis_id = Column(
        String(36),
        ForeignKey("dataset_analyses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_dataset_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    issue_description = Column(Text, nullable=False)
    priority = Column(String(20), nullable=False)
    resolution_prompt = Column(Text, nullable=False)
    cleaning_prompt_type = Column(String(100), nullable=True)
    target_columns = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    analysis = relationship("DatasetAnalysis", backref="suggestion_rows")
    created_by = relationship("User")
