from datetime import datetime
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import relationship
from app.db.database import Base

class CsvUploadedDataset(Base):
    __tablename__ = "csv_uploaded_datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    file_name = Column(String, nullable=False)
    file_size = Column(Integer, nullable=False, default=0)
    table_name = Column(String, nullable=False, unique=True, index=True)
    storage_key = Column(String, nullable=False, unique=True)
    file_url = Column(String, nullable=False)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    total_rows = Column(Integer, default=0, nullable=False)
    columns = Column(JSON, nullable=False)
    internal_columns = Column(JSON, nullable=False)
    is_retention = Column(Boolean, default=False, nullable=False)
    retention_until = Column(DateTime, nullable=True)
    retention_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = relationship("User")

class CsvMergedDataset(Base):
    __tablename__ = "csv_merged_datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    table_name = Column(String, nullable=False, unique=True, index=True)
    storage_key = Column(String, nullable=False, unique=True)
    file_url = Column(String, nullable=False)
    file_size = Column(Integer, nullable=True, default=0)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    source_datasets_metadata = Column(JSON, nullable=True)
    total_rows = Column(Integer, default=0, nullable=False)
    columns = Column(JSON, nullable=False)
    internal_columns = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = relationship("User")
