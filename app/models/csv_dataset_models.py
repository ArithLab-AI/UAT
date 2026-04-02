from datetime import datetime
from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import relationship
from app.db.database import Base

class CsvUploadedDataset(Base):
    __tablename__ = "csv_uploaded_datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    file_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False, unique=True, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    total_rows = Column(Integer, default=0, nullable=False)
    columns = Column(JSON, nullable=False)
    internal_columns = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = relationship("User")

class CsvMergedDataset(Base):
    __tablename__ = "csv_merged_datasets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    table_name = Column(String, nullable=False, unique=True, index=True)
    created_by_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    source_dataset_ids = Column(JSON, nullable=False)
    total_rows = Column(Integer, default=0, nullable=False)
    columns = Column(JSON, nullable=False)
    internal_columns = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = relationship("User")
