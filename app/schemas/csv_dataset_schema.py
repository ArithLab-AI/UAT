from datetime import datetime
from pydantic import BaseModel, Field
from app.schemas.common_schema import SuccessResponse

class CsvDatasetSummaryResponse(BaseModel):
    id: int
    name: str
    table_name: str
    total_rows: int
    columns: list[str]
    created_at: datetime

    class Config:
        from_attributes = True


class CsvUploadedDatasetResponse(CsvDatasetSummaryResponse):
    file_name: str


class CsvMergedDatasetResponse(CsvDatasetSummaryResponse):
    source_dataset_ids: list[int]


class CsvDatasetListResponse(BaseModel):
    uploaded_datasets: list[CsvUploadedDatasetResponse]
    merged_datasets: list[CsvMergedDatasetResponse]


class MergeCsvDatasetsRequest(BaseModel):
    merged_name: str = Field(..., min_length=1, max_length=255)
    source_dataset_ids: list[int] = Field(..., min_length=2)


CsvUploadedDatasetListSuccessResponse = SuccessResponse[list[CsvUploadedDatasetResponse]]
CsvMergedDatasetSuccessResponse = SuccessResponse[CsvMergedDatasetResponse]
CsvDatasetListSuccessResponse = SuccessResponse[CsvDatasetListResponse]
