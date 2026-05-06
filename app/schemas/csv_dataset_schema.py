from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field
from app.schemas.common_schema import SuccessResponse

class CsvDatasetSummaryResponse(BaseModel):
    id: int
    name: str
    table_name: str
    storage_key: str | None = None
    file_url: str | None = None
    file_size: int
    total_rows: int
    columns: list[str]
    created_at: datetime

    class Config:
        from_attributes = True


class CsvUploadedDatasetResponse(CsvDatasetSummaryResponse):
    file_name: str
    file_size: int
    is_retention: bool = False
    retention_until: datetime | None = None
    retention_at: datetime | None = None


class CsvMergedSourceDatasetResponse(BaseModel):
    id: int
    file_name: str


class CsvMergedDatasetResponse(CsvDatasetSummaryResponse):
    source_datasets: list[CsvMergedSourceDatasetResponse]


class CsvDatasetListResponse(BaseModel):
    uploaded_datasets: list[CsvUploadedDatasetResponse]
    merged_datasets: list[CsvMergedDatasetResponse]


class MergeJoinColumnMapping(BaseModel):
    left_column: str = Field(..., min_length=1)
    right_column: str = Field(..., min_length=1)


class MergeCsvDatasetsRequest(BaseModel):
    merged_name: str = Field(..., min_length=1, max_length=255)
    source_dataset_ids: list[int] = Field(..., min_length=2)
    merge_type: Literal["inner", "left", "right", "full"] | None = None
    join_columns: list[MergeJoinColumnMapping] | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "merged_name": "Customer Orders",
                "source_dataset_ids": [1, 2],
                "merge_type": "left",
                "join_columns": [
                    {
                        "left_column": "Email",
                        "right_column": "Email",
                    }
                ],
            }
        }


CsvUploadedDatasetListSuccessResponse = SuccessResponse[list[CsvUploadedDatasetResponse]]
CsvMergedDatasetSuccessResponse = SuccessResponse[CsvMergedDatasetResponse]
CsvDatasetListSuccessResponse = SuccessResponse[CsvDatasetListResponse]
