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
    sheet_name: str | None = None
    file_size: int
    is_retention: bool = False
    retention_until: datetime | None = None
    retention_at: datetime | None = None


class CsvMergedSourceDatasetResponse(BaseModel):
    id: int
    file_name: str
    sheet_name: str | None = None


class MultiSheetUploadPendingResponse(BaseModel):
    requires_sheet_selection: bool
    file_token: str | None = None
    file_name: str | None = None
    available_sheets: list[str] | None = None
    sheet_count: int | None = None
    preview_row_count: int | None = None
    pending_files: list["MultiSheetUploadPendingResponse"] | None = None
    uploaded_datasets: list[CsvUploadedDatasetResponse] | None = None


class SelectExcelSheetRequest(BaseModel):
    file_token: str = Field(..., min_length=1)
    sheet_name: str = Field(..., min_length=1)

    class Config:
        json_schema_extra = {
            "example": {
                "file_token": "temporary_upload_token",
                "sheet_name": "Orders",
            }
        }


class CsvMergedDatasetResponse(CsvDatasetSummaryResponse):
    source_datasets: list[CsvMergedSourceDatasetResponse]


class CsvDatasetListResponse(BaseModel):
    uploaded_datasets: list[CsvUploadedDatasetResponse]
    merged_datasets: list[CsvMergedDatasetResponse]


class MergeJoinColumnMapping(BaseModel):
    left_column: str = Field(..., min_length=1)
    right_column: str = Field(..., min_length=1)


class MergeSourceDatasetsRequest(BaseModel):
    source_dataset_ids: list[int] = Field(..., min_length=2, max_length=2)

    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset_ids": [1, 2],
            }
        }


class MergeDatasetInfoResponse(BaseModel):
    id: int
    name: str
    file_name: str
    sheet_name: str | None = None
    table_name: str
    columns: list[str]
    internal_columns: list[str]
    total_rows: int
    metadata: dict


class SuggestedJoinColumnResponse(BaseModel):
    left_column: str
    right_column: str
    confidence: Literal["high", "medium", "low"]


class MergeSuggestionsResponse(BaseModel):
    left_dataset: MergeDatasetInfoResponse
    right_dataset: MergeDatasetInfoResponse
    suggested_join_columns: list[SuggestedJoinColumnResponse]
    supported_merge_types: list[Literal["inner", "left", "right", "full"]]
    merge_type_info: dict[str, str]


class PreviewMergeRequest(MergeSourceDatasetsRequest):
    merge_type: Literal["inner", "left", "right", "full"]
    join_columns: list[MergeJoinColumnMapping] = Field(..., min_length=1)

    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset_ids": [1, 2],
                "merge_type": "left",
                "join_columns": [
                    {
                        "left_column": "CustomerID",
                        "right_column": "CustomerID",
                    }
                ],
            }
        }


class PreviewMergeResponse(BaseModel):
    merged_columns: list[str]
    preview_rows: list[dict]
    preview_row_count: int


class MergeCsvDatasetsRequest(BaseModel):
    merged_name: str = Field(..., min_length=1, max_length=255)
    source_dataset_ids: list[int] = Field(..., min_length=2, max_length=2)
    merge_type: Literal["inner", "left", "right", "full"]
    join_columns: list[MergeJoinColumnMapping] = Field(..., min_length=1)

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


CsvUploadedDatasetListSuccessResponse = SuccessResponse[
    list[CsvUploadedDatasetResponse] | MultiSheetUploadPendingResponse
]
CsvUploadedDatasetSuccessResponse = SuccessResponse[CsvUploadedDatasetResponse]
CsvMergedDatasetSuccessResponse = SuccessResponse[CsvMergedDatasetResponse]
CsvDatasetListSuccessResponse = SuccessResponse[CsvDatasetListResponse]
MergeSuggestionsSuccessResponse = SuccessResponse[MergeSuggestionsResponse]
PreviewMergeSuccessResponse = SuccessResponse[PreviewMergeResponse]
