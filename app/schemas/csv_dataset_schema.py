from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, model_validator
from app.schemas.common_schema import SuccessResponse

class CsvDatasetSummaryResponse(BaseModel):
    id: int
    name: str
    table_name: str
    storage_key: str | None = None
    file_url: str | None = None
    is_clean: bool = False
    clean_file_url: str | None = None
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
    sheet_name: str | None = Field(default=None, min_length=1)
    sheet_names: list[str] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def require_sheet_selection(self):
        selected_sheet_names = self.selected_sheet_names()
        if not selected_sheet_names:
            raise ValueError("Either sheet_name or sheet_names is required")
        normalized_sheet_names = [sheet_name.strip() for sheet_name in selected_sheet_names]
        if any(not sheet_name for sheet_name in normalized_sheet_names):
            raise ValueError("Sheet names cannot be empty")
        if len(set(normalized_sheet_names)) != len(normalized_sheet_names):
            raise ValueError("Sheet names must be unique for each file token")
        return self

    def selected_sheet_names(self) -> list[str]:
        sheet_names = []
        if self.sheet_name is not None:
            sheet_names.append(self.sheet_name)
        if self.sheet_names:
            sheet_names.extend(self.sheet_names)
        return sheet_names

    class Config:
        json_schema_extra = {
            "example": {
                "file_token": "lgNA820xPWbtX9EWXsxdTSjywBDQC93BmkJOgKWAFdU",
                "sheet_names": ["Sheet1", "Sheet2"],
            }
        }


class CsvMergedDatasetResponse(CsvDatasetSummaryResponse):
    file_name: str
    source_datasets: list[CsvMergedSourceDatasetResponse]

class PaginationMetaResponse(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int

class CsvDatasetListUploadedResponse(CsvUploadedDatasetResponse):
    dataset_type: Literal["uploaded"]

class CsvDatasetListMergedResponse(CsvMergedDatasetResponse):
    dataset_type: Literal["merged"]

class CsvDatasetListResponse(BaseModel):
    datasets: list[CsvDatasetListUploadedResponse | CsvDatasetListMergedResponse]
    pagination: PaginationMetaResponse


class MergeJoinColumnMapping(BaseModel):
    left_column: str = Field(..., min_length=1)
    right_column: str = Field(..., min_length=1)


class MultiSourceJoinRequest(BaseModel):
    source_dataset_ids: list[int] = Field(..., min_length=2)
    merge_type: Literal["inner", "left", "right", "full"] | None = None
    join_columns: list[MergeJoinColumnMapping] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def require_join_details_for_multiple_sources(self):
        if self.merge_type is None:
            raise ValueError("Merge type is required when merging source datasets")
        if not self.join_columns:
            raise ValueError("At least one join column is required when merging source datasets")
        return self


class MergeSourceDatasetsRequest(BaseModel):
    source_dataset_ids: list[int] = Field(..., min_length=2, max_length=2)

    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset_ids": [1, 2],
            }
        }


class MergeSuggestionsRequest(BaseModel):
    source_dataset_ids: list[int] = Field(..., min_length=1)

    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset_ids": [1, 2, 3],
            }
        }


class MergeDatasetColumnResponse(BaseModel):
    name: str
    isSuggested: bool
    confidence: Literal["high", "medium", "low"] | None = None


class MergeDatasetInfoResponse(BaseModel):
    id: int
    name: str
    file_name: str
    sheet_name: str | None = None
    table_name: str
    columns: list[MergeDatasetColumnResponse]
    total_rows: int
    metadata: dict


class SupportedMergeTypeResponse(BaseModel):
    type: Literal["inner", "left", "right", "full"]
    description: str


class MergeJoinSuggestionResponse(BaseModel):
    left_dataset_id: int
    right_dataset_id: int
    left_column: str
    right_column: str
    confidence: Literal["high", "medium", "low"]


class MergeSuggestionsResponse(BaseModel):
    left_dataset: MergeDatasetInfoResponse | None = None
    right_dataset: MergeDatasetInfoResponse | None = None
    source_datasets: list[MergeDatasetInfoResponse]
    join_suggestions: list[MergeJoinSuggestionResponse]
    supported_merge_types: list[SupportedMergeTypeResponse]


class PreviewMergeRequest(MultiSourceJoinRequest):
    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset_ids": [1, 2, 3],
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


class MergeCsvDatasetsRequest(MultiSourceJoinRequest):
    merged_name: str = Field(..., min_length=1, max_length=255)

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
