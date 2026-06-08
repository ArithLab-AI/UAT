from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.schemas.analysis_schema import DataSuggestionResponse
from app.schemas.common_schema import SuccessResponse


class AICleaningRunRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "dataset_id": 2,
                "dataset_type": "uploaded",
                "suggestion_id": "efbe7944-b9d3-4b13-a9b9-023991df3fff",
            }
        }
    )

    dataset_id: int = Field(..., ge=1)
    dataset_type: Literal["uploaded", "merged"]
    suggestion_id: str = Field(
        ...,
        min_length=1,
        max_length=36,
        description="Analysis suggestion ID. The matching resolution_prompt is used internally for cleaning.",
    )
    source_ai_job_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=36,
        description=(
            "Optional AI cleaning job ID to continue updating a specific AI-cleaned file. "
            "If omitted, the latest AI-cleaned file for the same dataset is reused automatically."
        ),
    )

    @field_validator("suggestion_id", "source_ai_job_id", mode="before")
    @classmethod
    def _normalize_optional_strings(cls, value):
        if value is None:
            return None
        if not isinstance(value, str):
            return value

        normalized = value.strip()
        if not normalized:
            return None
        if normalized.lower() in {"null", "none"}:
            return None
        if normalized == "string":
            return None
        return normalized


class AICleaningPayloadResponse(BaseModel):
    job_id: str
    source_dataset_id: int
    source_dataset_type: Literal["uploaded", "merged"]
    source_suggestion_id: str | None = None
    source_ai_job_id: str | None = None
    ai_cleaning_type: bool = True
    status: str
    prompt: str | None = None
    source_type: Literal["raw", "clean"] = "raw"
    cleaning_strategy: str | None = None
    llm_used: bool = False
    target_columns: list[str] = Field(default_factory=list)
    cleaned_file_url: str
    message: str | None = None
    cleaned_rows: int = 0
    preview_rows_returned: int = 0
    preview_limited: bool = False
    changes_detected: bool = False
    cleaned_data: list[dict] = Field(default_factory=list)


class AICleaningDetailResponse(AICleaningPayloadResponse):
    source_dataset_name: str | None = None
    source_file_name: str | None = None
    cleaned_file_path: str | None = None
    quality_score: int | None = None
    analysis: dict | None = None
    created_at: str | None = None
    updated_at: str | None = None


class AICleaningAnalysisRequest(BaseModel):
    use_llm: bool = True
    llm_provider: Literal["openai"] | None = None
    llm_model: str | None = Field(default=None, min_length=1, max_length=255)
    include_profile: bool = False


class AICleaningAnalysisResponse(BaseModel):
    job_id: str
    source_dataset_id: int
    source_dataset_type: Literal["uploaded", "merged"]
    source_type: Literal["clean"] = "clean"
    quality_score: int = Field(ge=0, le=100)
    llm_used: bool = False
    suggestion_source: Literal["llm", "rule_based"]
    llm_provider: str | None = None
    llm_model: str | None = None
    message: str | None = None
    suggestions: list[DataSuggestionResponse] = Field(default_factory=list)
    dataset_profile: dict | None = None


AICleaningRunSuccessResponse = SuccessResponse[AICleaningPayloadResponse]
AICleaningDetailSuccessResponse = SuccessResponse[AICleaningDetailResponse]
AICleaningAnalysisSuccessResponse = SuccessResponse[AICleaningAnalysisResponse]
