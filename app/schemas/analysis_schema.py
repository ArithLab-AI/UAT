from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common_schema import SuccessResponse


class DatasetAnalysisRunRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "dataset_id": 12,
                "dataset_type": "uploaded",
                "is_clean": False,
            }
        },
    )

    dataset_id: int = Field(..., ge=1)
    dataset_type: Literal["uploaded", "merged"]
    is_clean: bool = False


class LLMTestRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=2000)

    class Config:
        json_schema_extra = {
            "example": {
                "prompt": "What is the capital of France?",
            }
        }


class LLMTestResponse(BaseModel):
    provider: str
    model_id: str
    api_key_configured: bool
    client_ready: bool
    inference_ok: bool
    response_text: str | None = None
    error: str | None = None


class DataSuggestionResponse(BaseModel):
    id: str | None = None
    title: str
    issue_description: str
    priority: str
    resolution_prompt: str
    cleaning_prompt_type: str | None = None
    target_columns: list[str] = Field(default_factory=list)


class DatasetAnalysisPayloadResponse(BaseModel):
    quality_score: int = Field(ge=0, le=100)
    llm_used: bool = False
    suggestion_source: Literal["llm", "rule_based"]
    llm_provider: str | None = None
    llm_model: str | None = None
    suggestions: list[DataSuggestionResponse] = Field(default_factory=list)


class DatasetAnalysisResponse(BaseModel):
    analysis_id: str | None = None
    dataset_id: int
    dataset_type: Literal["uploaded", "merged"]
    is_clean: bool = False
    dataset_name: str
    file_name: str
    quality_score: int = Field(ge=0, le=100)
    llm_used: bool = False
    suggestion_source: Literal["llm", "rule_based"]
    llm_provider: str | None = None
    llm_model: str | None = None
    message: str | None = None
    source_suggestion_id: str | None = None
    source_suggestion_resolved: bool | None = None
    source_suggestion_match_count: int | None = None
    quality_score_delta: int | None = None
    suggestions: list[DataSuggestionResponse] = Field(default_factory=list)
    dataset_profile: dict | None = None


class DatasetAnalysisSuggestionsResponse(BaseModel):
    analysis_id: str
    dataset_id: int
    dataset_type: Literal["uploaded", "merged"]
    dataset_name: str
    file_name: str
    quality_score: int = Field(ge=0, le=100)
    llm_used: bool = False
    suggestion_source: Literal["llm", "rule_based"]
    llm_provider: str | None = None
    llm_model: str | None = None
    message: str | None = None
    suggestions: list[DataSuggestionResponse] = Field(default_factory=list)


DatasetAnalysisRunSuccessResponse = SuccessResponse[DatasetAnalysisResponse]
DatasetAnalysisSuggestionsSuccessResponse = SuccessResponse[DatasetAnalysisSuggestionsResponse]
LLMTestSuccessResponse = SuccessResponse[LLMTestResponse]
