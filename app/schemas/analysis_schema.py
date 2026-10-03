from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.common_schema import SuccessResponse

SUGGESTION_ITEM_EXAMPLE = {
    "id": "f3a9b6a0-5d45-4f41-b680-2b81838e2d8b",
    "title": "Missing Values",
    "issue_description": "Column(s) 'email' contain a meaningful share of missing, blank, or placeholder values.",
    "priority": "Medium",
    "resolution_prompt": "Only in column(s) 'email', normalize blank, null-like, and placeholder values to the single token 'N/A'.",
    "cleaning_prompt_type": "missing_value_normalization",
    "target_columns": ["email"],
}

CATEGORIZED_SUGGESTIONS_EXAMPLE = {
    "duplicate": [
        {
            "id": "a87d21d2-78a1-490b-bb65-6b7093ddf641",
            "title": "Duplicate Rows",
            "issue_description": "The sampled dataset contains exact duplicate rows.",
            "priority": "High",
            "resolution_prompt": "Identify exact duplicate records and remove redundant copies while preserving one canonical row for each duplicate set.",
            "cleaning_prompt_type": "duplicate_removal",
            "target_columns": [],
        }
    ],
    "missing_values": [
        SUGGESTION_ITEM_EXAMPLE,
    ],
    "date": [
        {
            "id": "7e994dbc-2b9b-4f42-b1f6-0e8cc533b707",
            "title": "Date Format Issues",
            "issue_description": "Date-like column(s) 'created_at' contain invalid values or inconsistent date formatting.",
            "priority": "High",
            "resolution_prompt": "Only in column(s) 'created_at', parse values with pandas.to_datetime(errors='coerce').",
            "cleaning_prompt_type": "date_normalization",
            "target_columns": ["created_at"],
        }
    ],
}

ANALYSIS_RESPONSE_EXAMPLE = {
    "analysis_id": "1d5cb780-0f7e-4dd4-b0c6-d31ce4d4b5db",
    "dataset_id": 10,
    "dataset_type": "uploaded",
    "is_clean": False,
    "dataset_name": "Customer Import",
    "file_name": "customer_import.csv",
    "quality_score": 72,
    "llm_used": False,
    "suggestion_source": "rule_based",
    "llm_provider": None,
    "llm_model": None,
    "message": "Analysis completed successfully using rule-based suggestions.",
    "source_suggestion_id": None,
    "source_suggestion_resolved": None,
    "source_suggestion_match_count": None,
    "quality_score_delta": None,
    "suggestions": CATEGORIZED_SUGGESTIONS_EXAMPLE,
    "dataset_profile": None,
}

ANALYSIS_SUGGESTIONS_RESPONSE_EXAMPLE = {
    "analysis_id": ANALYSIS_RESPONSE_EXAMPLE["analysis_id"],
    "dataset_id": ANALYSIS_RESPONSE_EXAMPLE["dataset_id"],
    "dataset_type": ANALYSIS_RESPONSE_EXAMPLE["dataset_type"],
    "dataset_name": ANALYSIS_RESPONSE_EXAMPLE["dataset_name"],
    "file_name": ANALYSIS_RESPONSE_EXAMPLE["file_name"],
    "quality_score": ANALYSIS_RESPONSE_EXAMPLE["quality_score"],
    "llm_used": ANALYSIS_RESPONSE_EXAMPLE["llm_used"],
    "suggestion_source": ANALYSIS_RESPONSE_EXAMPLE["suggestion_source"],
    "llm_provider": ANALYSIS_RESPONSE_EXAMPLE["llm_provider"],
    "llm_model": ANALYSIS_RESPONSE_EXAMPLE["llm_model"],
    "message": ANALYSIS_RESPONSE_EXAMPLE["message"],
    "suggestions": CATEGORIZED_SUGGESTIONS_EXAMPLE,
}


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
    model_config = ConfigDict(protected_namespaces=())

    provider: str
    model_id: str
    api_key_configured: bool
    client_ready: bool
    inference_ok: bool
    response_text: str | None = None
    error: str | None = None


class DataSuggestionResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": SUGGESTION_ITEM_EXAMPLE
        }
    )

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
    suggestions: dict[str, list[DataSuggestionResponse]] = Field(default_factory=dict)


class DatasetAnalysisResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": ANALYSIS_RESPONSE_EXAMPLE
        }
    )

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
    suggestions: dict[str, list[DataSuggestionResponse]] = Field(default_factory=dict)
    dataset_profile: dict | None = None


class DatasetAnalysisSuggestionsResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": ANALYSIS_SUGGESTIONS_RESPONSE_EXAMPLE
        }
    )

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
    suggestions: dict[str, list[DataSuggestionResponse]] = Field(default_factory=dict)


DatasetAnalysisRunSuccessResponse = SuccessResponse[DatasetAnalysisResponse]
DatasetAnalysisSuggestionsSuccessResponse = SuccessResponse[DatasetAnalysisSuggestionsResponse]
LLMTestSuccessResponse = SuccessResponse[LLMTestResponse]
