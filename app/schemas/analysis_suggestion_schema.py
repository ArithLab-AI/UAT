from datetime import datetime

from pydantic import BaseModel, Field

from app.schemas.common_schema import SuccessResponse


class AnalysisSuggestionDetailResponse(BaseModel):
    id: str
    analysis_id: str
    source_dataset_id: int
    source_type: str
    created_by_user_id: int
    title: str
    issue_description: str
    priority: str
    resolution_prompt: str
    cleaning_prompt_type: str | None = None
    target_columns: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


AnalysisSuggestionDetailSuccessResponse = SuccessResponse[AnalysisSuggestionDetailResponse]
