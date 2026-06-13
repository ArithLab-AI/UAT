from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.analysis_models import AnalysisSuggestion
from app.models.auth_models import User
from app.schemas.analysis_suggestion_schema import AnalysisSuggestionDetailSuccessResponse
from app.services.analysis_suggestion_title_service import build_suggestion_title
from app.utils.responses import error_response, success_response

router = APIRouter(prefix="/analysis/suggestions", tags=["Dataset Analysis"])


@router.get(
    "/{suggestion_id}",
    response_model=AnalysisSuggestionDetailSuccessResponse,
    response_model_exclude_none=True,
)
def get_analysis_suggestion_detail(
    suggestion_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    suggestion = (
        db.query(AnalysisSuggestion)
        .filter(
            AnalysisSuggestion.id == suggestion_id,
            AnalysisSuggestion.created_by_user_id == current_user.id,
        )
        .first()
    )
    if suggestion is None:
        raise error_response(status_code=404, detail="Analysis suggestion not found")

    return success_response(
        "Analysis suggestion fetched successfully",
        data={
            "id": suggestion.id,
            "analysis_id": suggestion.analysis_id,
            "source_dataset_id": suggestion.source_dataset_id,
            "source_type": suggestion.source_type,
            "created_by_user_id": suggestion.created_by_user_id,
            "title": build_suggestion_title(
                cleaning_prompt_type=suggestion.cleaning_prompt_type,
                issue_description=suggestion.issue_description,
            ),
            "issue_description": suggestion.issue_description,
            "priority": suggestion.priority,
            "resolution_prompt": suggestion.resolution_prompt,
            "cleaning_prompt_type": suggestion.cleaning_prompt_type,
            "target_columns": suggestion.target_columns or [],
            "created_at": suggestion.created_at,
            "updated_at": suggestion.updated_at,
        },
    )
