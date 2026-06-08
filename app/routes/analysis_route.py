from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.analysis_schema import DatasetAnalysisRunRequest, DatasetAnalysisRunSuccessResponse
from app.services.analysis_service import run_dataset_analysis
from app.utils.responses import success_response

router = APIRouter(prefix="/analysis", tags=["Dataset Analysis"])


@router.post(
    "",
    response_model=DatasetAnalysisRunSuccessResponse,
    response_model_exclude_none=True,
)
def analyze_dataset(
    payload: DatasetAnalysisRunRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = run_dataset_analysis(
        db,
        current_user=current_user,
        dataset_id=payload.dataset_id,
        dataset_type=payload.dataset_type,
        is_clean=payload.is_clean,
        use_llm=True,
    )
    data["dataset_profile"] = None
    return success_response("Dataset analysis completed successfully", data=data)
