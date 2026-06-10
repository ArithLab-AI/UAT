from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.analysis_schema import (
    DatasetAnalysisRunRequest,
    DatasetAnalysisRunSuccessResponse,
    DatasetAnalysisSuggestionsSuccessResponse,
)
from app.services.analysis_service import (
    get_dataset_analysis_suggestions,
    get_latest_dataset_analysis_suggestions,
    run_dataset_analysis,
)
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


@router.get(
    "/dataset/{dataset_id}/suggestions",
    response_model=DatasetAnalysisSuggestionsSuccessResponse,
    response_model_exclude_none=True,
)
def get_latest_analysis_suggestions_for_dataset(
    dataset_id: int,
    dataset_type: Literal["uploaded", "merged"] = Query(...),
    is_clean: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_latest_dataset_analysis_suggestions(
        db,
        current_user=current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        is_clean=is_clean,
    )
    return success_response("Latest dataset analysis suggestions fetched successfully", data=data)


@router.get(
    "/{analysis_id}/suggestions",
    response_model=DatasetAnalysisSuggestionsSuccessResponse,
    response_model_exclude_none=True,
)
def get_analysis_suggestions(
    analysis_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = get_dataset_analysis_suggestions(
        db,
        current_user=current_user,
        analysis_id=analysis_id,
    )
    return success_response("Dataset analysis suggestions fetched successfully", data=data)
