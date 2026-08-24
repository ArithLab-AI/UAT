from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.basic_analysis_schema import (
    AnalysisTypesSuccessResponse,
    BasicAnalysisRequest,
    BasicAnalysisRunSuccessResponse,
)
from app.services.basic_analysis_service import list_analysis_type_metadata, run_basic_analysis
from app.utils.responses import success_response

router = APIRouter(prefix="/basic-analysis", tags=["Basic Analysis"])


@router.get(
    "/types",
    response_model=AnalysisTypesSuccessResponse,
    response_model_exclude_none=True,
)
def get_analysis_types():
    """List the supported basic analysis types with their default/supported chart
    types, supported aggregations, and required column roles — everything a UI
    needs to render the analysis-type and chart-type pickers."""
    data = list_analysis_type_metadata()
    return success_response("Analysis types fetched successfully", data=data)


@router.post(
    "/run",
    response_model=BasicAnalysisRunSuccessResponse,
    response_model_exclude_none=True,
)
def run_analysis(
    payload: BasicAnalysisRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Run one of the supported basic analysis types against a dataset and return a
    chart-ready payload for the selected (or default) chart type."""
    data = run_basic_analysis(db, current_user=current_user, request=payload)
    return success_response("Analysis completed successfully", data=data)
