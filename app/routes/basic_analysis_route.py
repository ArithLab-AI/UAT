import logging

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
from app.schemas.dashboard_schema import SaveChartRequest, SaveChartSuccessResponse
from app.services.basic_analysis_service import list_analysis_type_metadata, run_basic_analysis
from app.services.dashboard_service import save_chart
from app.utils.responses import success_response

router = APIRouter(prefix="/basic-analysis", tags=["Basic Analysis"])
logger = logging.getLogger(__name__)


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
    chart-ready payload for the selected (or default) chart type.

    Computes only — nothing is persisted. Call ``POST /basic-analysis/charts`` with
    the returned payload to pin the chart to the dashboard."""
    data = run_basic_analysis(db, current_user=current_user, request=payload)
    return success_response("Analysis completed successfully", data=data)


@router.post(
    "/charts",
    response_model=SaveChartSuccessResponse,
    response_model_exclude_none=True,
    status_code=201,
)
def save_analysis_chart(
    payload: SaveChartRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Pin a chart built with the manual Basic Analysis flow to the dashboard.

    Send the fields from the ``POST /basic-analysis/run`` response plus an optional
    ``title``. Stored as-is and served straight back — nothing is recomputed.
    Re-saving the same analysis config updates that chart in place instead of
    adding a duplicate. The dashboard reads, refreshes and deletes these; it never
    creates them."""
    data = save_chart(db, current_user=current_user, request=payload)
    logger.info(
        "Saved dashboard chart id=%s dataset=%s:%s for user_id=%s",
        data["id"],
        data["source_type"],
        data["source_dataset_id"],
        current_user.id,
    )
    return success_response("Chart saved successfully", status_code=201, data=data)
