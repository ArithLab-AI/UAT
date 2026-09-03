import logging
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config.deps import get_current_user
from app.db.database import get_db
from app.models.auth_models import User
from app.schemas.common_schema import MessageSuccessResponse
from app.schemas.dashboard_schema import (
    DashboardDatasetListSuccessResponse,
    DashboardOverviewSuccessResponse,
    SaveChartRequest,
    SaveChartSuccessResponse,
    SavedChartDetailSuccessResponse,
    SavedChartListSuccessResponse,
)
# ``POST /dashboard/charts`` moved to ``POST /basic-analysis/charts`` — chart
# creation now lives with the analysis flow. This router only reads, refreshes,
# renames and deletes.
from app.services.dashboard_service import (
    delete_saved_chart,
    get_dashboard_overview,
    get_saved_chart,
    list_dashboard_datasets,
    list_saved_charts,
    refresh_saved_chart,
    update_saved_chart,
)
from app.utils.responses import success_response

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])
logger = logging.getLogger(__name__)


@router.get(
    "/datasets",
    response_model=DashboardDatasetListSuccessResponse,
    response_model_exclude_none=True,
)
def get_dashboard_datasets(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Datasets the user has saved at least one chart for — the dataset dropdown."""
    data = list_dashboard_datasets(db, current_user=current_user)
    return success_response("Dashboard datasets fetched successfully", data=data)


@router.get(
    "/overview",
    response_model=DashboardOverviewSuccessResponse,
    response_model_exclude_none=True,
)
def get_dashboard_overview_route(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Every dataset with its saved charts nested — populates both dropdowns in one call."""
    data = get_dashboard_overview(db, current_user=current_user)
    return success_response("Dashboard overview fetched successfully", data=data)


@router.get(
    "/charts",
    response_model=SavedChartListSuccessResponse,
    response_model_exclude_none=True,
)
def get_saved_charts(
    source_dataset_id: Optional[int] = Query(
        default=None, description="Filter to charts saved for this dataset id."
    ),
    source_type: Optional[Literal["uploaded", "merged"]] = Query(
        default=None, description="Filter to uploaded or merged datasets."
    ),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Saved charts for the user, optionally scoped to one dataset — the chart dropdown."""
    data = list_saved_charts(
        db,
        current_user=current_user,
        source_dataset_id=source_dataset_id,
        source_type=source_type,
    )
    return success_response("Saved charts fetched successfully", data=data)


@router.get(
    "/charts/{chart_id}",
    response_model=SavedChartDetailSuccessResponse,
    response_model_exclude_none=True,
)
def get_saved_chart_detail(
    chart_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Full chart payload for the selected chart — what the frontend renders."""
    data = get_saved_chart(db, current_user=current_user, chart_id=chart_id)
    return success_response("Saved chart fetched successfully", data=data)


@router.put(
    "/charts/{chart_id}",
    response_model=SaveChartSuccessResponse,
    response_model_exclude_none=True,
)
def update_saved_chart_route(
    chart_id: str,
    payload: SaveChartRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Replace a saved chart by id with a client-supplied payload.

    Same body as ``POST /basic-analysis/charts``. Stored as-is; nothing is
    recomputed. Use this for metadata-only edits (e.g. renaming via ``title``);
    to pull fresh data from the dataset use ``POST /charts/{chart_id}/refresh``."""
    data = update_saved_chart(
        db, current_user=current_user, chart_id=chart_id, request=payload
    )
    logger.info("Updated dashboard chart id=%s for user_id=%s", chart_id, current_user.id)
    return success_response("Saved chart updated successfully", data=data)


@router.post(
    "/charts/{chart_id}/refresh",
    response_model=SavedChartDetailSuccessResponse,
    response_model_exclude_none=True,
)
def refresh_saved_chart_route(
    chart_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Re-run this chart's original analysis against the current dataset and
    overwrite the stored snapshot server-side.

    No request body — the analysis config saved with the chart is replayed. The
    chart keeps its id and title. Returns 409 if the chart has no saved config,
    or 400/404 (with the existing snapshot left intact) if the dataset changed
    so much the analysis can no longer run."""
    data = refresh_saved_chart(db, current_user=current_user, chart_id=chart_id)
    logger.info("Refreshed dashboard chart id=%s for user_id=%s", chart_id, current_user.id)
    return success_response("Saved chart refreshed successfully", data=data)


@router.delete(
    "/charts/{chart_id}",
    response_model=MessageSuccessResponse,
    response_model_exclude_none=True,
)
def delete_saved_chart_route(
    chart_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    delete_saved_chart(db, current_user=current_user, chart_id=chart_id)
    logger.info("Deleted dashboard chart id=%s for user_id=%s", chart_id, current_user.id)
    return success_response("Saved chart deleted successfully", data=None)
