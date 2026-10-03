from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from app.schemas.basic_analysis_schema import ChartPayload
from app.schemas.common_schema import SuccessResponse


class SaveChartRequest(BaseModel):
    """The already-prepared chart from ``POST /basic-analysis/run``, plus a title.

    The dashboard stores this payload as-is and serves it straight back — nothing
    is recomputed. Send the fields from the analysis response.
    """

    dataset_id: int = Field(..., ge=1)
    dataset_type: Literal["uploaded", "merged"]
    is_clean: bool = False

    analysis_type: str = Field(..., max_length=50)
    chart_type: str = Field(..., max_length=50)
    row_count_used: int = Field(default=0, ge=0)

    title: Optional[str] = Field(
        default=None,
        max_length=255,
        description="Label shown in the dashboard chart dropdown. Falls back to "
        "analysis_name, then '<analysis_type> - <chart_type>'.",
    )
    analysis_name: Optional[str] = Field(default=None, max_length=200)

    chart: dict[str, Any] = Field(
        ...,
        description="The chart-ready envelope from the analysis response "
        "(response.data.chart).",
    )
    summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    request_payload: dict[str, Any] = Field(
        default_factory=dict,
        description="The original /basic-analysis/run request body. Used to detect "
        "re-saves of the same analysis (updates in place) and to re-run it later.",
    )


class SavedChartSummaryResponse(BaseModel):
    """Lightweight row for the chart dropdown / list."""

    id: str
    title: str
    analysis_type: str
    chart_type: str
    source_dataset_id: int
    source_type: Literal["uploaded", "merged"]
    is_clean: bool
    dataset_name: str
    file_name: str
    row_count_used: int
    created_at: datetime
    updated_at: datetime


class SavedChartDetailResponse(SavedChartSummaryResponse):
    """Full chart payload the frontend renders when a chart is selected."""

    request_payload: dict[str, Any]
    chart: ChartPayload
    summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class DashboardDatasetResponse(BaseModel):
    """One entry in the dataset dropdown, with how many charts it has."""

    source_dataset_id: int
    source_type: Literal["uploaded", "merged"]
    dataset_name: str
    file_name: str
    chart_count: int
    last_chart_created_at: datetime


class DashboardDatasetChartsResponse(DashboardDatasetResponse):
    """A dataset together with every chart saved under it."""

    charts: list[SavedChartSummaryResponse] = Field(default_factory=list)


SaveChartSuccessResponse = SuccessResponse[SavedChartDetailResponse]
SavedChartDetailSuccessResponse = SuccessResponse[SavedChartDetailResponse]
SavedChartListSuccessResponse = SuccessResponse[list[SavedChartSummaryResponse]]
DashboardDatasetListSuccessResponse = SuccessResponse[list[DashboardDatasetResponse]]
DashboardOverviewSuccessResponse = SuccessResponse[list[DashboardDatasetChartsResponse]]


class DashboardSourceDataset(BaseModel):
    """The dataset a Dashboard Builder board was built from."""

    id: int = Field(..., ge=1)
    type: Literal["uploaded", "merged"]
    name: Optional[str] = None
    rows: Optional[int] = None
    columns: Optional[list[str]] = None


class CreateDashboardRequest(BaseModel):
    """A full Dashboard Builder board, saved (or re-saved) as one unit.

    ``widgets``, ``layout_engine`` and ``render_state`` are frontend-owned and
    stored as-is — nothing here is recomputed, matching how
    ``POST /basic-analysis/charts`` treats its ``chart`` payload. Re-saving with
    the same ``client_generated_id`` updates this board in place.
    """

    schema_version: int = 1
    client_generated_id: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)

    source_dataset: DashboardSourceDataset

    layout_engine: Optional[dict[str, Any]] = None
    widgets: list[dict[str, Any]] = Field(default_factory=list)
    render_state: Optional[dict[str, Any]] = None
    selected_widget_id: Optional[str] = None

    created_from: Optional[str] = Field(default=None, max_length=50)
    saved_at: Optional[datetime] = None


class UpdateDashboardRequest(BaseModel):
    """Partial update of an existing board by id (``PUT /dashboard/{id}``).

    Only the fields actually sent are changed; ``client_generated_id`` is
    immutable and cannot be updated here.
    """

    schema_version: Optional[int] = None
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=1000)

    source_dataset: Optional[DashboardSourceDataset] = None

    layout_engine: Optional[dict[str, Any]] = None
    widgets: Optional[list[dict[str, Any]]] = None
    render_state: Optional[dict[str, Any]] = None
    selected_widget_id: Optional[str] = None

    created_from: Optional[str] = Field(default=None, max_length=50)
    saved_at: Optional[datetime] = None


class DashboardSummaryResponse(BaseModel):
    """Lightweight row for the "Your Dashboards" list."""

    id: str
    client_generated_id: str
    name: str
    description: Optional[str] = None
    source_dataset_id: int
    source_type: Literal["uploaded", "merged"]
    source_dataset_name: Optional[str] = None
    widget_count: int
    created_at: datetime
    updated_at: datetime


class DashboardDetailResponse(DashboardSummaryResponse):
    """Full board the Dashboard Builder needs to reopen it."""

    schema_version: int
    source_dataset_columns: list[str] = Field(default_factory=list)
    layout_engine: Optional[dict[str, Any]] = None
    widgets: list[dict[str, Any]] = Field(default_factory=list)
    render_state: Optional[dict[str, Any]] = None
    selected_widget_id: Optional[str] = None
    created_from: Optional[str] = None


class DashboardPaginationMeta(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int


class DashboardListResponse(BaseModel):
    dashboards: list[DashboardSummaryResponse] = Field(default_factory=list)
    pagination: DashboardPaginationMeta


DashboardSuccessResponse = SuccessResponse[DashboardDetailResponse]
DashboardListSuccessResponse = SuccessResponse[DashboardListResponse]
