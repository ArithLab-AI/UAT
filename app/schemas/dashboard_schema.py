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
