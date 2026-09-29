from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from app.enum.aggregation_type_enum import (
    AggregationType,
    TimeGranularity,
)
from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType
from app.schemas.common_schema import SuccessResponse


class BasicAnalysisRequest(BaseModel):
    """Basic Analysis request payload — matches the Data Analysis Workflow Specification.

    Which column-role fields are required depends on ``analysis_type``:
    see ``app.enum.analysis_chart_config.ANALYSIS_TYPE_CONFIGS``.
    """

    # ── Header (spec: mandatory) ──
    analysis_name: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Project name for identification (mandatory per spec).",
    )

    # ── Source ──
    dataset_id: int
    dataset_type: Literal["uploaded", "merged"]
    is_clean: bool = False

    # ── Analysis selection ──
    analysis_type: AnalysisType
    chart_type: Optional[ChartType] = None

    # ── Column roles — used by different analyses ──
    x_column: Optional[str] = Field(
        default=None,
        description="X column (categorical for most, date for time series).",
    )
    y_column: Optional[str] = Field(
        default=None,
        description="Y column (numeric). Optional for Top/Bottom N and Time Series. "
        "Primary/left Y axis for Multi Axis.",
    )
    columns: Optional[list[str]] = Field(
        default=None,
        description="For Correlation: 2+ numeric columns.",
    )

    # ── Aggregation (Simple Distribution, Top/Bottom N, Time Series, Advanced
    #    Distribution, Geospatial) ──
    aggregation: Optional[AggregationType] = None

    # ── Top N / Bottom N (spec: max 10) ──
    n: int = Field(default=10, ge=1, le=10)

    # ── Time Series (also used by Multi Axis when X is a date column) ──
    granularity: TimeGranularity = TimeGranularity.MONTHLY

    # ── Multi Axis ──
    # x_column = shared X axis (categorical or date), y_column = primary/left axis
    # (columns), aggregation = primary aggregation (defaults to Sum).
    secondary_y_column: Optional[str] = Field(
        default=None,
        description="Secondary/right Y axis column (numeric, drawn as a line). Required "
        "for Multi Axis analysis.",
    )
    secondary_aggregation: Optional[AggregationType] = Field(
        default=None,
        description="Aggregation for the secondary Y axis in Multi Axis analysis "
        "(defaults to Average — suited to rates, ratios and averages).",
    )

    # ── Geospatial & Location ──
    location_column: Optional[str] = Field(
        default=None,
        description="Location column: City/State/Country/Zip Code, or a Latitude column "
        "when paired with location_column_2. Required for Geospatial analysis.",
    )
    location_column_2: Optional[str] = Field(
        default=None,
        description="Longitude column — only set this alongside location_column when the "
        "dataset stores latitude/longitude as two separate numeric columns.",
    )
    metric_column: Optional[str] = Field(
        default=None,
        description="Metric column to aggregate for Geospatial analysis (numeric, optional "
        "— defaults to Count of records).",
    )
    # Map visualization type reuses chart_type above (CHOROPLETH_MAP / PIN_MAP /
    # HEATMAP_MAP / BUBBLE_MAP) rather than a separate field.


class ColumnRequirementResponse(BaseModel):
    role: str
    required: bool
    data_type: str
    label: str
    example: str


class AnalysisTypeMetadataResponse(BaseModel):
    analysis_type: AnalysisType
    label: str
    tagline: str
    default_chart_type: ChartType
    supported_chart_types: list[ChartType]
    supported_aggregations: list[AggregationType]
    column_requirements: list[ColumnRequirementResponse]


class ChartPayload(BaseModel):
    """Generic chart-ready envelope. Only the fields relevant to the resolved
    chart_type are populated; the rest stay None."""

    chart_type: ChartType
    labels: Optional[list[Any]] = None
    series: Optional[list[dict[str, Any]]] = None
    points: Optional[list[dict[str, Any]]] = None
    table: Optional[list[dict[str, Any]]] = None
    extra: dict[str, Any] = Field(default_factory=dict)


class BasicAnalysisResponse(BaseModel):
    analysis_type: AnalysisType
    chart_type: ChartType
    dataset_id: int
    dataset_type: str
    dataset_name: str
    file_name: str
    row_count_used: int
    chart: ChartPayload
    summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


AnalysisTypesSuccessResponse = SuccessResponse[list[AnalysisTypeMetadataResponse]]
BasicAnalysisRunSuccessResponse = SuccessResponse[BasicAnalysisResponse]
