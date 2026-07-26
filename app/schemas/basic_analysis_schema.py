from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from app.enum.aggregation_type_enum import (
    AggregationType,
    CorrelationMethod,
    SortDirection,
    TimeGranularity,
)
from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType
from app.schemas.common_schema import SuccessResponse


class BasicAnalysisRequest(BaseModel):
    dataset_id: int
    dataset_type: Literal["uploaded", "merged"]
    is_clean: bool = False

    analysis_type: AnalysisType
    chart_type: Optional[ChartType] = None

    # Column roles — which ones are required depends on analysis_type
    # (see app.enum.analysis_chart_config.ANALYSIS_TYPE_CONFIGS).
    x_column: Optional[str] = None
    y_column: Optional[str] = None
    group_by_column: Optional[str] = None
    secondary_group_column: Optional[str] = None
    series_column: Optional[str] = None
    size_column: Optional[str] = None

    aggregation: Optional[AggregationType] = None

    # Top N / Bottom N
    n: int = Field(default=10, ge=1, le=1000)
    direction: SortDirection = SortDirection.TOP

    # Distribution
    bin_count: Optional[int] = Field(default=None, ge=2, le=200)

    # Time Series
    granularity: TimeGranularity = TimeGranularity.MONTHLY
    rolling_window: int = Field(default=3, ge=2, le=365)

    # Correlation
    correlation_method: CorrelationMethod = CorrelationMethod.PEARSON
    heatmap_columns: Optional[list[str]] = None


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
