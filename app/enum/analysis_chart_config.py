"""Static metadata tying each basic AnalysisType to its supported chart types,
default chart, required column roles, and supported aggregations.

This is a direct code representation of the "Analysis Types Developer Reference"
doc (Required Data Structure / Supported Chart Types / Supported Aggregations
tables) for the 6 basic analysis types only. It drives both request validation
and the ``GET /basic-analysis/types`` metadata endpoint that a frontend uses to
build the analysis-type / chart-type pickers.
"""

from dataclasses import dataclass

from app.enum.aggregation_type_enum import AggregationType
from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType

ColumnDataType = str  # "numeric" | "categorical" | "date" | "any"


@dataclass(frozen=True)
class ColumnRequirement:
    role: str
    required: bool
    data_type: ColumnDataType
    label: str
    example: str


@dataclass(frozen=True)
class AnalysisTypeConfig:
    analysis_type: AnalysisType
    label: str
    tagline: str
    default_chart_type: ChartType
    supported_chart_types: tuple[ChartType, ...]
    supported_aggregations: tuple[AggregationType, ...]
    column_requirements: tuple[ColumnRequirement, ...]


ANALYSIS_TYPE_CONFIGS: dict[AnalysisType, AnalysisTypeConfig] = {
    AnalysisType.DESCRIPTIVE: AnalysisTypeConfig(
        analysis_type=AnalysisType.DESCRIPTIVE,
        label="Descriptive Analysis",
        tagline="Summarize the basic characteristics of your dataset",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.HORIZONTAL_BAR,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.BOX_PLOT,
            ChartType.STATS_TABLE,
        ),
        supported_aggregations=(
            AggregationType.COUNT,
            AggregationType.SUM,
            AggregationType.AVG,
            AggregationType.MEDIAN,
            AggregationType.MIN,
            AggregationType.MAX,
            AggregationType.STD_DEV,
        ),
        column_requirements=(
            ColumnRequirement("x", False, "categorical", "Group / dimension", "Product Category, Region"),
            ColumnRequirement("y", True, "numeric", "Measure to analyze", "Sales Amount, Age, Revenue"),
        ),
    ),
    AnalysisType.DISTRIBUTION: AnalysisTypeConfig(
        analysis_type=AnalysisType.DISTRIBUTION,
        label="Distribution Analysis",
        tagline="Understand how values spread across a range",
        default_chart_type=ChartType.HISTOGRAM,
        supported_chart_types=(
            ChartType.HISTOGRAM,
            ChartType.BOX_PLOT,
            ChartType.VIOLIN_PLOT,
            ChartType.DENSITY_KDE,
            ChartType.HISTOGRAM_CURVE,
            ChartType.CDF,
        ),
        supported_aggregations=(),
        column_requirements=(
            ColumnRequirement("x", True, "numeric", "Value to distribute", "Order Amount, Age, Score"),
            ColumnRequirement("group_by", False, "categorical", "Split by category", "Product Type, Gender"),
        ),
    ),
    AnalysisType.TOP_N_BOTTOM_N: AnalysisTypeConfig(
        analysis_type=AnalysisType.TOP_N_BOTTOM_N,
        label="Top N / Bottom N Analysis",
        tagline="Rank entities and surface the best and worst performers",
        default_chart_type=ChartType.HORIZONTAL_BAR,
        supported_chart_types=(
            ChartType.HORIZONTAL_BAR,
            ChartType.VERTICAL_BAR,
            ChartType.RANKED_TABLE,
            ChartType.PODIUM,
            ChartType.TREEMAP,
            ChartType.DONUT_TOP_N_SHARE,
        ),
        supported_aggregations=(
            AggregationType.SUM,
            AggregationType.COUNT,
            AggregationType.AVG,
            AggregationType.MAX,
        ),
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Entity to rank", "Product Name, Customer ID, Region"),
            ColumnRequirement("y", True, "numeric", "Metric to rank by", "Revenue, Units Sold, Profit"),
        ),
    ),
    AnalysisType.TIME_SERIES: AnalysisTypeConfig(
        analysis_type=AnalysisType.TIME_SERIES,
        label="Time Series Analysis",
        tagline="Analyze how a metric changes over time",
        default_chart_type=ChartType.LINE,
        supported_chart_types=(
            ChartType.LINE,
            ChartType.AREA,
            ChartType.BAR_PERIOD,
            ChartType.MULTI_LINE,
            ChartType.STACKED_AREA_BAR,
            ChartType.CALENDAR_HEATMAP,
        ),
        supported_aggregations=(
            AggregationType.SUM,
            AggregationType.COUNT,
            AggregationType.AVG,
            AggregationType.CUMSUM,
            AggregationType.MOM_PERCENT,
            AggregationType.YOY_PERCENT,
            AggregationType.ROLLING_AVG,
        ),
        column_requirements=(
            ColumnRequirement("x", True, "date", "Time axis", "Order Date, Created At, Month"),
            ColumnRequirement("y", True, "numeric", "Metric over time", "Revenue, Users, Page Views"),
            ColumnRequirement("series", False, "categorical", "Multiple lines", "Region, Product Category"),
        ),
    ),
    AnalysisType.AGGREGATION: AnalysisTypeConfig(
        analysis_type=AnalysisType.AGGREGATION,
        label="Aggregation / Group By Analysis",
        tagline="Slice a metric by one or more categorical dimensions",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.GROUPED_BAR,
            ChartType.STACKED_BAR,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.TREEMAP,
        ),
        supported_aggregations=(
            AggregationType.SUM,
            AggregationType.COUNT,
            AggregationType.COUNT_DISTINCT,
            AggregationType.AVG,
            AggregationType.MIN,
            AggregationType.MAX,
            AggregationType.PERCENT_OF_TOTAL,
        ),
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Grouping dimension", "City, Department, Product"),
            ColumnRequirement("y", True, "numeric", "Metric to aggregate", "Revenue, Orders, Profit"),
            ColumnRequirement("secondary_group", False, "categorical", "Second dimension", "Month, Channel"),
        ),
    ),
    AnalysisType.CORRELATION: AnalysisTypeConfig(
        analysis_type=AnalysisType.CORRELATION,
        label="Correlation Analysis",
        tagline="Measure the relationship strength between two numeric variables",
        default_chart_type=ChartType.SCATTER,
        supported_chart_types=(
            ChartType.SCATTER,
            ChartType.SCATTER_TREND_LINE,
            ChartType.CORRELATION_HEATMAP,
            ChartType.BUBBLE_CHART,
            ChartType.NO_CORRELATION_VIEW,
        ),
        supported_aggregations=(),
        column_requirements=(
            ColumnRequirement("x", True, "numeric", "First variable", "Ad Spend, Temperature"),
            ColumnRequirement("y", True, "numeric", "Second variable", "Revenue, Ice Cream Sales"),
            ColumnRequirement("group_by", False, "categorical", "Color-code points by segment", "Region, Product Type"),
            ColumnRequirement("size", False, "numeric", "Bubble size (Bubble Chart only)", "Deal Count"),
        ),
    ),
}


def get_analysis_type_config(analysis_type: AnalysisType) -> AnalysisTypeConfig:
    return ANALYSIS_TYPE_CONFIGS[analysis_type]


def resolve_chart_type(analysis_type: AnalysisType, chart_type: ChartType | None) -> ChartType:
    """Return the requested chart type if it's valid for this analysis, else the default."""
    config = get_analysis_type_config(analysis_type)
    if chart_type is not None and chart_type in config.supported_chart_types:
        return chart_type
    return config.default_chart_type
