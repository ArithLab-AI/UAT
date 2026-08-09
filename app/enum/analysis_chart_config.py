"""Static metadata tying each basic AnalysisType to its supported chart types,
default chart, required column roles, and supported aggregations.

Direct code representation of the Data Analysis Workflow Specification
(section 2 — Basic Analysis Module) for the 7 basic analysis types. Drives
both request validation and the ``GET /basic-analysis/types`` metadata
endpoint that a frontend uses to build the pickers.
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


# Aggregation set shared by most spec analyses (Simple Dist, Top/Bottom N,
# Time Series, Advanced Distribution). Spec section 2.
_SPEC_AGGREGATIONS: tuple[AggregationType, ...] = (
    AggregationType.COUNT,
    AggregationType.SUM,
    AggregationType.AVERAGE,
    AggregationType.MEDIAN,
    AggregationType.MINIMUM,
    AggregationType.MAXIMUM,
    AggregationType.PERCENTAGE,
)


ANALYSIS_TYPE_CONFIGS: dict[AnalysisType, AnalysisTypeConfig] = {
    # ── 1. Descriptive Analysis ──────────────────────────────────────────
    # Spec: auto-picks all numeric columns, no user column selection,
    # returns table (Count/Mean/Std/Min/25%/50%/75%/Max), view type = Table.
    AnalysisType.DESCRIPTIVE: AnalysisTypeConfig(
        analysis_type=AnalysisType.DESCRIPTIVE,
        label="Descriptive Analysis",
        tagline="Summary statistics across all numeric columns",
        default_chart_type=ChartType.TABLE,
        supported_chart_types=(ChartType.TABLE,),
        supported_aggregations=(),  # auto — no user aggregation
        column_requirements=(),  # no user column selection
    ),

    # ── 2. Simple Distribution Analysis ──────────────────────────────────
    # Spec: X = categorical only. Groups by X, applies aggregation.
    # Charts: Bar, Line, Pie, Doughnut, Line Area.
    AnalysisType.SIMPLE_DISTRIBUTION: AnalysisTypeConfig(
        analysis_type=AnalysisType.SIMPLE_DISTRIBUTION,
        label="Simple Distribution Analysis",
        tagline="Group by a category and see counts, sums, or percentages",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.LINE,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.LINE_AREA,
        ),
        supported_aggregations=_SPEC_AGGREGATIONS,
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Category to group by",
                              "Region, Product, Category"),
        ),
    ),

    # ── 3. Top N Analysis ────────────────────────────────────────────────
    # Spec: X = categorical (required). Y = numeric (optional).
    # If Y not selected → default to Count of X. Sorted descending, N max = 10.
    AnalysisType.TOP_N: AnalysisTypeConfig(
        analysis_type=AnalysisType.TOP_N,
        label="Top N Analysis",
        tagline="Rank the highest-performing entities by a metric",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.HORIZONTAL_BAR,
            ChartType.LINE,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.LINE_AREA,
        ),
        supported_aggregations=_SPEC_AGGREGATIONS,
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Entity to rank",
                              "Region, Product, Category"),
            ColumnRequirement("y", False, "numeric", "Metric to rank by (optional — defaults to Count of X)",
                              "Sales, Profit"),
        ),
    ),

    # ── 4. Bottom N Analysis ────────────────────────────────────────────
    # Spec: same as Top N but sorted ascending. N max = 10.
    AnalysisType.BOTTOM_N: AnalysisTypeConfig(
        analysis_type=AnalysisType.BOTTOM_N,
        label="Bottom N Analysis",
        tagline="Rank the lowest-performing entities by a metric",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.HORIZONTAL_BAR,
            ChartType.LINE,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.LINE_AREA,
        ),
        supported_aggregations=_SPEC_AGGREGATIONS,
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Entity to rank",
                              "Region, Product, Category"),
            ColumnRequirement("y", False, "numeric", "Metric to rank by (optional — defaults to Count of X)",
                              "Sales, Profit"),
        ),
    ),

    # ── 5. Time Series Analysis ─────────────────────────────────────────
    # Spec: X = date/datetime (required). Y = numeric (optional).
    # If Y not selected → aggregation locks to Count.
    # Charts: Line, Line Area, Bar, Horizontal Bar, Step Line.
    AnalysisType.TIME_SERIES: AnalysisTypeConfig(
        analysis_type=AnalysisType.TIME_SERIES,
        label="Time Series Analysis",
        tagline="Analyze how a metric evolves over time",
        default_chart_type=ChartType.LINE,
        supported_chart_types=(
            ChartType.LINE,
            ChartType.LINE_AREA,
            ChartType.BAR,
            ChartType.HORIZONTAL_BAR,
            ChartType.STEP_LINE,
        ),
        supported_aggregations=_SPEC_AGGREGATIONS,
        column_requirements=(
            ColumnRequirement("x", True, "date", "Date/time axis",
                              "Order Date, Transaction Date, Month"),
            ColumnRequirement("y", False, "numeric", "Metric over time (optional — defaults to Count)",
                              "Sales, Revenue, Quantity"),
        ),
    ),

    # ── 6. Advanced Distribution Analysis / Group By ─────────────────────
    # Spec: X = categorical, Y = numeric MANDATORY.
    AnalysisType.ADVANCED_DISTRIBUTION: AnalysisTypeConfig(
        analysis_type=AnalysisType.ADVANCED_DISTRIBUTION,
        label="Advanced Distribution / Group By",
        tagline="Group by a category and aggregate a numeric measure",
        default_chart_type=ChartType.BAR,
        supported_chart_types=(
            ChartType.BAR,
            ChartType.HORIZONTAL_BAR,
            ChartType.LINE,
            ChartType.PIE,
            ChartType.DOUGHNUT,
            ChartType.LINE_AREA,
        ),
        supported_aggregations=_SPEC_AGGREGATIONS,
        column_requirements=(
            ColumnRequirement("x", True, "categorical", "Grouping dimension",
                              "City, Department, Product"),
            ColumnRequirement("y", True, "numeric", "Metric to aggregate (mandatory)",
                              "Revenue, Orders, Profit"),
        ),
    ),

    # ── 7. Correlation Analysis ─────────────────────────────────────────
    # Spec: multi-select numeric, min 2 required.
    #   Exactly 2 cols → Scatter Plot, Scatter + Trend Line.
    #   3 or more cols → Correlation Heatmap, Pair Plot.
    # Method: Pearson only (per spec section 3).
    AnalysisType.CORRELATION: AnalysisTypeConfig(
        analysis_type=AnalysisType.CORRELATION,
        label="Correlation Analysis",
        tagline="Measure how strongly two or more numeric variables move together",
        default_chart_type=ChartType.SCATTER_TREND_LINE,
        supported_chart_types=(
            ChartType.SCATTER,
            ChartType.SCATTER_TREND_LINE,
            ChartType.CORRELATION_HEATMAP,
            ChartType.PAIR_PLOT,
        ),
        supported_aggregations=(),
        column_requirements=(
            ColumnRequirement("columns", True, "numeric",
                              "2+ numeric columns (2 → scatter; 3+ → heatmap or pair plot)",
                              "Sales, Profit, Quantity"),
        ),
    ),
}


def get_analysis_type_config(analysis_type: AnalysisType) -> AnalysisTypeConfig:
    return ANALYSIS_TYPE_CONFIGS[analysis_type]


def resolve_chart_type(analysis_type: AnalysisType, chart_type: ChartType | None) -> ChartType:
    """Return the requested chart type if valid for this analysis, else the default."""
    config = get_analysis_type_config(analysis_type)
    if chart_type is not None and chart_type in config.supported_chart_types:
        return chart_type
    return config.default_chart_type
