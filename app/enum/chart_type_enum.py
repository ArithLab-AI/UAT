from enum import Enum


class ChartType(str, Enum):
    """Every chart type supported by the 6 basic analysis types.

    Values mirror the "Supported Chart Types" cards in the Analysis Types
    Developer Reference doc. Which subset is valid for a given analysis type
    is defined in ``app.enum.analysis_chart_config``.
    """

    # Descriptive
    BAR = "bar"
    HORIZONTAL_BAR = "horizontal_bar"
    PIE = "pie"
    DOUGHNUT = "doughnut"
    BOX_PLOT = "box_plot"
    STATS_TABLE = "stats_table"

    # Distribution
    HISTOGRAM = "histogram"
    VIOLIN_PLOT = "violin_plot"
    DENSITY_KDE = "density_kde"
    HISTOGRAM_CURVE = "histogram_curve"
    CDF = "cdf"

    # Top N / Bottom N
    VERTICAL_BAR = "vertical_bar"
    RANKED_TABLE = "ranked_table"
    PODIUM = "podium"
    TREEMAP = "treemap"
    DONUT_TOP_N_SHARE = "donut_top_n_share"

    # Time Series
    LINE = "line"
    AREA = "area"
    BAR_PERIOD = "bar_period"
    MULTI_LINE = "multi_line"
    STACKED_AREA_BAR = "stacked_area_bar"
    CALENDAR_HEATMAP = "calendar_heatmap"

    # Aggregation / Group By
    GROUPED_BAR = "grouped_bar"
    STACKED_BAR = "stacked_bar"

    # Correlation
    SCATTER = "scatter"
    SCATTER_TREND_LINE = "scatter_trend_line"
    CORRELATION_HEATMAP = "correlation_heatmap"
    BUBBLE_CHART = "bubble_chart"
    NO_CORRELATION_VIEW = "no_correlation_view"
