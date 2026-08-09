from enum import Enum


class ChartType(str, Enum):
    """Chart types supported by the 7 basic analyses per the Data Analysis
    Workflow Specification.

    Which subset applies to each analysis is defined in
    ``app.enum.analysis_chart_config``.
    """

    # Descriptive (spec section 2 — table view only)
    TABLE = "table"

    # Simple Distribution (Bar, Line, Pie, Doughnut, Line Area per spec)
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    DOUGHNUT = "doughnut"
    LINE_AREA = "line_area"

    # Top N / Bottom N (same options as Simple Distribution)
    # (uses BAR, LINE, PIE, DOUGHNUT, LINE_AREA above)

    # Time Series adds
    HORIZONTAL_BAR = "horizontal_bar"
    STEP_LINE = "step_line"

    # Correlation
    SCATTER = "scatter"
    SCATTER_TREND_LINE = "scatter_trend_line"
    CORRELATION_HEATMAP = "correlation_heatmap"
    PAIR_PLOT = "pair_plot"
