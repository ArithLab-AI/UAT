from enum import Enum


class ChartType(str, Enum):
    """Chart types supported by the basic analyses per the Data Analysis
    Workflow Specification.

    Which subset applies to each analysis is defined in
    ``app.enum.analysis_chart_config``.
    """

    # Descriptive (spec section 2 — table view only)
    TABLE = "table"

    # Simple Distribution (Bar, Line, Pie, Doughnut, Line Area per spec)
    BAR = "bar"
    COLUMN = "column"
    LINE = "line"
    PIE = "pie"
    DOUGHNUT = "doughnut"
    LINE_AREA = "line_area"

    # Top N / Bottom N (Bar, Column, Line, Line Area only — no table view)

    # Time Series adds
    HORIZONTAL_BAR = "horizontal_bar"
    STEP_LINE = "step_line"

    # Correlation
    SCATTER = "scatter"
    SCATTER_TREND_LINE = "scatter_trend_line"
    CORRELATION_HEATMAP = "correlation_heatmap"
    PAIR_PLOT = "pair_plot"

    # Predictive Regression
    ACTUAL_VS_PREDICTED_SCATTER = "actual_vs_predicted_scatter"
    FEATURE_IMPORTANCE_BAR = "feature_importance_bar"

    # Geospatial & Location
    CHOROPLETH_MAP = "choropleth_map"
    PIN_MAP = "pin_map"
    HEATMAP_MAP = "heatmap_map"
    BUBBLE_MAP = "bubble_map"
