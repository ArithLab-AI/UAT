from enum import Enum


class ChartType(str, Enum):
    """Chart types supported by the basic analyses per the Data Analysis
    Workflow Specification.

    Which subset applies to each analysis is defined in
    ``app.enum.analysis_chart_config``.
    """

    # Descriptive (spec section 2 — table view only)
    TABLE = "table"

    # Simple Distribution (Bar, Column, Line, Pie, Doughnut, Line Area)
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

    # Correlation (Scatter for X vs Y, Bubble for X/Y/size/color, Heat Map for the
    # pairwise correlation matrix of every selected column)
    SCATTER = "scatter"
    BUBBLE = "bubble"
    HEATMAP = "heatmap"

    # Multi Axis (mixed chart: columns on the primary/left Y axis, a line on the
    # secondary/right Y axis, sharing one X axis)
    MIXED_BAR_LINE = "mixed_bar_line"

    # Geospatial & Location
    CHOROPLETH_MAP = "choropleth_map"
    PIN_MAP = "pin_map"
    HEATMAP_MAP = "heatmap_map"
    BUBBLE_MAP = "bubble_map"
