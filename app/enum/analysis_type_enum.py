from enum import Enum


class AnalysisType(str, Enum):
    """The 6 "basic" analysis types from the Analysis Types Developer Reference doc.

    The doc defines 10 analysis types total; Cohort, Funnel, Comparative, and
    Outlier/Anomaly are "advanced" and intentionally not modeled here.
    """

    DESCRIPTIVE = "descriptive"
    DISTRIBUTION = "distribution"
    TOP_N_BOTTOM_N = "top_n_bottom_n"
    TIME_SERIES = "time_series"
    AGGREGATION = "aggregation"
    CORRELATION = "correlation"
