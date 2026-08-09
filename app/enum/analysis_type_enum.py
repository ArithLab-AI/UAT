from enum import Enum


class AnalysisType(str, Enum):
    """The 7 basic analysis types defined in the Data Analysis Workflow Specification.

    Top N and Bottom N are separate analysis types per the spec.
    """

    DESCRIPTIVE = "descriptive"
    SIMPLE_DISTRIBUTION = "simple_distribution"
    TOP_N = "top_n"
    BOTTOM_N = "bottom_n"
    TIME_SERIES = "time_series"
    ADVANCED_DISTRIBUTION = "advanced_distribution"
    CORRELATION = "correlation"
