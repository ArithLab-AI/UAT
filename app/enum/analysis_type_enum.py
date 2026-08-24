from enum import Enum


class AnalysisType(str, Enum):
    """The basic analysis types defined in the Data Analysis Workflow Specification.

    Top N and Bottom N are separate analysis types per the spec. Predictive Regression
    and Geospatial & Location are heavier analyses (model training / location
    aggregation) but share this same request/response contract and ``/basic-analysis``
    endpoint rather than getting a separate module.
    """

    DESCRIPTIVE = "descriptive"
    SIMPLE_DISTRIBUTION = "simple_distribution"
    TOP_N = "top_n"
    BOTTOM_N = "bottom_n"
    TIME_SERIES = "time_series"
    ADVANCED_DISTRIBUTION = "advanced_distribution"
    CORRELATION = "correlation"
    PREDICTIVE_REGRESSION = "predictive_regression"
    GEOSPATIAL = "geospatial"
