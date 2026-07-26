from enum import Enum


class AggregationType(str, Enum):
    """Aggregation / metric options from the "Supported Aggregations" tables."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    STD_DEV = "std_dev"
    COUNT_DISTINCT = "count_distinct"
    PERCENT_OF_TOTAL = "percent_of_total"
    CUMSUM = "cumsum"
    MOM_PERCENT = "mom_percent"
    YOY_PERCENT = "yoy_percent"
    ROLLING_AVG = "rolling_avg"


class SortDirection(str, Enum):
    TOP = "top"
    BOTTOM = "bottom"


class TimeGranularity(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class CorrelationMethod(str, Enum):
    PEARSON = "pearson"
    SPEARMAN = "spearman"
