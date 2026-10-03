from enum import Enum


class AggregationType(str, Enum):
    """The 7 aggregation options defined in the Data Analysis Workflow Specification.

    Spec section 2 lists: Count, Sum, Average, Median, Minimum, Maximum, Percentage (%).
    """

    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    MEDIAN = "median"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    PERCENTAGE = "percentage"


class TimeGranularity(str, Enum):
    """Time granularity options for Time Series Analysis (spec section 2 - Analysis 5).

    Spec lists: Daily, Weekly, Monthly, Quarterly, Yearly, Auto.
    """

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    AUTO = "auto"
