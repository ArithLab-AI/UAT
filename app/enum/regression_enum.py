from enum import Enum


class RegressionModelType(str, Enum):
    """Model choices for Predictive Regression Analysis (spec Step 4)."""

    AUTO_ML = "auto_ml"
    LINEAR_REGRESSION = "linear_regression"
    DECISION_TREE = "decision_tree"
    RANDOM_FOREST = "random_forest"
    XGBOOST = "xgboost"


class TrainTestSplitType(str, Enum):
    """Train/test split choices for Predictive Regression Analysis (spec Step 5)."""

    SPLIT_80_20 = "split_80_20"
    SPLIT_70_30 = "split_70_30"
    SPLIT_90_10 = "split_90_10"
    TIME_BASED = "time_based"
