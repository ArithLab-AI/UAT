"""Shared, generic helpers used across the basic-analysis computation modules
(``basic_analysis_service``, ``predictive_regression_service``,
``geospatial_analysis_service``).

Pulled out of ``basic_analysis_service`` so the newer Predictive Regression and
Geospatial analyses can reuse the same column-typing/coercion/aggregation logic
without a circular import back into that module.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from app.enum.aggregation_type_enum import AggregationType
from app.utils.responses import error_response

# Pandas aggregation names for each spec aggregation.
# COUNT is handled specially (groupby size or column count).
# PERCENTAGE is handled specially (share of total based on sum or count).
PANDAS_AGG: dict[AggregationType, str] = {
    AggregationType.SUM: "sum",
    AggregationType.AVERAGE: "mean",
    AggregationType.MEDIAN: "median",
    AggregationType.MINIMUM: "min",
    AggregationType.MAXIMUM: "max",
}


def round_value(value: Any, ndigits: int = 4) -> float | None:
    if value is None:
        return None
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(as_float) or math.isinf(as_float):
        return None
    return round(as_float, ndigits)


def clean_label(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        as_float = float(value)
        return None if math.isnan(as_float) else as_float
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    return value


def require_column(df: pd.DataFrame, column: str | None, role_label: str) -> str:
    if not column:
        raise error_response(
            status_code=400,
            detail=f"'{role_label}' column is required for this analysis type.",
        )
    if column not in df.columns:
        raise error_response(status_code=400, detail=f"Column '{column}' was not found in the dataset.")
    return column


def numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce")


def datetime_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(df[column], errors="coerce")


def is_numeric_column(df: pd.DataFrame, column: str) -> bool:
    series = df[column]
    if pd.api.types.is_numeric_dtype(series):
        return True
    # CSV-sourced columns are always loaded as object/string dtype (see
    # load_dataset_dataframe), so dtype alone can never detect a numeric column here.
    # Coerce and check how much of the column actually parses as numeric instead.
    non_null = series.dropna()
    non_null = non_null[non_null.astype(str).str.strip() != ""]
    if non_null.empty:
        return False
    coerced = pd.to_numeric(non_null, errors="coerce")
    return coerced.notna().mean() >= 0.6


def is_categorical_column(df: pd.DataFrame, column: str) -> bool:
    dtype = df[column].dtype
    return dtype == object or str(dtype) in ("string", "str", "category", "bool")


def apply_groupby_aggregation(
    df: pd.DataFrame,
    group_col: str,
    value_col: str | None,
    agg: AggregationType,
) -> pd.Series:
    """
    Apply a spec aggregation on df grouped by group_col.

    - value_col=None  -> COUNT of rows per group (used when Y is optional/absent).
    - agg=COUNT       -> count of rows (or non-null values if value_col given).
    - agg=PERCENTAGE  -> share of total, based on SUM if value_col given else COUNT.
    - other aggs      -> standard pandas agg on value_col (numeric).
    """
    if agg == AggregationType.PERCENTAGE:
        if value_col is None:
            counts = df.groupby(group_col, dropna=False).size()
            total = counts.sum()
            return (counts / total * 100) if total > 0 else counts
        working = df.groupby(group_col, dropna=False)[value_col].sum()
        total = float(working.sum())
        return (working / total * 100) if total != 0 else working

    if agg == AggregationType.COUNT or value_col is None:
        if value_col is None:
            return df.groupby(group_col, dropna=False).size()
        return df.groupby(group_col, dropna=False)[value_col].count()

    pandas_agg = PANDAS_AGG[agg]
    numeric = numeric_series(df, value_col)
    working = df[[group_col]].copy()
    working["__value__"] = numeric
    return working.groupby(group_col, dropna=False)["__value__"].agg(pandas_agg)
