"""Computation engine for the basic analysis types defined in the
Data Analysis Workflow Specification.

Analyses:
  1. Descriptive              - auto stats table across all numeric columns
  2. Simple Distribution      - group by X (categorical), aggregate
  3. Top N                    - rank descending, N max 10
  4. Bottom N                 - rank ascending, N max 10
  5. Time Series              - resample X (date) by granularity, aggregate Y
  6. Advanced Distribution    - group by X, aggregate Y (Y mandatory)
  7. Correlation              - Pearson only; user picks scatter, bubble or heat map
  8. Multi Axis               - shared X (category/date), primary Y as columns on the
                                 left axis, secondary Y as a line on the right axis
  9. Geospatial & Location    - aggregate a metric per location / lat-long point
                                 (see geospatial_analysis_service.py)

Reuses the existing dataset-resolution/download plumbing from ``analysis_service``
and ``data_chat_query_engine`` (dataset -> pandas DataFrame) rather than duplicating
it. Every computation here is read-only.
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.enum.aggregation_type_enum import AggregationType, TimeGranularity
from app.enum.analysis_chart_config import ANALYSIS_TYPE_CONFIGS, resolve_chart_type
from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType
from app.models.auth_models import User
from app.schemas.basic_analysis_schema import BasicAnalysisRequest, ChartPayload
from app.services.analysis_service import _resolve_analysis_source
from app.services.basic_analysis_helpers import (
    PANDAS_AGG as _PANDAS_AGG,
    apply_groupby_aggregation as _apply_groupby_aggregation,
    clean_label as _clean_label,
    datetime_series as _datetime_series,
    is_categorical_column as _is_categorical_column,
    is_numeric_column as _is_numeric_column,
    numeric_series as _numeric_series,
    require_column as _require_column,
    round_value as _round,
)
from app.services.data_chat_query_engine import load_dataset_dataframe
from app.services.geospatial_analysis_service import _compute_geospatial
from app.utils.responses import error_response


MAX_POINTS = 2000       # cap for scatter plots
MAX_BUBBLES = 500       # cap for bubble charts (too many bubbles become unreadable)
MAX_GROUPS = 100        # cap for group counts on charts

_PANDAS_FREQ: dict[TimeGranularity, str] = {
    TimeGranularity.DAILY: "D",
    TimeGranularity.WEEKLY: "W",
    TimeGranularity.MONTHLY: "ME",
    TimeGranularity.QUARTERLY: "QE",
    TimeGranularity.YEARLY: "YE",
}


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------
# _round / _clean_label / _require_column / _numeric_series / _datetime_series /
# _is_numeric_column / _is_categorical_column / _apply_groupby_aggregation / _PANDAS_AGG
# now live in basic_analysis_helpers.py (imported above) so geospatial_analysis_service
# can reuse them without importing this module.


def _auto_granularity_freq(dates: pd.Series) -> tuple[str, TimeGranularity]:
    """Pick sensible pandas frequency and enum granularity from date range."""
    span_days = (dates.max() - dates.min()).days
    if span_days <= 60:
        return "D", TimeGranularity.DAILY
    if span_days <= 365:
        return "W", TimeGranularity.WEEKLY
    if span_days <= 365 * 3:
        return "ME", TimeGranularity.MONTHLY
    if span_days <= 365 * 10:
        return "QE", TimeGranularity.QUARTERLY
    return "YE", TimeGranularity.YEARLY


def _correlation_strength(r: float | None) -> str:
    if r is None:
        return "unknown"
    abs_r = abs(r)
    if abs_r < 0.2:
        return "no correlation"
    if abs_r >= 0.7:
        return "strong positive" if r > 0 else "strong negative"
    if abs_r >= 0.4:
        return "moderate positive" if r > 0 else "moderate negative"
    return "weak positive" if r > 0 else "weak negative"


# ---------------------------------------------------------------------------
# 1. Descriptive Analysis
# ---------------------------------------------------------------------------
# Spec: auto-picks ALL numeric columns. No user column selection.
# Backend: df.describe().T.reset_index()
# Output: Column, Count, Mean, Std, Min, 25%, 50%, 75%, Max
# View: Table only.

def _compute_descriptive(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    numeric_cols = [c for c in df.columns if _is_numeric_column(df, c)]
    if not numeric_cols:
        raise error_response(status_code=400, detail="No numeric columns found in the dataset.")

    numeric_df = pd.DataFrame({col: _numeric_series(df, col) for col in numeric_cols})

    described = numeric_df.describe().T.reset_index().rename(columns={"index": "column"})

    table: list[dict[str, Any]] = []
    for _, row in described.iterrows():
        table.append({
            "column": str(row["column"]),
            "count": int(row["count"]) if pd.notna(row["count"]) else 0,
            "mean": _round(row["mean"]),
            "std": _round(row["std"]),
            "min": _round(row["min"]),
            "25%": _round(row["25%"]),
            "50%": _round(row["50%"]),
            "75%": _round(row["75%"]),
            "max": _round(row["max"]),
        })

    chart = ChartPayload(chart_type=ChartType.TABLE, table=table)
    summary = {"columns_analyzed": len(table), "total_rows": int(len(df))}
    return chart, summary, []


# ---------------------------------------------------------------------------
# 2. Simple Distribution
# ---------------------------------------------------------------------------
# Spec: X = categorical only. Aggregation on X grouped by X itself.
# Backend: df.groupby(X)[X].agg(agg_func)
# Charts: Bar, Column, Line, Pie, Doughnut, Line Area.

def _compute_simple_distribution(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    if not _is_categorical_column(df, x_col):
        raise error_response(status_code=400, detail=f"'{x_col}' must be categorical for Simple Distribution.")

    agg = req.aggregation or AggregationType.COUNT
    # Simple distribution operates on X only; without a numeric Y, only
    # COUNT and PERCENTAGE make sense - anything else silently degrades to COUNT.
    if agg not in (AggregationType.COUNT, AggregationType.PERCENTAGE):
        agg = AggregationType.COUNT

    grouped = _apply_groupby_aggregation(df, x_col, None, agg).dropna().sort_values(ascending=False)

    warnings: list[str] = []
    if len(grouped) > MAX_GROUPS:
        warnings.append(f"Result truncated to top {MAX_GROUPS} groups by value.")
        grouped = grouped.iloc[:MAX_GROUPS]

    labels = [_clean_label(k) for k in grouped.index.tolist()]
    values = [_round(v) for v in grouped.tolist()]

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({x_col})", "data": values}],
    )
    summary = {
        "x_column": x_col,
        "aggregation": agg.value,
        "total_categories": int(len(grouped)),
    }
    return chart, summary, warnings


# ---------------------------------------------------------------------------
# 3 & 4. Top N and Bottom N Analysis
# ---------------------------------------------------------------------------
# Spec: X=Categorical (required). Y=Numeric (optional).
#       If Y not given -> default to Count of X. N max = 10.
# Backend: df.groupby(X)[Y].agg(agg_func).nlargest(N) / nsmallest(N)

def _compute_top_bottom_n(
    df: pd.DataFrame,
    req: BasicAnalysisRequest,
    chart_type: ChartType,
    direction: str,
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    if not _is_categorical_column(df, x_col):
        raise error_response(status_code=400, detail=f"'{x_col}' must be categorical for Top/Bottom N.")

    y_col = req.y_column if req.y_column else None
    if y_col is not None:
        if y_col not in df.columns:
            raise error_response(status_code=400, detail=f"Column '{y_col}' was not found in the dataset.")
        if not _is_numeric_column(df, y_col):
            raise error_response(status_code=400, detail=f"'{y_col}' must be numeric for Top/Bottom N.")

    agg = req.aggregation or (AggregationType.COUNT if y_col is None else AggregationType.SUM)
    # Y required for numeric-aggregation types; if Y missing -> force COUNT.
    if y_col is None and agg not in (AggregationType.COUNT, AggregationType.PERCENTAGE):
        agg = AggregationType.COUNT

    grouped = _apply_groupby_aggregation(df, x_col, y_col, agg).dropna()

    n = min(max(1, int(req.n)), 10)  # spec: N max = 10
    ranked = grouped.nlargest(n) if direction == "top" else grouped.nsmallest(n)

    ranking = [
        {"rank": i + 1, "category": _clean_label(k), "value": _round(v)}
        for i, (k, v) in enumerate(ranked.items())
    ]

    labels = [row["category"] for row in ranking]
    values = [row["value"] for row in ranking]
    y_label = f"{agg.value}({y_col})" if y_col else f"count({x_col})"

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": y_label, "data": values}],
    )
    summary = {
        "x_column": x_col,
        "y_column": y_col,
        "aggregation": agg.value,
        "direction": direction,
        "n": n,
        "total_categories": int(len(grouped)),
        "ranking": ranking,
    }
    return chart, summary, []


def _compute_top_n(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    return _compute_top_bottom_n(df, req, chart_type, direction="top")


def _compute_bottom_n(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    return _compute_top_bottom_n(df, req, chart_type, direction="bottom")


# ---------------------------------------------------------------------------
# 5. Time Series Analysis
# ---------------------------------------------------------------------------
# Spec: X=Date (required), Y=Numeric (optional).
#       If Y not selected -> aggregation locked to Count.
# Backend: df.set_index(X).resample(granularity)[Y].agg(agg_func)
# Charts: Line, Line Area, Bar, Horizontal Bar, Step Line.

def _compute_time_series(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")

    working = df[[x_col] + ([req.y_column] if req.y_column and req.y_column in df.columns else [])].copy()
    working[x_col] = _datetime_series(working, x_col)
    working = working.dropna(subset=[x_col])
    if working.empty:
        raise error_response(status_code=400, detail=f"No valid dates found in '{x_col}'.")

    # Granularity: AUTO -> derive from date range
    if req.granularity == TimeGranularity.AUTO:
        freq, used_granularity = _auto_granularity_freq(working[x_col])
    else:
        freq = _PANDAS_FREQ.get(req.granularity)
        if not freq:
            raise error_response(status_code=400, detail=f"Unknown granularity: {req.granularity}")
        used_granularity = req.granularity

    y_col = req.y_column if req.y_column else None
    if y_col is not None:
        if not _is_numeric_column(working, y_col):
            raise error_response(status_code=400, detail=f"'{y_col}' must be numeric for Time Series.")
        # Coerce: CSV-sourced columns load as strings, so without this sum concatenates
        # text, mean/median/percentage crash, and min/max compare alphabetically.
        working[y_col] = _numeric_series(working, y_col)

    agg = req.aggregation or (AggregationType.COUNT if y_col is None else AggregationType.SUM)
    if y_col is None and agg not in (AggregationType.COUNT, AggregationType.PERCENTAGE):
        agg = AggregationType.COUNT

    # Resample
    resampled = working.set_index(x_col).resample(freq)

    if y_col is None:
        series_values = resampled.size()
    elif agg == AggregationType.PERCENTAGE:
        sums = resampled[y_col].sum()
        total = float(sums.sum())
        series_values = (sums / total * 100) if total != 0 else sums
    elif agg == AggregationType.COUNT:
        series_values = resampled[y_col].count()
    else:
        pandas_agg = _PANDAS_AGG[agg]
        series_values = resampled[y_col].agg(pandas_agg)

    series_values = series_values.dropna()

    labels = [ts.strftime("%Y-%m-%d") for ts in series_values.index]
    values = [_round(v) for v in series_values.tolist()]
    y_label = f"{agg.value}({y_col})" if y_col else f"count({x_col})"

    # Simple trend detection
    trend = "flat"
    if len(values) >= 2 and values[0] is not None and values[-1] is not None and values[0] != 0:
        change_pct = ((values[-1] - values[0]) / abs(values[0])) * 100
        if change_pct > 5:
            trend = "increasing"
        elif change_pct < -5:
            trend = "decreasing"

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": y_label, "data": values}],
    )
    summary = {
        "x_column": x_col,
        "y_column": y_col,
        "aggregation": agg.value,
        "granularity": used_granularity.value,
        "trend": trend,
        "data_points": len(values),
    }
    return chart, summary, []


# ---------------------------------------------------------------------------
# 6. Advanced Distribution
# ---------------------------------------------------------------------------
# Spec: X=Categorical (required), Y=Numeric (MANDATORY).
# Backend: df.groupby(X)[Y].agg(agg_func)

def _compute_advanced_distribution(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    y_col = _require_column(df, req.y_column, "y")

    if not _is_categorical_column(df, x_col):
        raise error_response(status_code=400, detail=f"'{x_col}' must be categorical for Advanced Distribution.")
    if not _is_numeric_column(df, y_col):
        raise error_response(status_code=400, detail=f"'{y_col}' must be numeric for Advanced Distribution.")

    agg = req.aggregation or AggregationType.SUM
    grouped = _apply_groupby_aggregation(df, x_col, y_col, agg).dropna().sort_values(ascending=False)

    warnings: list[str] = []
    if len(grouped) > MAX_GROUPS:
        warnings.append(f"Result truncated to top {MAX_GROUPS} groups by value.")
        grouped = grouped.iloc[:MAX_GROUPS]

    labels = [_clean_label(k) for k in grouped.index.tolist()]
    values = [_round(v) for v in grouped.tolist()]

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({y_col})", "data": values}],
    )
    summary = {
        "x_column": x_col,
        "y_column": y_col,
        "aggregation": agg.value,
        "total_groups": int(len(grouped)),
    }
    return chart, summary, warnings


# ---------------------------------------------------------------------------
# 7. Correlation Analysis
# ---------------------------------------------------------------------------
# Multi-select numeric columns. Pearson only.
# The user picks ONE chart type and only that chart is built:
#   Scatter Plot -> exactly 2 cols: X (independent) vs Y (dependent)
#   Bubble Chart -> 3 or 4 cols: X, Y, 3rd col = bubble size, 4th col = bubble color
#   Heat Map     -> 2 to 10 cols: pairwise correlation matrix of every selected column
# No chart_type -> picked from the column count (2 -> scatter, 3-4 -> bubble, 5+ -> heat map).

_CORRELATION_COLUMN_LIMITS: dict[ChartType, tuple[int, int]] = {
    ChartType.SCATTER: (2, 2),
    ChartType.BUBBLE: (3, 4),
    ChartType.HEATMAP: (2, 10),
}


def _correlation_pairs(corr: pd.DataFrame, cols: list[str]) -> list[dict[str, Any]]:
    """Every unique column pair from a correlation matrix, strongest first."""
    pairs: list[dict[str, Any]] = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            v = corr.iloc[i, j]
            if pd.isna(v):
                continue
            pairs.append({
                "column_a": cols[i],
                "column_b": cols[j],
                "correlation": float(v),
                "r_squared": round(float(v) * float(v), 4),
                "strength": _correlation_strength(float(v)),
            })
    pairs.sort(key=lambda p: abs(p["correlation"]), reverse=True)
    return pairs


def _xy_stats(r: float, col_x: str, col_y: str) -> dict[str, Any]:
    return {
        "r": _round(r),
        "r_squared": _round(r * r),
        "strength": _correlation_strength(None if pd.isna(r) else r),
        "x_column": col_x,
        "y_column": col_y,
    }


def _value_range(series: pd.Series) -> dict[str, float | None]:
    return {"min": _round(float(series.min())), "max": _round(float(series.max()))}


def _correlation_scatter(
    pair: pd.DataFrame, col_x: str, col_y: str, r: float, warnings: list[str]
) -> ChartPayload:
    """Chart 1 - Scatter: direct linear relationship between X and Y."""
    sample = pair.sample(MAX_POINTS, random_state=42) if len(pair) > MAX_POINTS else pair
    if len(pair) > MAX_POINTS:
        warnings.append(f"Scatter sampled down to {MAX_POINTS} points for performance.")

    points = [
        {"x": _round(float(row[col_x])), "y": _round(float(row[col_y]))}
        for _, row in sample.iterrows()
    ]
    extra = {"title": "Scatter Plot", **_xy_stats(r, col_x, col_y)}
    return ChartPayload(chart_type=ChartType.SCATTER, points=points, extra=extra)


def _correlation_bubble(
    rows: pd.DataFrame,
    col_x: str,
    col_y: str,
    col_size: str,
    col_color: str | None,
    r: float,
    warnings: list[str],
) -> ChartPayload:
    """Chart 2 - Bubble: up to 4 dimensions - X, Y, size (3rd col), color (4th col)."""
    bubble_cols = [col_x, col_y, col_size] + ([col_color] if col_color else [])
    sample = rows.sample(MAX_BUBBLES, random_state=42) if len(rows) > MAX_BUBBLES else rows
    if len(rows) > MAX_BUBBLES:
        warnings.append(f"Bubble chart sampled down to {MAX_BUBBLES} bubbles for readability.")

    points = []
    for _, row in sample.iterrows():
        point = {
            "x": _round(float(row[col_x])),
            "y": _round(float(row[col_y])),
            "size": _round(float(row[col_size])),
        }
        if col_color:
            point["color"] = _round(float(row[col_color]))
        points.append(point)

    # Raw size/color values are returned with their min/max so the frontend can scale
    # symbol sizes and map color onto a continuous palette (e.g. viridis).
    extra: dict[str, Any] = {
        "title": "Bubble Chart",
        **_xy_stats(r, col_x, col_y),
        "size_column": col_size,
        "size_range": _value_range(sample[col_size]),
        "pairs": _correlation_pairs(rows.corr(method="pearson").round(4), bubble_cols),
    }
    if col_color:
        extra["color_column"] = col_color
        extra["color_range"] = _value_range(sample[col_color])
    return ChartPayload(chart_type=ChartType.BUBBLE, points=points, extra=extra)


def _correlation_heatmap(corr: pd.DataFrame, cols: list[str]) -> ChartPayload:
    """Chart 3 - Heat Map: pairwise correlation matrix on a fixed -1..1 diverging scale."""
    matrix = [[_round(corr.iloc[i, j]) for j in range(len(cols))] for i in range(len(cols))]
    # One cell per (row, column), so the frontend can render it directly as a heat map.
    points = [
        {"x": cols[j], "y": cols[i], "value": matrix[i][j]}
        for i in range(len(cols))
        for j in range(len(cols))
    ]
    extra = {
        "title": "Heat Map",
        "columns": cols,
        "matrix": matrix,
        "scale": {"min": -1, "max": 1, "center": 0},
    }
    return ChartPayload(chart_type=ChartType.HEATMAP, labels=cols, points=points, extra=extra)


def _compute_correlation(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    cols = list(dict.fromkeys(req.columns or []))  # drop duplicates, keep order

    # The resolved chart_type always falls back to SCATTER, so read the raw request to
    # tell an explicit choice from "not selected".
    if req.chart_type is not None:
        chart_type = req.chart_type
    elif len(cols) <= 2:
        chart_type = ChartType.SCATTER
    elif len(cols) <= 4:
        chart_type = ChartType.BUBBLE
    else:
        chart_type = ChartType.HEATMAP

    lo, hi = _CORRELATION_COLUMN_LIMITS[chart_type]
    if not lo <= len(cols) <= hi:
        expected = f"exactly {lo}" if lo == hi else f"{lo} to {hi}"
        raise error_response(
            status_code=400,
            detail=f"{chart_type.value.replace('_', ' ').title()} correlation requires {expected} "
            f"distinct numeric columns; {len(cols)} selected.",
        )

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise error_response(status_code=400, detail=f"Column(s) not found: {missing}")

    non_numeric = [c for c in cols if not _is_numeric_column(df, c)]
    if non_numeric:
        raise error_response(status_code=400, detail=f"Column(s) must be numeric: {non_numeric}")

    # Coerce once: df columns are always loaded as object/string dtype (see
    # load_dataset_dataframe), so .corr() needs real numeric dtypes here, not raw strings.
    numeric_df = pd.DataFrame({col: _numeric_series(df, col) for col in cols})
    # A source value like "inf"/"Infinity" or an overflowing literal (e.g. "1e400") coerces
    # to +/-inf rather than NaN, and dropna() does not remove inf. Treat +/-inf as missing,
    # same as any other unusable value.
    numeric_df = numeric_df.replace([np.inf, -np.inf], np.nan)

    warnings: list[str] = []
    constant = [c for c in cols if numeric_df[c].nunique(dropna=True) < 2]
    if constant:
        warnings.append(f"Correlation is undefined for constant column(s): {constant}")

    summary: dict[str, Any] = {"method": "pearson", "columns": cols}

    if chart_type == ChartType.HEATMAP:
        # Pairwise-complete Pearson matrix: each cell uses every row where both columns have values.
        corr = numeric_df.corr(method="pearson").round(4)
        pairs = _correlation_pairs(corr, cols)
        chart = _correlation_heatmap(corr, cols)
        summary.update({
            "mode": "matrix",
            "strongest_pair": pairs[0] if pairs else None,
            "pairs": pairs,
        })
        return chart, summary, warnings

    col_x, col_y = cols[0], cols[1]
    rows = numeric_df.dropna()
    if len(rows) < 3:
        raise error_response(
            status_code=400,
            detail=f"Need at least 3 rows with values in all selected columns: {cols}",
        )
    r = float(rows[col_x].corr(rows[col_y], method="pearson"))

    if chart_type == ChartType.SCATTER:
        chart = _correlation_scatter(rows, col_x, col_y, r, warnings)
        summary["mode"] = "pairwise"
    else:
        chart = _correlation_bubble(
            rows, col_x, col_y, cols[2], cols[3] if len(cols) == 4 else None, r, warnings
        )
        pairs = chart.extra["pairs"]
        summary.update({"mode": "bubble", "strongest_pair": pairs[0] if pairs else None})

    summary.update({**_xy_stats(r, col_x, col_y), "n": int(len(rows))})
    return chart, summary, warnings


# ---------------------------------------------------------------------------
# 8. Multi Axis Analysis
# ---------------------------------------------------------------------------
# X = shared dimension: categorical (Product Line, Region) or date/time (Months, Years).
# Primary Y (left axis)    = numeric, higher volume -> Columns (bar).
# Secondary Y (right axis) = numeric, different unit/scale (rate, ratio, average) -> Line.
# Backend: categorical X -> df.groupby(X)[Y].agg(); date X -> df.resample(granularity)[Y].agg()
# Chart: mixed Bar + Line on dual Y axes.

_MONTH_ORDER: dict[str, int] = {
    name: i
    for i, names in enumerate(
        [("jan", "january"), ("feb", "february"), ("mar", "march"), ("apr", "april"),
         ("may",), ("jun", "june"), ("jul", "july"), ("aug", "august"),
         ("sep", "sept", "september"), ("oct", "october"), ("nov", "november"), ("dec", "december")]
    )
    for name in names
}

_WEEKDAY_ORDER: dict[str, int] = {
    name: i
    for i, names in enumerate(
        [("mon", "monday"), ("tue", "tues", "tuesday"), ("wed", "wednesday"),
         ("thu", "thur", "thurs", "thursday"), ("fri", "friday"), ("sat", "saturday"),
         ("sun", "sunday")]
    )
    for name in names
}


def _calendar_name_order(df: pd.DataFrame, column: str) -> dict[str, int] | None:
    """Month-name or weekday-name ordering when every value in the column is one, else None."""
    values = df[column].dropna().astype(str).str.strip().str.lower()
    values = values[values != ""]
    if values.empty:
        return None
    for order in (_MONTH_ORDER, _WEEKDAY_ORDER):
        if values.isin(order.keys()).all():
            return order
    return None


def _is_month_name_column(df: pd.DataFrame, column: str) -> bool:
    return _calendar_name_order(df, column) is _MONTH_ORDER


def _looks_like_dates(df: pd.DataFrame, column: str) -> bool:
    non_blank = df[column].dropna()
    non_blank = non_blank[non_blank.astype(str).str.strip() != ""]
    if non_blank.empty:
        return False
    # Bare month names ("Jan", "February") would parse to year 0001 — keep them categorical.
    if _is_month_name_column(df, column):
        return False
    parsed = pd.to_datetime(non_blank, errors="coerce")
    return parsed.notna().mean() >= 0.8


def _resample_aggregate(resampled: Any, column: str, agg: AggregationType) -> pd.Series:
    if agg == AggregationType.PERCENTAGE:
        sums = resampled[column].sum()
        total = float(sums.sum())
        return (sums / total * 100) if total != 0 else sums
    if agg == AggregationType.COUNT:
        return resampled[column].count()
    return resampled[column].agg(_PANDAS_AGG[agg])


def _compute_multi_axis(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")

    # Primary (left axis, columns): y_columns when given, else the single y_column.
    requested_primary = [c.strip() for c in (req.y_columns or []) if c and c.strip()]
    if not requested_primary and req.y_column:
        requested_primary = [req.y_column]
    if not requested_primary:
        raise error_response(
            status_code=400,
            detail="At least one primary Y column ('y_columns') is required for Multi Axis analysis.",
        )
    primary_cols = list(dict.fromkeys(requested_primary))  # drop duplicates, keep order
    for col in primary_cols:
        _require_column(df, col, "y_columns")
    secondary_col = _require_column(df, req.secondary_y_column, "secondary_y")

    if secondary_col in primary_cols:
        raise error_response(
            status_code=400,
            detail="Primary and secondary Y columns must be different for Multi Axis analysis.",
        )
    if x_col in primary_cols or x_col == secondary_col:
        raise error_response(
            status_code=400,
            detail="X column cannot also be used as a Y column for Multi Axis analysis.",
        )
    for col in (*primary_cols, secondary_col):
        if not _is_numeric_column(df, col):
            raise error_response(status_code=400, detail=f"'{col}' must be numeric for Multi Axis analysis.")

    primary_agg = req.aggregation or AggregationType.SUM
    secondary_agg = req.secondary_aggregation or AggregationType.AVERAGE

    working = pd.DataFrame({x_col: df[x_col]})
    for col in (*primary_cols, secondary_col):
        working[col] = _numeric_series(df, col)

    # One result column per series: primary columns first (in request order), then secondary.
    series_specs = [(col, primary_agg) for col in primary_cols] + [(secondary_col, secondary_agg)]
    first_primary_key = 0

    warnings: list[str] = []
    used_granularity: TimeGranularity | None = None

    # Numeric X (e.g. Year = 2021, 2022) is treated as ordered categories rather than
    # parsed as dates, so years don't get resampled into mostly-empty monthly buckets.
    x_is_numeric = _is_numeric_column(df, x_col)
    if not x_is_numeric and _looks_like_dates(df, x_col):
        x_axis_type = "date"
        working[x_col] = _datetime_series(working, x_col)
        working = working.dropna(subset=[x_col])
        if working.empty:
            raise error_response(status_code=400, detail=f"No valid dates found in '{x_col}'.")

        if req.granularity == TimeGranularity.AUTO:
            freq, used_granularity = _auto_granularity_freq(working[x_col])
        else:
            freq = _PANDAS_FREQ.get(req.granularity)
            if not freq:
                raise error_response(status_code=400, detail=f"Unknown granularity: {req.granularity}")
            used_granularity = req.granularity

        resampled = working.set_index(x_col).resample(freq)
        combined = pd.DataFrame({
            i: _resample_aggregate(resampled, col, agg) for i, (col, agg) in enumerate(series_specs)
        }).dropna(how="all")
        labels = [ts.strftime("%Y-%m-%d") for ts in combined.index]
    else:
        x_axis_type = "category"
        if x_is_numeric:
            working[x_col] = _numeric_series(working, x_col)
        combined = pd.DataFrame({
            i: _apply_groupby_aggregation(working, x_col, col, agg) for i, (col, agg) in enumerate(series_specs)
        }).dropna(how="all")

        if len(combined) > MAX_GROUPS:
            warnings.append(f"Result truncated to top {MAX_GROUPS} groups by the first primary value.")
            combined = combined.sort_values(first_primary_key, ascending=False).iloc[:MAX_GROUPS]

        # Keep a natural order for the shared axis: ascending for numeric X (years),
        # calendar order for month / weekday names, otherwise largest first-primary value first.
        name_keys = [str(k).strip().lower() for k in combined.index]
        name_order = _calendar_name_order(df, x_col)
        if x_is_numeric:
            combined = combined.sort_index()
        elif name_order is not None and all(k in name_order for k in name_keys):
            combined = combined.iloc[sorted(range(len(combined)), key=lambda i: name_order[name_keys[i]])]
        else:
            combined = combined.sort_values(first_primary_key, ascending=False)
        labels = [_clean_label(k) for k in combined.index.tolist()]

    if combined.empty:
        raise error_response(status_code=400, detail="No data available to plot for the selected columns.")

    series: list[dict[str, Any]] = []
    for i, (col, agg) in enumerate(series_specs):
        is_primary = i < len(primary_cols)
        series.append({
            "name": f"{agg.value}({col})",
            "column": col,
            "aggregation": agg.value,
            "type": "bar" if is_primary else "line",
            "y_axis": "primary" if is_primary else "secondary",
            "axis_position": "left" if is_primary else "right",
            "y_axis_index": 0 if is_primary else 1,
            "data": [_round(v) for v in combined[i].tolist()],
        })

    primary_names = [s["name"] for s in series if s["y_axis"] == "primary"]
    secondary_name = series[-1]["name"]

    chart = ChartPayload(
        chart_type=ChartType.MIXED_BAR_LINE,
        labels=labels,
        series=series,
        extra={
            "x_column": x_col,
            "x_axis_type": x_axis_type,
            "y_axes": {
                "primary": {
                    "columns": primary_cols,
                    "label": ", ".join(primary_names),
                    "position": "left",
                    "chart": "bar",
                },
                "secondary": {
                    "column": secondary_col,
                    "label": secondary_name,
                    "position": "right",
                    "chart": "line",
                },
            },
        },
    )
    summary: dict[str, Any] = {
        "x_column": x_col,
        "x_axis_type": x_axis_type,
        "primary_y_columns": primary_cols,
        "primary_aggregation": primary_agg.value,
        "secondary_y_column": secondary_col,
        "secondary_aggregation": secondary_agg.value,
        "data_points": len(labels),
    }
    if used_granularity is not None:
        summary["granularity"] = used_granularity.value
    return chart, summary, warnings


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_ANALYSIS_HANDLERS: dict[
    AnalysisType,
    Callable[[pd.DataFrame, BasicAnalysisRequest, ChartType], tuple[ChartPayload, dict[str, Any], list[str]]],
] = {
    AnalysisType.DESCRIPTIVE: _compute_descriptive,
    AnalysisType.SIMPLE_DISTRIBUTION: _compute_simple_distribution,
    AnalysisType.TOP_N: _compute_top_n,
    AnalysisType.BOTTOM_N: _compute_bottom_n,
    AnalysisType.TIME_SERIES: _compute_time_series,
    AnalysisType.ADVANCED_DISTRIBUTION: _compute_advanced_distribution,
    AnalysisType.CORRELATION: _compute_correlation,
    AnalysisType.MULTI_AXIS: _compute_multi_axis,
    AnalysisType.GEOSPATIAL: _compute_geospatial,
}


def list_analysis_type_metadata() -> list[dict[str, Any]]:
    metadata = []
    for analysis_type in AnalysisType:
        config = ANALYSIS_TYPE_CONFIGS[analysis_type]
        metadata.append(
            {
                "analysis_type": config.analysis_type,
                "label": config.label,
                "tagline": config.tagline,
                "default_chart_type": config.default_chart_type,
                "supported_chart_types": list(config.supported_chart_types),
                "supported_aggregations": list(config.supported_aggregations),
                "column_requirements": [
                    {
                        "role": r.role,
                        "required": r.required,
                        "data_type": r.data_type,
                        "label": r.label,
                        "example": r.example,
                    }
                    for r in config.column_requirements
                ],
            }
        )
    return metadata


def run_basic_analysis(db: Session, *, current_user: User, request: BasicAnalysisRequest) -> dict[str, Any]:
    chart_type = resolve_chart_type(request.analysis_type, request.chart_type)

    source = _resolve_analysis_source(
        db,
        current_user,
        dataset_type=request.dataset_type,
        dataset_id=request.dataset_id,
        is_clean=request.is_clean,
    )
    df = load_dataset_dataframe(source)
    if df.empty:
        raise error_response(status_code=400, detail="Dataset has no rows to analyze.")

    handler = _ANALYSIS_HANDLERS[request.analysis_type]
    chart, summary, warnings = handler(df, request, chart_type)

    return {
        "analysis_type": request.analysis_type,
        "chart_type": chart.chart_type,
        "dataset_id": source.dataset_id,
        "dataset_type": source.dataset_type,
        "dataset_name": source.dataset_name,
        "file_name": source.file_name,
        "row_count_used": int(len(df)),
        "chart": chart,
        "summary": summary,
        "warnings": warnings,
    }
