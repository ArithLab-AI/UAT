"""Computation engine for the basic analysis types defined in the
Data Analysis Workflow Specification.

Analyses:
  1. Descriptive              - auto stats table across all numeric columns
  2. Simple Distribution      - group by X (categorical), aggregate
  3. Top N                    - rank descending, N max 10
  4. Bottom N                 - rank ascending, N max 10
  5. Time Series              - resample X (date) by granularity, aggregate Y
  6. Advanced Distribution    - group by X, aggregate Y (Y mandatory)
  7. Correlation              - Pearson only; 2 cols -> scatter; 3 -> bubble/heatmap;
                                 4+ -> heatmap
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
MAX_HEATMAP_COLS = 20   # cap for correlation heatmap columns

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
# Spec: multi-select numeric, min 2 required. Pearson only.
# Chart type follows the column selection:
#   Exactly 2 cols -> Scatter Plot  (X = independent, Y = dependent)
#   Exactly 3 cols -> Bubble Chart  (X, Y, 3rd col = bubble size) or Heatmap
#   4 or more cols -> Correlation Heatmap

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


def _compute_correlation(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    cols = req.columns or []
    if len(cols) < 2:
        raise error_response(status_code=400, detail="Correlation requires at least 2 numeric columns.")

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
    n_cols = len(cols)

    # Case A: Exactly 2 columns -> scatter plot
    if n_cols == 2:
        chart_type = ChartType.SCATTER

        col_x, col_y = cols[0], cols[1]
        pair = numeric_df[[col_x, col_y]].dropna()
        if len(pair) < 3:
            raise error_response(
                status_code=400,
                detail="Need at least 3 rows with values in both columns.",
            )

        r = float(pair[col_x].corr(pair[col_y], method="pearson"))
        r_squared = round(r * r, 4)

        sample = pair.sample(min(len(pair), MAX_POINTS), random_state=42) if len(pair) > MAX_POINTS else pair
        if len(pair) > MAX_POINTS:
            warnings.append(f"Scatter sampled down to {MAX_POINTS} points for performance.")

        points = [
            {"x": _round(float(row[col_x])), "y": _round(float(row[col_y]))}
            for _, row in sample.iterrows()
        ]

        extra: dict[str, Any] = {
            "r": round(r, 4),
            "r_squared": r_squared,
            "strength": _correlation_strength(r),
            "x_column": col_x,
            "y_column": col_y,
        }

        chart = ChartPayload(chart_type=chart_type, points=points, extra=extra)
        summary = {
            "mode": "pairwise",
            "method": "pearson",
            "r": round(r, 4),
            "r_squared": r_squared,
            "strength": _correlation_strength(r),
            "n": int(len(pair)),
        }
        return chart, summary, warnings

    # Case B: Exactly 3 columns -> bubble chart (default) unless heatmap was chosen.
    # X = independent, Y = dependent, 3rd column drives the bubble size. The raw size
    # value is returned with its min/max so the frontend can scale symbol sizes.
    if n_cols == 3 and chart_type != ChartType.CORRELATION_HEATMAP:
        chart_type = ChartType.BUBBLE

        col_x, col_y, col_size = cols[0], cols[1], cols[2]
        triple = numeric_df[[col_x, col_y, col_size]].dropna()
        if len(triple) < 3:
            raise error_response(
                status_code=400,
                detail="Need at least 3 rows with values in all three columns.",
            )

        r = float(triple[col_x].corr(triple[col_y], method="pearson"))
        r_squared = round(r * r, 4)
        pairs = _correlation_pairs(triple.corr(method="pearson").round(4), [col_x, col_y, col_size])

        sample = triple.sample(MAX_BUBBLES, random_state=42) if len(triple) > MAX_BUBBLES else triple
        if len(triple) > MAX_BUBBLES:
            warnings.append(f"Bubble chart sampled down to {MAX_BUBBLES} bubbles for readability.")

        points = [
            {
                "x": _round(float(row[col_x])),
                "y": _round(float(row[col_y])),
                "size": _round(float(row[col_size])),
            }
            for _, row in sample.iterrows()
        ]

        extra = {
            "r": round(r, 4),
            "r_squared": r_squared,
            "strength": _correlation_strength(r),
            "x_column": col_x,
            "y_column": col_y,
            "size_column": col_size,
            "size_range": {
                "min": _round(float(sample[col_size].min())),
                "max": _round(float(sample[col_size].max())),
            },
            "pairs": pairs,
        }

        chart = ChartPayload(chart_type=chart_type, points=points, extra=extra)
        summary = {
            "mode": "bubble",
            "method": "pearson",
            "r": round(r, 4),
            "r_squared": r_squared,
            "strength": _correlation_strength(r),
            "n": int(len(triple)),
            "strongest_pair": pairs[0] if pairs else None,
        }
        return chart, summary, warnings

    # Case C: 3 columns with heatmap chosen, or 4+ columns -> correlation heatmap
    chart_type = ChartType.CORRELATION_HEATMAP

    if n_cols > MAX_HEATMAP_COLS:
        warnings.append(f"Truncated to first {MAX_HEATMAP_COLS} columns for the heatmap.")
        cols = cols[:MAX_HEATMAP_COLS]

    corr = numeric_df[cols].corr(method="pearson").round(4)

    matrix: list[list[dict[str, Any]]] = []
    for i, row_col in enumerate(cols):
        row = []
        for j, col_col in enumerate(cols):
            v = corr.iloc[i, j]
            row.append({
                "x": col_col,
                "y": row_col,
                "value": None if pd.isna(v) else float(v),
            })
        matrix.append(row)

    pairs = _correlation_pairs(corr, cols)

    # Diverging scale: -1 (strong negative) .. 0 (none) .. +1 (strong positive).
    extra_multi: dict[str, Any] = {
        "matrix": matrix,
        "pairs": pairs,
        "columns": cols,
        "color_scale": {"min": -1, "max": 1, "midpoint": 0},
    }

    chart = ChartPayload(chart_type=chart_type, labels=cols, extra=extra_multi)
    summary = {
        "mode": "matrix",
        "method": "pearson",
        "columns_analyzed": n_cols,
        "strongest_pair": pairs[0] if pairs else None,
    }
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


def _is_month_name_column(df: pd.DataFrame, column: str) -> bool:
    values = df[column].dropna().astype(str).str.strip().str.lower()
    values = values[values != ""]
    return not values.empty and values.isin(_MONTH_ORDER.keys()).all()


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
    primary_col = _require_column(df, req.y_column, "y")
    secondary_col = _require_column(df, req.secondary_y_column, "secondary_y")

    if primary_col == secondary_col:
        raise error_response(
            status_code=400,
            detail="Primary and secondary Y columns must be different for Multi Axis analysis.",
        )
    if x_col in (primary_col, secondary_col):
        raise error_response(
            status_code=400,
            detail="X column cannot also be used as a Y column for Multi Axis analysis.",
        )
    for col in (primary_col, secondary_col):
        if not _is_numeric_column(df, col):
            raise error_response(status_code=400, detail=f"'{col}' must be numeric for Multi Axis analysis.")

    primary_agg = req.aggregation or AggregationType.SUM
    secondary_agg = req.secondary_aggregation or AggregationType.AVERAGE

    working = pd.DataFrame({
        x_col: df[x_col],
        primary_col: _numeric_series(df, primary_col),
        secondary_col: _numeric_series(df, secondary_col),
    })

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
            "primary": _resample_aggregate(resampled, primary_col, primary_agg),
            "secondary": _resample_aggregate(resampled, secondary_col, secondary_agg),
        }).dropna(how="all")
        labels = [ts.strftime("%Y-%m-%d") for ts in combined.index]
    else:
        x_axis_type = "category"
        if x_is_numeric:
            working[x_col] = _numeric_series(working, x_col)
        combined = pd.DataFrame({
            "primary": _apply_groupby_aggregation(working, x_col, primary_col, primary_agg),
            "secondary": _apply_groupby_aggregation(working, x_col, secondary_col, secondary_agg),
        }).dropna(how="all")

        if len(combined) > MAX_GROUPS:
            warnings.append(f"Result truncated to top {MAX_GROUPS} groups by primary value.")
            combined = combined.sort_values("primary", ascending=False).iloc[:MAX_GROUPS]

        # Keep a natural order for the shared axis: ascending for numeric X (years),
        # calendar order for month names, otherwise largest primary value first.
        month_keys = [str(k).strip().lower() for k in combined.index]
        if x_is_numeric:
            combined = combined.sort_index()
        elif _is_month_name_column(df, x_col) and all(k in _MONTH_ORDER for k in month_keys):
            combined = combined.iloc[sorted(range(len(combined)), key=lambda i: _MONTH_ORDER[month_keys[i]])]
        else:
            combined = combined.sort_values("primary", ascending=False)
        labels = [_clean_label(k) for k in combined.index.tolist()]

    if combined.empty:
        raise error_response(status_code=400, detail="No data available to plot for the selected columns.")

    primary_values = [_round(v) for v in combined["primary"].tolist()]
    secondary_values = [_round(v) for v in combined["secondary"].tolist()]
    primary_name = f"{primary_agg.value}({primary_col})"
    secondary_name = f"{secondary_agg.value}({secondary_col})"

    chart = ChartPayload(
        chart_type=ChartType.MIXED_BAR_LINE,
        labels=labels,
        series=[
            {
                "name": primary_name,
                "column": primary_col,
                "aggregation": primary_agg.value,
                "type": "bar",
                "y_axis": "primary",
                "axis_position": "left",
                "data": primary_values,
            },
            {
                "name": secondary_name,
                "column": secondary_col,
                "aggregation": secondary_agg.value,
                "type": "line",
                "y_axis": "secondary",
                "axis_position": "right",
                "data": secondary_values,
            },
        ],
        extra={
            "x_column": x_col,
            "x_axis_type": x_axis_type,
            "y_axes": {
                "primary": {"column": primary_col, "label": primary_name, "position": "left", "chart": "bar"},
                "secondary": {"column": secondary_col, "label": secondary_name, "position": "right", "chart": "line"},
            },
        },
    )
    summary: dict[str, Any] = {
        "x_column": x_col,
        "x_axis_type": x_axis_type,
        "primary_y_column": primary_col,
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
