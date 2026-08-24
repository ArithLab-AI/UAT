"""Computation engine for the basic analysis types defined in the
Data Analysis Workflow Specification.

Analyses:
  1. Descriptive              - auto stats table across all numeric columns
  2. Simple Distribution      - group by X (categorical), aggregate
  3. Top N                    - rank descending, N max 10
  4. Bottom N                 - rank ascending, N max 10
  5. Time Series              - resample X (date) by granularity, aggregate Y
  6. Advanced Distribution    - group by X, aggregate Y (Y mandatory)
  7. Correlation              - Pearson only; 2 cols -> scatter; 3+ -> heatmap/pair plot
  8. Predictive Regression    - train a regression model, report fit metrics + feature
                                 importance (see predictive_regression_service.py)
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
from app.services.predictive_regression_service import _compute_predictive_regression
from app.utils.responses import error_response


MAX_POINTS = 2000       # cap for scatter plots
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
# now live in basic_analysis_helpers.py (imported above) so predictive_regression_service
# and geospatial_analysis_service can reuse them without importing this module.


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
# Charts: Bar, Line, Pie, Doughnut, Line Area.

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
# 6. Advanced Distribution / Group By
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
#   Exactly 2 cols -> Scatter Plot, Scatter + Trend Line.
#   3 or more cols -> Correlation Heatmap, Pair Plot.

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
    # load_dataset_dataframe), so np.polyfit/.corr() need real numeric dtypes here, not raw
    # strings — operating on df directly raised an unhandled TypeError from np.polyfit.
    numeric_df = pd.DataFrame({col: _numeric_series(df, col) for col in cols})
    # A source value like "inf"/"Infinity" or an overflowing literal (e.g. "1e400") coerces
    # to +/-inf rather than NaN, and dropna() does not remove inf. np.polyfit then fails the
    # SVD it relies on internally (numpy.linalg.LinAlgError) — an unhandled exception that
    # surfaced as a raw 500. Treat +/-inf as missing, same as any other unusable value.
    numeric_df = numeric_df.replace([np.inf, -np.inf], np.nan)

    warnings: list[str] = []
    n_cols = len(cols)

    # Case A: Exactly 2 columns -> scatter / scatter + trend line
    if n_cols == 2:
        # If caller passed a 3+ col chart type (heatmap/pair_plot) with only 2 cols,
        # snap to scatter+trend which is the appropriate default.
        if chart_type not in (ChartType.SCATTER, ChartType.SCATTER_TREND_LINE):
            chart_type = ChartType.SCATTER_TREND_LINE

        col_x, col_y = cols[0], cols[1]
        pair = numeric_df[[col_x, col_y]].dropna()
        if len(pair) < 3:
            raise error_response(
                status_code=400,
                detail="Need at least 3 rows with values in both columns.",
            )

        r = float(pair[col_x].corr(pair[col_y], method="pearson"))
        r_squared = round(r * r, 4)

        slope, intercept = np.polyfit(pair[col_x].values, pair[col_y].values, 1)
        trend_line = {
            "slope": _round(float(slope), 6),
            "intercept": _round(float(intercept), 6),
            "start": {
                "x": _round(float(pair[col_x].min())),
                "y": _round(float(intercept + slope * pair[col_x].min())),
            },
            "end": {
                "x": _round(float(pair[col_x].max())),
                "y": _round(float(intercept + slope * pair[col_x].max())),
            },
        }

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
        if chart_type == ChartType.SCATTER_TREND_LINE:
            extra["trend_line"] = trend_line

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

    # Case B: 3+ columns -> heatmap / pair plot
    # If caller passed a 2-col chart type (scatter/scatter_trend_line) as default
    # because 3+ cols were provided, snap to correlation_heatmap.
    if chart_type not in (ChartType.CORRELATION_HEATMAP, ChartType.PAIR_PLOT):
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

    pairs: list[dict[str, Any]] = []
    for i in range(n_cols):
        for j in range(i + 1, n_cols):
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

    extra_multi: dict[str, Any] = {
        "matrix": matrix,
        "pairs": pairs,
        "columns": cols,
        "color_scale": {"min": -1, "max": 1, "midpoint": 0},
    }

    # Pair plot: include sampled scatter points per column pair
    if chart_type == ChartType.PAIR_PLOT:
        pair_plot_data: dict[str, list[dict[str, Any]]] = {}
        for i in range(n_cols):
            for j in range(n_cols):
                if i == j:
                    continue
                cx, cy = cols[j], cols[i]
                pair_df = numeric_df[[cx, cy]].dropna()
                sample = pair_df.sample(min(len(pair_df), 500), random_state=42) if len(pair_df) > 500 else pair_df
                pair_plot_data[f"{cy}__vs__{cx}"] = [
                    {"x": _round(float(row[cx])), "y": _round(float(row[cy]))}
                    for _, row in sample.iterrows()
                ]
        extra_multi["pair_plot_data"] = pair_plot_data

    chart = ChartPayload(chart_type=chart_type, labels=cols, extra=extra_multi)
    summary = {
        "mode": "matrix",
        "method": "pearson",
        "columns_analyzed": n_cols,
        "strongest_pair": pairs[0] if pairs else None,
    }
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
    AnalysisType.PREDICTIVE_REGRESSION: _compute_predictive_regression,
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
