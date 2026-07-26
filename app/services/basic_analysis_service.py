"""Computation engine for the 6 "basic" analysis types (Descriptive, Distribution,
Top N / Bottom N, Time Series, Aggregation / Group By, Correlation).

Reuses the existing dataset-resolution/download plumbing from ``analysis_service``
and ``data_chat_query_engine`` (dataset -> pandas DataFrame) rather than duplicating
it. Every computation here is read-only: nothing is written back to those services
or their tables.
"""

from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from sqlalchemy.orm import Session

from app.enum.aggregation_type_enum import (
    AggregationType,
    CorrelationMethod,
    SortDirection,
    TimeGranularity,
)
from app.enum.analysis_chart_config import ANALYSIS_TYPE_CONFIGS, resolve_chart_type
from app.enum.analysis_type_enum import AnalysisType
from app.enum.chart_type_enum import ChartType
from app.models.auth_models import User
from app.schemas.basic_analysis_schema import BasicAnalysisRequest, ChartPayload
from app.services.analysis_service import _resolve_analysis_source
from app.services.data_chat_query_engine import load_dataset_dataframe
from app.utils.responses import error_response

MAX_POINTS = 2000
MAX_GROUPS = 100
MAX_HEATMAP_COLUMNS = 20
MAX_TOP_N = 500

_AGG_FUNCS: dict[AggregationType, str] = {
    AggregationType.SUM: "sum",
    AggregationType.AVG: "mean",
    AggregationType.MEDIAN: "median",
    AggregationType.MIN: "min",
    AggregationType.MAX: "max",
    AggregationType.STD_DEV: "std",
}

_PANDAS_PERIOD_FREQ: dict[TimeGranularity, str] = {
    TimeGranularity.HOURLY: "H",
    TimeGranularity.DAILY: "D",
    TimeGranularity.WEEKLY: "W",
    TimeGranularity.MONTHLY: "M",
    TimeGranularity.QUARTERLY: "Q",
    TimeGranularity.YEARLY: "Y",
}

_PERIODS_PER_YEAR: dict[TimeGranularity, int] = {
    TimeGranularity.HOURLY: 24 * 365,
    TimeGranularity.DAILY: 365,
    TimeGranularity.WEEKLY: 52,
    TimeGranularity.MONTHLY: 12,
    TimeGranularity.QUARTERLY: 4,
    TimeGranularity.YEARLY: 1,
}


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _round(value: Any, ndigits: int = 4) -> float | None:
    if value is None:
        return None
    try:
        as_float = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(as_float) or math.isinf(as_float):
        return None
    return round(as_float, ndigits)


def _clean_label(value: Any) -> Any:
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


def _require_column(df: pd.DataFrame, column: str | None, role_label: str) -> str:
    if not column:
        raise error_response(
            status_code=400,
            detail=f"'{role_label}' column is required for this analysis type.",
        )
    if column not in df.columns:
        raise error_response(status_code=400, detail=f"Column '{column}' was not found in the dataset.")
    return column


def _optional_column(df: pd.DataFrame, column: str | None) -> str | None:
    if not column:
        return None
    if column not in df.columns:
        raise error_response(status_code=400, detail=f"Column '{column}' was not found in the dataset.")
    return column


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce")


def _datetime_series(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(df[column], errors="coerce")


def _groupby_aggregate(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    agg: AggregationType,
    secondary_col: str | None = None,
) -> pd.Series:
    group_cols = [group_col] if not secondary_col else [group_col, secondary_col]
    if agg == AggregationType.COUNT:
        return df.groupby(group_cols).size()
    if agg == AggregationType.COUNT_DISTINCT:
        return df.groupby(group_cols)[value_col].nunique()

    working = df[group_cols].copy()
    working["__value__"] = _numeric_series(df, value_col)
    grouped = working.groupby(group_cols)["__value__"]
    func = _AGG_FUNCS.get(agg, "sum")
    return grouped.agg(func)


def _box_plot_stats(series: pd.Series, max_outliers: int = 50) -> dict[str, Any]:
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr
    outliers = series[(series < lower_fence) | (series > upper_fence)]
    non_outliers = series[(series >= lower_fence) & (series <= upper_fence)]
    return {
        "min": _round(float(series.min())),
        "q1": _round(q1),
        "median": _round(float(series.median())),
        "q3": _round(q3),
        "max": _round(float(series.max())),
        "lower_whisker": _round(float(non_outliers.min())) if not non_outliers.empty else _round(float(series.min())),
        "upper_whisker": _round(float(non_outliers.max())) if not non_outliers.empty else _round(float(series.max())),
        "outlier_count": int(len(outliers)),
        "outliers": [_round(v) for v in outliers.head(max_outliers).tolist()],
    }


def _kde_curve(series: pd.Series, num_points: int = 100) -> dict[str, list[float | None]] | None:
    values = series.to_numpy(dtype=float)
    if len(values) < 2 or np.allclose(values, values[0]):
        return None
    try:
        kde = scipy_stats.gaussian_kde(values)
    except Exception:
        return None
    xs = np.linspace(values.min(), values.max(), num_points)
    ys = kde(xs)
    return {"x": [_round(v) for v in xs.tolist()], "y": [_round(v) for v in ys.tolist()]}


def _interpret_correlation(r: float | None) -> str:
    if r is None:
        return "unknown"
    abs_r = abs(r)
    if abs_r < 0.2:
        return "No correlation"
    if r < 0:
        return "Negative"
    if abs_r >= 0.7:
        return "Strong positive"
    if abs_r >= 0.4:
        return "Moderate positive"
    return "Weak"


# ---------------------------------------------------------------------------
# 1. Descriptive
# ---------------------------------------------------------------------------


def _compute_descriptive(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    y_col = _require_column(df, req.y_column, "y")
    x_col = _optional_column(df, req.x_column)
    warnings: list[str] = []

    y = _numeric_series(df, y_col).dropna()
    if y.empty:
        raise error_response(status_code=400, detail=f"Column '{y_col}' has no numeric values to analyze.")

    stats = {
        "count": int(y.count()),
        "sum": _round(y.sum()),
        "mean": _round(y.mean()),
        "median": _round(y.median()),
        "min": _round(y.min()),
        "max": _round(y.max()),
        "std_dev": _round(y.std()) if y.count() > 1 else 0.0,
    }

    if chart_type == ChartType.STATS_TABLE or not x_col:
        table = [{"metric": key.upper(), "value": value} for key, value in stats.items()]
        chart = ChartPayload(chart_type=ChartType.STATS_TABLE, table=table)
        return chart, stats, warnings

    if chart_type == ChartType.BOX_PLOT:
        chart = ChartPayload(chart_type=chart_type, extra={"box": _box_plot_stats(y)})
        return chart, stats, warnings

    agg = req.aggregation or AggregationType.AVG
    grouped = _groupby_aggregate(df, x_col, y_col, agg).dropna().sort_values(ascending=False)
    if len(grouped) > MAX_GROUPS:
        warnings.append(f"Result truncated to top {MAX_GROUPS} groups by value.")
        grouped = grouped.iloc[:MAX_GROUPS]

    labels = [_clean_label(v) for v in grouped.index.tolist()]
    values = [_round(v) for v in grouped.tolist()]
    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({y_col})", "data": values}],
    )
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# 2. Distribution
# ---------------------------------------------------------------------------


def _compute_distribution(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    group_col = _optional_column(df, req.group_by_column)
    warnings: list[str] = []

    x = _numeric_series(df, x_col).dropna()
    if x.empty:
        raise error_response(status_code=400, detail=f"Column '{x_col}' has no numeric values to analyze.")

    n = len(x)
    bin_count = req.bin_count or max(5, min(50, int(math.ceil(math.log2(n) + 1))))

    stats: dict[str, Any] = {
        "count": int(n),
        "mean": _round(x.mean()),
        "median": _round(x.median()),
        "std_dev": _round(x.std()) if n > 1 else 0.0,
        "min": _round(x.min()),
        "max": _round(x.max()),
        "skewness": _round(float(scipy_stats.skew(x))) if n > 2 else None,
        "kurtosis": _round(float(scipy_stats.kurtosis(x))) if n > 2 else None,
        "p25": _round(float(x.quantile(0.25))),
        "p50": _round(float(x.quantile(0.50))),
        "p75": _round(float(x.quantile(0.75))),
        "p90": _round(float(x.quantile(0.90))),
        "p95": _round(float(x.quantile(0.95))),
    }

    if chart_type == ChartType.BOX_PLOT:
        if group_col:
            table = []
            for key, sub in df.groupby(group_col):
                sub_x = _numeric_series(sub, x_col).dropna()
                if sub_x.empty:
                    continue
                table.append({"group": _clean_label(key), **_box_plot_stats(sub_x)})
            chart = ChartPayload(chart_type=chart_type, table=table)
        else:
            chart = ChartPayload(chart_type=chart_type, extra={"box": _box_plot_stats(x)})
        return chart, stats, warnings

    if chart_type in (ChartType.DENSITY_KDE, ChartType.VIOLIN_PLOT, ChartType.HISTOGRAM_CURVE):
        extra: dict[str, Any] = {"density_curve": _kde_curve(x)}
        if chart_type == ChartType.VIOLIN_PLOT and group_col:
            groups = []
            for key, sub in df.groupby(group_col):
                sub_x = _numeric_series(sub, x_col).dropna()
                if sub_x.empty:
                    continue
                groups.append(
                    {"group": _clean_label(key), "box": _box_plot_stats(sub_x), "density_curve": _kde_curve(sub_x)}
                )
            extra["groups"] = groups

        if chart_type != ChartType.HISTOGRAM_CURVE:
            chart = ChartPayload(chart_type=chart_type, extra=extra)
            return chart, stats, warnings

        counts, edges = np.histogram(x, bins=bin_count)
        chart = ChartPayload(
            chart_type=chart_type,
            labels=[_round((edges[i] + edges[i + 1]) / 2) for i in range(len(counts))],
            series=[{"name": "frequency", "data": [int(c) for c in counts]}],
            extra={**extra, "bin_edges": [_round(e) for e in edges.tolist()], "bin_count": bin_count},
        )
        return chart, stats, warnings

    if chart_type == ChartType.CDF:
        sorted_vals = np.sort(x.to_numpy())
        cum_pct = (np.arange(1, len(sorted_vals) + 1) / len(sorted_vals)) * 100
        step = max(1, len(sorted_vals) // MAX_POINTS)
        chart = ChartPayload(
            chart_type=chart_type,
            labels=[_round(v) for v in sorted_vals[::step].tolist()],
            series=[{"name": "cumulative_percent", "data": [_round(v) for v in cum_pct[::step].tolist()]}],
        )
        return chart, stats, warnings

    # default: HISTOGRAM
    counts, edges = np.histogram(x, bins=bin_count)
    chart = ChartPayload(
        chart_type=ChartType.HISTOGRAM,
        labels=[_round((edges[i] + edges[i + 1]) / 2) for i in range(len(counts))],
        series=[{"name": "frequency", "data": [int(c) for c in counts]}],
        extra={"bin_edges": [_round(e) for e in edges.tolist()], "bin_count": bin_count},
    )
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# 3. Top N / Bottom N
# ---------------------------------------------------------------------------


def _compute_top_n(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    y_col = _require_column(df, req.y_column, "y")
    warnings: list[str] = []

    agg = req.aggregation or AggregationType.SUM
    n = min(req.n, MAX_TOP_N)
    ascending = req.direction == SortDirection.BOTTOM

    grouped = _groupby_aggregate(df, x_col, y_col, agg).dropna()
    total = float(grouped.sum())
    sorted_series = grouped.sort_values(ascending=ascending)
    top = sorted_series.iloc[:n]

    labels = [_clean_label(v) for v in top.index.tolist()]
    values = [_round(v) for v in top.tolist()]

    stats = {
        "total_groups": int(len(grouped)),
        "n_requested": req.n,
        "n_returned": int(len(top)),
        "direction": req.direction.value,
        "aggregation": agg.value,
    }

    if chart_type == ChartType.RANKED_TABLE:
        table = [
            {"rank": idx + 1, "label": label, "value": value}
            for idx, (label, value) in enumerate(zip(labels, values))
        ]
        chart = ChartPayload(chart_type=chart_type, table=table, labels=labels)
        return chart, stats, warnings

    if chart_type == ChartType.DONUT_TOP_N_SHARE:
        donut_labels = list(labels)
        donut_values = list(values)
        others_value = _round(total - sum(v for v in values if v is not None))
        if others_value is not None and others_value > 0:
            donut_labels.append("Others")
            donut_values.append(others_value)
        chart = ChartPayload(
            chart_type=chart_type,
            labels=donut_labels,
            series=[{"name": agg.value, "data": donut_values}],
        )
        return chart, stats, warnings

    if chart_type in (ChartType.TREEMAP, ChartType.PODIUM):
        table = [
            {"rank": idx + 1, "label": label, "value": value}
            for idx, (label, value) in enumerate(zip(labels, values))
        ]
        chart = ChartPayload(
            chart_type=chart_type,
            labels=labels,
            series=[{"name": agg.value, "data": values}],
            table=table,
        )
        return chart, stats, warnings

    # default: HORIZONTAL_BAR / VERTICAL_BAR
    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({y_col})", "data": values}],
    )
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# 4. Time Series
# ---------------------------------------------------------------------------


def _apply_time_series_post_agg(
    values: pd.Series,
    agg: AggregationType,
    rolling_window: int,
    granularity: TimeGranularity,
) -> pd.Series:
    if agg == AggregationType.CUMSUM:
        return values.cumsum()
    if agg == AggregationType.MOM_PERCENT:
        return values.pct_change().mul(100)
    if agg == AggregationType.YOY_PERCENT:
        return values.pct_change(periods=_PERIODS_PER_YEAR.get(granularity, 12)).mul(100)
    if agg == AggregationType.ROLLING_AVG:
        return values.rolling(window=rolling_window, min_periods=1).mean()
    return values


def _compute_time_series(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    y_col = _require_column(df, req.y_column, "y")
    series_col = _optional_column(df, req.series_column)
    warnings: list[str] = []

    x = _datetime_series(df, x_col)
    valid_mask = x.notna()
    if not valid_mask.any():
        raise error_response(status_code=400, detail=f"Column '{x_col}' has no parseable dates.")
    if int((~valid_mask).sum()) > 0:
        warnings.append(f"{int((~valid_mask).sum())} row(s) with unparseable dates were excluded.")

    working = pd.DataFrame(
        {"__period__": x[valid_mask].dt.to_period(_PANDAS_PERIOD_FREQ[req.granularity]), "__value__": _numeric_series(df, y_col)[valid_mask]}
    )
    if series_col:
        working["__series__"] = df.loc[valid_mask, series_col].to_numpy()

    agg = req.aggregation or AggregationType.SUM
    group_cols = ["__period__"] if not series_col else ["__period__", "__series__"]
    if agg == AggregationType.COUNT:
        grouped = working.groupby(group_cols).size().rename("__value__").reset_index()
    elif agg == AggregationType.AVG:
        grouped = working.groupby(group_cols)["__value__"].mean().reset_index()
    else:
        grouped = working.groupby(group_cols)["__value__"].sum().reset_index()

    stats = {
        "granularity": req.granularity.value,
        "aggregation": agg.value,
        "periods": int(grouped["__period__"].nunique()),
    }

    if series_col:
        pivot = grouped.pivot(index="__period__", columns="__series__", values="__value__").sort_index()
        if pivot.shape[1] > MAX_GROUPS:
            top_cols = pivot.sum(axis=0, skipna=True).sort_values(ascending=False).index[:MAX_GROUPS]
            pivot = pivot[top_cols]
            warnings.append(f"Series limited to top {MAX_GROUPS} by total value.")
        if len(pivot) > MAX_POINTS:
            pivot = pivot.iloc[-MAX_POINTS:]
            warnings.append(f"Chart truncated to the most recent {MAX_POINTS} periods.")

        labels = [str(p) for p in pivot.index.tolist()]
        series = []
        for col in pivot.columns:
            col_values = _apply_time_series_post_agg(pivot[col], agg, req.rolling_window, req.granularity)
            series.append({"name": _clean_label(col), "data": [_round(v) for v in col_values.tolist()]})
        chart = ChartPayload(chart_type=chart_type, labels=labels, series=series)
        return chart, stats, warnings

    single = grouped.set_index("__period__")["__value__"].sort_index()
    if len(single) > MAX_POINTS:
        single = single.iloc[-MAX_POINTS:]
        warnings.append(f"Chart truncated to the most recent {MAX_POINTS} periods.")
    single = _apply_time_series_post_agg(single, agg, req.rolling_window, req.granularity)

    labels = [str(p) for p in single.index.tolist()]
    values = [_round(v) for v in single.tolist()]

    if chart_type == ChartType.CALENDAR_HEATMAP:
        table = [{"period": label, "value": value} for label, value in zip(labels, values)]
        chart = ChartPayload(chart_type=chart_type, labels=labels, series=[{"name": agg.value, "data": values}], table=table)
        return chart, stats, warnings

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({y_col})", "data": values}],
    )
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# 5. Aggregation / Group By
# ---------------------------------------------------------------------------


def _compute_aggregation(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    x_col = _require_column(df, req.x_column, "x")
    y_col = _require_column(df, req.y_column, "y")
    secondary_col = _optional_column(df, req.secondary_group_column)
    warnings: list[str] = []

    agg = req.aggregation or AggregationType.SUM

    if secondary_col and chart_type in (ChartType.GROUPED_BAR, ChartType.STACKED_BAR):
        grouped = _groupby_aggregate(df, x_col, y_col, agg, secondary_col=secondary_col)
        pivot = grouped.unstack(secondary_col).fillna(0)
        if pivot.shape[0] > MAX_GROUPS:
            pivot = pivot.iloc[:MAX_GROUPS]
            warnings.append(f"Result truncated to first {MAX_GROUPS} groups.")
        if pivot.shape[1] > MAX_GROUPS:
            top_cols = pivot.sum(axis=0).sort_values(ascending=False).index[:MAX_GROUPS]
            pivot = pivot[top_cols]
            warnings.append(f"Secondary groups limited to top {MAX_GROUPS} by total value.")

        labels = [_clean_label(v) for v in pivot.index.tolist()]
        series = [
            {"name": _clean_label(col), "data": [_round(v) for v in pivot[col].tolist()]} for col in pivot.columns
        ]
        chart = ChartPayload(chart_type=chart_type, labels=labels, series=series)
        stats = {"groups": len(labels), "secondary_groups": len(pivot.columns), "aggregation": agg.value}
        return chart, stats, warnings

    grouped = _groupby_aggregate(df, x_col, y_col, agg).dropna().sort_values(ascending=False)
    total = float(grouped.sum())

    if agg == AggregationType.PERCENT_OF_TOTAL and total:
        grouped = (grouped / total) * 100

    if len(grouped) > MAX_GROUPS:
        warnings.append(f"Result truncated to top {MAX_GROUPS} groups by value.")
        grouped = grouped.iloc[:MAX_GROUPS]

    if chart_type in (ChartType.PIE, ChartType.DOUGHNUT) and len(grouped) > 8:
        warnings.append("More than 8 categories selected — consider Bar/Treemap instead of Pie/Doughnut.")

    labels = [_clean_label(v) for v in grouped.index.tolist()]
    values = [_round(v) for v in grouped.tolist()]
    stats = {"groups": len(labels), "aggregation": agg.value, "grand_total": _round(total)}

    chart = ChartPayload(
        chart_type=chart_type,
        labels=labels,
        series=[{"name": f"{agg.value}({y_col})", "data": values}],
    )
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# 6. Correlation
# ---------------------------------------------------------------------------


def _compute_correlation(
    df: pd.DataFrame, req: BasicAnalysisRequest, chart_type: ChartType
) -> tuple[ChartPayload, dict[str, Any], list[str]]:
    warnings: list[str] = []

    if chart_type == ChartType.CORRELATION_HEATMAP:
        candidate_cols = [c for c in (req.heatmap_columns or df.columns.tolist()) if c in df.columns]
        if not req.heatmap_columns:
            candidate_cols = [
                col for col in candidate_cols if pd.to_numeric(df[col], errors="coerce").notna().mean() >= 0.5
            ]
        candidate_cols = candidate_cols[:MAX_HEATMAP_COLUMNS]
        if len(candidate_cols) < 2:
            raise error_response(
                status_code=400, detail="At least 2 numeric columns are required for a correlation heatmap."
            )

        numeric_df = df[candidate_cols].apply(lambda col: pd.to_numeric(col, errors="coerce"))
        corr_matrix = numeric_df.corr(method=req.correlation_method.value)
        table = [
            {"row": row, **{col: _round(corr_matrix.loc[row, col]) for col in candidate_cols}}
            for row in candidate_cols
        ]
        chart = ChartPayload(chart_type=chart_type, labels=candidate_cols, table=table)
        stats = {"columns": candidate_cols, "method": req.correlation_method.value}
        return chart, stats, warnings

    x_col = _require_column(df, req.x_column, "x")
    y_col = _require_column(df, req.y_column, "y")
    group_col = _optional_column(df, req.group_by_column)
    size_col = _optional_column(df, req.size_column)

    x = _numeric_series(df, x_col)
    y = _numeric_series(df, y_col)
    mask = x.notna() & y.notna()
    size_series = _numeric_series(df, size_col) if size_col else None
    if size_series is not None:
        mask &= size_series.notna()

    if int(mask.sum()) < 2:
        raise error_response(status_code=400, detail="Not enough numeric (x, y) pairs to compute a correlation.")

    x_valid = x[mask]
    y_valid = y[mask]
    method = "spearman" if req.correlation_method == CorrelationMethod.SPEARMAN else "pearson"
    r = _round(float(x_valid.corr(y_valid, method=method)))
    r_squared = _round(r**2) if r is not None else None
    interpretation = _interpret_correlation(r)

    slope = intercept = None
    if len(x_valid) >= 2 and float(x_valid.std()) > 0:
        fit_slope, fit_intercept = np.polyfit(x_valid.to_numpy(), y_valid.to_numpy(), 1)
        slope, intercept = _round(float(fit_slope)), _round(float(fit_intercept))

    stats: dict[str, Any] = {
        "r": r,
        "r_squared": r_squared,
        "method": req.correlation_method.value,
        "interpretation": interpretation,
        "n": int(mask.sum()),
        "trend_line": {"slope": slope, "intercept": intercept} if slope is not None else None,
    }

    indices = np.where(mask.to_numpy())[0]
    if len(indices) > MAX_POINTS:
        rng = np.random.default_rng(42)
        indices = np.sort(rng.choice(indices, size=MAX_POINTS, replace=False))
        warnings.append(f"Scatter sampled down to {MAX_POINTS} points for performance.")

    points = []
    for idx in indices:
        point: dict[str, Any] = {"x": _round(float(x.iat[idx])), "y": _round(float(y.iat[idx]))}
        if group_col:
            point["group"] = _clean_label(df[group_col].iat[idx])
        if size_series is not None:
            point["size"] = _round(float(size_series.iat[idx]))
        points.append(point)

    extra: dict[str, Any] = {}
    if chart_type == ChartType.SCATTER_TREND_LINE and slope is not None:
        extra["trend_line"] = {"slope": slope, "intercept": intercept}
    if chart_type == ChartType.NO_CORRELATION_VIEW and abs(r or 0) >= 0.2:
        warnings.append("Correlation is not negligible (|r| >= 0.2) — consider Scatter or Scatter + Trend Line.")

    chart = ChartPayload(chart_type=chart_type, points=points, extra=extra)
    return chart, stats, warnings


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_ANALYSIS_HANDLERS: dict[
    AnalysisType, Callable[[pd.DataFrame, BasicAnalysisRequest, ChartType], tuple[ChartPayload, dict[str, Any], list[str]]]
] = {
    AnalysisType.DESCRIPTIVE: _compute_descriptive,
    AnalysisType.DISTRIBUTION: _compute_distribution,
    AnalysisType.TOP_N_BOTTOM_N: _compute_top_n,
    AnalysisType.TIME_SERIES: _compute_time_series,
    AnalysisType.AGGREGATION: _compute_aggregation,
    AnalysisType.CORRELATION: _compute_correlation,
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
                        "role": requirement.role,
                        "required": requirement.required,
                        "data_type": requirement.data_type,
                        "label": requirement.label,
                        "example": requirement.example,
                    }
                    for requirement in config.column_requirements
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
