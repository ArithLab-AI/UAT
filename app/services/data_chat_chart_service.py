"""Normalise data-chat results into stable chart payloads for the frontend."""

from __future__ import annotations

import math
import re
from typing import Any, Optional

import pandas as pd

CHART_FORMAT_VERSION = 1

CHART_SELECTION_GUIDE = """
- smooth_line: time-series trend with one or more numeric metrics; use when the x-axis is time-like.
- area: time-series where the filled area should emphasise the total scale over time.
- stacked_line: multi-series time trend with one numeric metric split by a grouping dimension.
- stacked_area: multi-series time trend where contribution to the running total matters.
- bar: categorical comparison, rankings, top-N, or category vs metric analysis.
- stacked_bar: categorical comparison split by a second grouping dimension.
- waterfall: sequential positive/negative deltas from a starting point to an ending state.
- mixed: same x-axis with exactly two numeric measures that should be shown as bar + line on dual y-axes.
- doughnut: part-to-whole with 2 to 5 slices, never more than 8 slices.
- scatter: raw x/y numeric points.
- bubble: raw x/y numeric points plus a third numeric size value.
- heatmap: two categorical axes plus one numeric intensity value.
- kpi: a single headline value, optionally with a comparison delta.
- table: fallback when the result is not chart-friendly or a chart would be misleading.
""".strip()

SUPPORTED_CHART_TYPES = {
    "table",
    "kpi",
    "bar",
    "stacked_bar",
    "smooth_line",
    "area",
    "stacked_line",
    "stacked_area",
    "waterfall",
    "mixed",
    "doughnut",
    "scatter",
    "bubble",
    "heatmap",
}

_CHART_TYPE_ALIASES = {
    "line": "smooth_line",
    "pie": "doughnut",
    "correlation": "bubble",
    "basic_bar": "bar",
    "basic_area": "area",
    "basic_scatter": "scatter",
    "smooth_line_chart": "smooth_line",
    "stacked_line_chart": "stacked_line",
    "stacked_area_chart": "stacked_area",
    "mixed_line_bar_chart": "mixed",
    "doughnut_chart": "doughnut",
    "kpi_card": "kpi",
}

_AXIS_CHART_TYPES = {
    "bar",
    "stacked_bar",
    "smooth_line",
    "area",
    "stacked_line",
    "stacked_area",
    "waterfall",
    "mixed",
}
_STACKED_CHART_TYPES = {"stacked_bar", "stacked_line", "stacked_area"}
_TIME_SERIES_CHART_TYPES = {"smooth_line", "area", "stacked_line", "stacked_area"}
# Axis charts ki tarah inko bhi ek categorical dimension chahiye, warna builder None return
# karke table par gir jata hai.
_CATEGORY_CHART_TYPES = {"doughnut", "heatmap"}
_SINGLE_VALUE_CHART_TYPES = {
    "kpi",
    "doughnut",
    "waterfall",
    "stacked_bar",
    "stacked_line",
    "stacked_area",
    "heatmap",
}


# Agar user khud bol de "show it as a pie chart", to wahi chart type dena hai — LLM ki
# apni pasand ya inference se override nahi hona chahiye. Yeh detection question text par
# deterministic hai, isliye follow-up turns me bhi reliably kaam karta hai.
_EXPLICIT_CHART_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Order matters: zyada specific phrases (stacked bar) pehle match hone chahiye. Ambiguous
    # words (bar, line, area) ke saath chart/graph/plot zaroori hai, warna "product line" jaise
    # normal sawaal galti se chart type force kar denge.
    (re.compile(r"\bstacked\s+bar\b", re.I), "stacked_bar"),
    (re.compile(r"\bstacked\s+area\b", re.I), "stacked_area"),
    (re.compile(r"\bstacked\s+line\b", re.I), "stacked_line"),
    (re.compile(r"\b(pie|donut|doughnut)\b", re.I), "doughnut"),
    (re.compile(r"\bwaterfall\b", re.I), "waterfall"),
    (re.compile(r"\bheat\s*-?\s*map\b", re.I), "heatmap"),
    (re.compile(r"\bbubble\b", re.I), "bubble"),
    (re.compile(r"\bscatter\b", re.I), "scatter"),
    (re.compile(r"\bcombo\s+(chart|graph)\b|\bbar\s*\+\s*line\b", re.I), "mixed"),
    (re.compile(r"\b(bar|column)[\s-]*(chart|graph|plot)\b", re.I), "bar"),
    (re.compile(r"\bline[\s-]*(chart|graph|plot)\b", re.I), "smooth_line"),
    (re.compile(r"\barea[\s-]*(chart|graph|plot)\b", re.I), "area"),
    (re.compile(r"\bkpi\b|\bheadline\s+number\b|\bsingle\s+number\b", re.I), "kpi"),
    (
        re.compile(r"\b(as|in|to)\s+(a\s+)?table\b|\btable\s+(view|format)\b", re.I),
        "table",
    ),
)


def detect_explicit_chart_type(question: str) -> Optional[str]:
    """Return the chart type the user asked for by name, or None if they didn't ask."""
    text = question or ""
    if not text.strip():
        return None
    for pattern, chart_type in _EXPLICIT_CHART_PATTERNS:
        if pattern.search(text):
            return chart_type
    return None


def _build_forced_doughnut_chart(
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Doughnut without the 2-8 slice cap, used only when the user names pie explicitly."""
    category_column = mapping.get("category")
    value_column = (mapping.get("value") or [None])[0]
    if not category_column or not value_column:
        return None

    bucket: dict[str, float] = {}
    for row in rows:
        numeric_value = _coerce_number(row.get(value_column))
        if numeric_value is None:
            continue
        label = _label(row.get(category_column))
        bucket[label] = bucket.get(label, 0.0) + numeric_value

    items = [{"name": label, "value": value} for label, value in bucket.items()]
    if len(items) < 2:
        return None
    return {"items": items}


def normalize_chart_spec(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    raw_chart: Optional[dict[str, Any]],
) -> dict[str, Any]:
    """Return a stable chart payload for the frontend."""
    safe_columns = [str(column) for column in (columns or [])]
    safe_rows = [dict(row) for row in (rows or [])]
    chart_request = raw_chart if isinstance(raw_chart, dict) else {}

    requested_type = _normalise_chart_type(chart_request.get("type"))
    explicit_type = detect_explicit_chart_type(question)
    inferred_type = _infer_chart_type(question, safe_columns, safe_rows, requested_type)
    candidates: list[str] = []
    for candidate in (
        explicit_type,  # user ne khud chart type maanga hai to wahi pehle try hota hai
        inferred_type if requested_type == "table" else requested_type,
        requested_type,
        inferred_type,
        "table",
    ):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    for chart_type in candidates:
        spec = _base_chart_spec(
            chart_type=chart_type,
            question=question,
            columns=safe_columns,
            rows=safe_rows,
            raw_chart=chart_request,
        )
        builder = _CHART_BUILDERS.get(chart_type)
        if builder is None:
            return spec

        build_rows = safe_rows
        category_column = spec["mapping"].get("category")
        if chart_type in _AXIS_CHART_TYPES and category_column and _is_time_like(category_column, safe_rows):
            build_rows = _sort_rows_chronologically(safe_rows, category_column)

        built = builder(safe_columns, build_rows, spec["mapping"], spec["options"])
        if built is None and chart_type == explicit_type == "doughnut":
            # Explicit pie request: slice-count cap ko relax karte hain (e.g. 12 mahine).
            built = _build_forced_doughnut_chart(build_rows, spec["mapping"])
        if built is None:
            continue

        spec["payload"] = built
        return spec

    return _base_chart_spec(
        chart_type="table",
        question=question,
        columns=safe_columns,
        rows=safe_rows,
        raw_chart=chart_request,
    )


def _base_chart_spec(
    *,
    chart_type: str,
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    raw_chart: dict[str, Any],
) -> dict[str, Any]:
    title = str(raw_chart.get("title") or question or "Chart").strip()
    mapping = _resolve_mapping(chart_type, columns, rows, raw_chart)
    options = _default_options(chart_type, rows, mapping, raw_chart)
    return {
        "format_version": CHART_FORMAT_VERSION,
        "type": chart_type,
        "title": title,
        "mapping": mapping,
        "options": options,
        "payload": {},
    }


def _normalise_chart_type(value: Any) -> str:
    chart_type = str(value or "table").strip().lower().replace(" ", "_")
    chart_type = _CHART_TYPE_ALIASES.get(chart_type, chart_type)
    if chart_type not in SUPPORTED_CHART_TYPES:
        return "table"
    return chart_type


def _infer_chart_type(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    requested_type: str,
) -> str:
    if not columns or not rows:
        return "table"

    numeric_columns = _numeric_columns(columns, rows)
    dimension_columns = [column for column in columns if column not in numeric_columns]
    time_columns = [column for column in dimension_columns if _is_time_like(column, rows)]

    if requested_type == "doughnut" and dimension_columns and len(numeric_columns) == 1:
        category_count = len(_ordered_labels(rows, dimension_columns[0]))
        if 1 < category_count <= 8:
            return "doughnut"

    question_lower = (question or "").lower()
    wants_part_to_whole = any(token in question_lower for token in ("share", "split", "breakdown", "part"))

    if len(rows) == 1 and numeric_columns:
        return "kpi"
    if len(numeric_columns) >= 3 and not dimension_columns:
        return "bubble"
    if len(numeric_columns) >= 2 and not dimension_columns:
        return "scatter"
    if len(dimension_columns) >= 2 and len(numeric_columns) == 1:
        if time_columns:
            return "stacked_line"
        if _is_low_cardinality_dimension(rows, dimension_columns[0]) and _is_low_cardinality_dimension(
            rows, dimension_columns[1]
        ):
            return "heatmap"
        return "stacked_bar"
    if time_columns and len(numeric_columns) >= 2:
        return "mixed"
    if time_columns and len(numeric_columns) == 1:
        return "smooth_line"
    if dimension_columns and len(numeric_columns) == 1:
        if wants_part_to_whole and 1 < len(_ordered_labels(rows, dimension_columns[0])) <= 8:
            return "doughnut"
        return "bar"
    if len(numeric_columns) >= 2:
        return "scatter"
    return "table"


def _resolve_mapping(
    chart_type: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    raw_chart: dict[str, Any],
) -> dict[str, Any]:
    numeric_columns = _numeric_columns(columns, rows)
    dimension_columns = [column for column in columns if column not in numeric_columns]
    time_columns = [column for column in dimension_columns if _is_time_like(column, rows)]

    raw_x = _match_column(columns, raw_chart.get("x"))
    raw_series = _match_column(columns, raw_chart.get("series"))
    raw_size = _match_column(columns, raw_chart.get("size"))
    raw_y = [_match_column(columns, value) for value in _as_list(raw_chart.get("y"))]
    raw_y = [value for value in raw_y if value and value in numeric_columns]

    category = raw_x
    if chart_type in _TIME_SERIES_CHART_TYPES and not category:
        category = time_columns[0] if time_columns else None
    if chart_type in _AXIS_CHART_TYPES and not category:
        category = (time_columns or dimension_columns or columns[:1] or [None])[0]
    # "Ise pie chart bana do" jaise follow-up par LLM aksar x=null bhejta hai. Us case me
    # category khud result se nikal lete hain, warna doughnut/heatmap build hi nahi hota aur
    # response me table chala jata hai.
    if chart_type in _CATEGORY_CHART_TYPES and not category:
        category = (dimension_columns or columns[:1] or [None])[0]

    value_columns = raw_y
    if chart_type in _SINGLE_VALUE_CHART_TYPES:
        value_columns = value_columns[:1]
    if chart_type == "mixed":
        value_columns = value_columns[:2]
    if not value_columns:
        if chart_type == "mixed":
            value_columns = numeric_columns[:2]
        elif chart_type in {"scatter", "bubble"}:
            value_columns = []
        elif chart_type == "heatmap":
            value_columns = numeric_columns[:1]
        elif chart_type == "kpi":
            value_columns = numeric_columns[:1] or columns[:1]
        else:
            value_columns = numeric_columns[:1] if chart_type in _STACKED_CHART_TYPES else numeric_columns[:2]
    if chart_type in _SINGLE_VALUE_CHART_TYPES:
        value_columns = value_columns[:1]

    series = raw_series if raw_series in dimension_columns else None
    if chart_type in _STACKED_CHART_TYPES and not series:
        series_candidates = [column for column in dimension_columns if column != category]
        series = series_candidates[0] if series_candidates else None
    if chart_type == "heatmap" and not series:
        series_candidates = [column for column in dimension_columns if column != category]
        series = series_candidates[0] if series_candidates else None

    x_value = raw_x
    y_values = value_columns
    size = raw_size if raw_size in numeric_columns else None
    if chart_type in {"scatter", "bubble"}:
        x_value = raw_x if raw_x in numeric_columns else None
        if not x_value and numeric_columns:
            x_value = numeric_columns[0]
        y_values = raw_y[:1]
        if not y_values:
            remaining = [column for column in numeric_columns if column != x_value]
            y_values = remaining[:1]
        if chart_type == "bubble" and not size:
            remaining = [column for column in numeric_columns if column not in {x_value, *(y_values or [])}]
            size = remaining[0] if remaining else None

    if chart_type in _CATEGORY_CHART_TYPES and category and not x_value:
        x_value = category
    if chart_type == "kpi" and not y_values:
        y_values = value_columns[:1]

    return {
        "category": category,
        "value": value_columns,
        "series": series,
        "x": x_value,
        "y": y_values,
        "size": size,
    }


def _default_options(
    chart_type: str,
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    raw_chart: dict[str, Any],
) -> dict[str, Any]:
    orientation = None
    if chart_type in {"bar", "stacked_bar"}:
        orientation = str(raw_chart.get("orientation") or "").strip().lower() or None
        if orientation not in {"horizontal", "vertical"}:
            orientation = _default_bar_orientation(rows, mapping.get("category"))

    options: dict[str, Any] = {
        "orientation": orientation,
        "smooth": chart_type == "smooth_line",
        "stack": "Total" if chart_type in {"stacked_bar", "stacked_line", "stacked_area", "waterfall"} else None,
        "dual_axis": chart_type == "mixed",
        "radius": ["40%", "70%"] if chart_type == "doughnut" else None,
        "item_border_radius": 10 if chart_type == "doughnut" else None,
        "helper_series_name": "baseline" if chart_type == "waterfall" else None,
        "delta_series_name": "delta" if chart_type == "waterfall" else None,
        "visual_map": None,
    }

    if chart_type == "heatmap":
        metric_column = (mapping.get("value") or [None])[0]
        values = [
            _coerce_number(row.get(metric_column))
            for row in rows
            if metric_column is not None and _coerce_number(row.get(metric_column)) is not None
        ]
        if values:
            options["visual_map"] = {
                "min": min(values),
                "max": max(values),
            }

    return options


def _default_bar_orientation(rows: list[dict[str, Any]], category_column: Optional[str]) -> str:
    if not category_column:
        return "vertical"
    labels = [_label(row.get(category_column)) for row in rows[:20]]
    longest = max((len(label) for label in labels), default=0)
    return "horizontal" if longest >= 18 else "vertical"


def _build_table_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> dict[str, Any]:
    del mapping, options
    return {"columns": columns, "rows": rows}


def _build_kpi_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns, options
    if len(rows) != 1:
        return None

    row = rows[0]
    primary_column = (mapping.get("y") or mapping.get("value") or [None])[0]
    if primary_column is None:
        return None

    secondary_delta = None
    for column in row:
        if column == primary_column:
            continue
        numeric_value = _coerce_number(row.get(column))
        if numeric_value is None:
            continue
        if any(token in column.lower() for token in ("delta", "change", "growth", "pct", "percent", "variance")):
            secondary_delta = numeric_value
            break

    return {
        "primary_value": row.get(primary_column),
        "secondary_delta": secondary_delta,
        "label": primary_column,
    }


def _build_simple_axis_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
    *,
    series_type: str,
    area_style: bool = False,
) -> Optional[dict[str, Any]]:
    del columns
    category_column = mapping.get("category")
    value_columns = mapping.get("value") or []
    if not category_column or not value_columns:
        return None

    categories = [_label(row.get(category_column)) for row in rows]
    series_entries = []
    for column in value_columns:
        values = [_coerce_number(row.get(column)) for row in rows]
        if not any(value is not None for value in values):
            continue
        series_entries.append(
            {
                "name": column,
                "type": series_type,
                "data": values,
                "stack": options.get("stack"),
                "y_axis_index": 0,
                "smooth": bool(options.get("smooth")),
                "area_style": area_style,
            }
        )

    if not series_entries:
        return None
    return {"categories": categories, "series": series_entries}


def _build_stacked_axis_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
    *,
    series_type: str,
    area_style: bool = False,
) -> Optional[dict[str, Any]]:
    del columns
    category_column = mapping.get("category")
    series_column = mapping.get("series")
    value_column = (mapping.get("value") or [None])[0]
    if not category_column or not series_column or not value_column:
        return None

    categories = _ordered_labels(rows, category_column)
    series_names = _ordered_labels(rows, series_column)
    if not categories or not series_names:
        return None

    bucket: dict[tuple[str, str], float] = {}
    for row in rows:
        category_key = _label(row.get(category_column))
        series_key = _label(row.get(series_column))
        numeric_value = _coerce_number(row.get(value_column))
        if numeric_value is None:
            continue
        bucket[(category_key, series_key)] = bucket.get((category_key, series_key), 0.0) + numeric_value

    series_entries = []
    for series_name in series_names:
        series_entries.append(
            {
                "name": series_name,
                "type": series_type,
                "data": [bucket.get((category, series_name)) for category in categories],
                "stack": options.get("stack"),
                "y_axis_index": 0,
                "smooth": bool(options.get("smooth")),
                "area_style": area_style,
            }
        )

    return {"categories": categories, "series": series_entries}


def _build_waterfall_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns
    category_column = mapping.get("category")
    value_column = (mapping.get("value") or [None])[0]
    if not category_column or not value_column:
        return None

    categories: list[str] = []
    helper: list[float] = []
    delta_values: list[float] = []
    cumulative_total = 0.0

    for row in rows:
        delta = _coerce_number(row.get(value_column))
        if delta is None:
            return None
        categories.append(_label(row.get(category_column)))
        if delta >= 0:
            helper.append(cumulative_total)
            delta_values.append(delta)
        else:
            helper.append(cumulative_total + delta)
            delta_values.append(abs(delta))
        cumulative_total += delta

    return {
        "categories": categories,
        "series": [
            {
                "name": options.get("helper_series_name") or "baseline",
                "type": "bar",
                "data": helper,
                "stack": options.get("stack"),
                "y_axis_index": 0,
                "smooth": False,
                "area_style": False,
            },
            {
                "name": options.get("delta_series_name") or value_column,
                "type": "bar",
                "data": delta_values,
                "stack": options.get("stack"),
                "y_axis_index": 0,
                "smooth": False,
                "area_style": False,
            },
        ],
    }


def _build_mixed_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns
    category_column = mapping.get("category")
    value_columns = (mapping.get("value") or [])[:2]
    if not category_column or len(value_columns) < 2:
        return None

    categories = [_label(row.get(category_column)) for row in rows]
    first_metric, second_metric = value_columns[:2]
    first_values = [_coerce_number(row.get(first_metric)) for row in rows]
    second_values = [_coerce_number(row.get(second_metric)) for row in rows]
    if not any(value is not None for value in first_values + second_values):
        return None

    return {
        "categories": categories,
        "series": [
            {
                "name": first_metric,
                "type": "bar",
                "data": first_values,
                "stack": None,
                "y_axis_index": 0,
                "smooth": False,
                "area_style": False,
            },
            {
                "name": second_metric,
                "type": "line",
                "data": second_values,
                "stack": None,
                "y_axis_index": 1 if options.get("dual_axis") else 0,
                "smooth": True,
                "area_style": False,
            },
        ],
    }


def _build_doughnut_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns, options
    category_column = mapping.get("category")
    value_column = (mapping.get("value") or [None])[0]
    if not category_column or not value_column:
        return None

    bucket: dict[str, float] = {}
    for row in rows:
        numeric_value = _coerce_number(row.get(value_column))
        if numeric_value is None:
            continue
        label = _label(row.get(category_column))
        bucket[label] = bucket.get(label, 0.0) + numeric_value

    items = [{"name": label, "value": value} for label, value in bucket.items()]

    if not (1 < len(items) <= 8):
        return None
    return {"items": items}


def _build_scatter_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns, options
    x_column = mapping.get("x")
    y_column = (mapping.get("y") or [None])[0]
    if not x_column or not y_column:
        return None

    points = []
    for row in rows:
        x_value = _coerce_number(row.get(x_column))
        y_value = _coerce_number(row.get(y_column))
        if x_value is None or y_value is None:
            continue
        points.append([x_value, y_value])

    if not points:
        return None
    return {"points": points}


def _build_bubble_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns, options
    x_column = mapping.get("x")
    y_column = (mapping.get("y") or [None])[0]
    size_column = mapping.get("size")
    if not x_column or not y_column or not size_column:
        return None

    points = []
    for row in rows:
        x_value = _coerce_number(row.get(x_column))
        y_value = _coerce_number(row.get(y_column))
        size_value = _coerce_number(row.get(size_column))
        if x_value is None or y_value is None or size_value is None:
            continue
        points.append([x_value, y_value, size_value])

    if not points:
        return None
    return {"points": points}


def _build_heatmap_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
    mapping: dict[str, Any],
    options: dict[str, Any],
) -> Optional[dict[str, Any]]:
    del columns, options
    x_column = mapping.get("category") or mapping.get("x")
    y_column = mapping.get("series")
    value_column = (mapping.get("value") or [None])[0]
    if not x_column or not y_column or not value_column:
        return None

    x_axis = _ordered_labels(rows, x_column)
    y_axis = _ordered_labels(rows, y_column)
    if not x_axis or not y_axis:
        return None

    x_lookup = {label: index for index, label in enumerate(x_axis)}
    y_lookup = {label: index for index, label in enumerate(y_axis)}
    bucket: dict[tuple[int, int], float] = {}

    for row in rows:
        value = _coerce_number(row.get(value_column))
        if value is None:
            continue
        x_label = _label(row.get(x_column))
        y_label = _label(row.get(y_column))
        key = (x_lookup[x_label], y_lookup[y_label])
        bucket[key] = bucket.get(key, 0.0) + value

    points = [[x_index, y_index, value] for (x_index, y_index), value in bucket.items()]
    if not points:
        return None

    return {
        "x_axis": x_axis,
        "y_axis": y_axis,
        "points": points,
    }


def _match_column(columns: list[str], requested: Any) -> Optional[str]:
    if requested is None:
        return None
    candidate = str(requested).strip()
    if not candidate:
        return None

    exact = next((column for column in columns if column == candidate), None)
    if exact:
        return exact

    lowered = candidate.casefold()
    return next((column for column in columns if column.casefold() == lowered), None)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _coerce_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    percent = text.endswith("%")
    cleaned = text.replace(",", "").replace("%", "")
    cleaned = cleaned.replace("$", "").replace("₹", "").replace("€", "")
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return number / 100 if percent else number


def _numeric_columns(columns: list[str], rows: list[dict[str, Any]]) -> list[str]:
    numeric: list[str] = []
    for column in columns:
        values = [row.get(column) for row in rows if row.get(column) not in (None, "")]
        if not values:
            continue
        if all(_coerce_number(value) is not None for value in values):
            numeric.append(column)
    return numeric


def _is_time_like(column: str, rows: list[dict[str, Any]]) -> bool:
    column_lower = column.lower()
    if any(token in column_lower for token in ("date", "time", "month", "year", "day", "week", "quarter")):
        return True

    sample = [row.get(column) for row in rows if row.get(column) not in (None, "")]
    if not sample:
        return False
    if all(_coerce_number(value) is not None for value in sample[:10]):
        return False

    parsed = pd.to_datetime(sample[:20], errors="coerce", format="mixed")
    return float(parsed.notna().mean()) >= 0.6


def _sort_rows_chronologically(rows: list[dict[str, Any]], column: str) -> list[dict[str, Any]]:
    """Order rows by a time-like category so trend charts read left-to-right in time,
    regardless of how the SQL happened to order them (e.g. ORDER BY the metric)."""
    parsed = pd.to_datetime([row.get(column) for row in rows], errors="coerce", format="mixed")
    indexed = list(enumerate(rows))
    indexed.sort(key=lambda item: (1, item[0]) if pd.isna(parsed[item[0]]) else (0, parsed[item[0]]))
    return [row for _, row in indexed]


def _is_low_cardinality_dimension(rows: list[dict[str, Any]], column: str) -> bool:
    return 1 < len(_ordered_labels(rows, column)) <= 12


def _ordered_labels(rows: list[dict[str, Any]], column: str) -> list[str]:
    ordered: list[str] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        label = _label(row.get(column))
        key = (type(label).__name__, label)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(label)
    return ordered


def _label(value: Any) -> str:
    if value is None:
        return "Unknown"
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:  # pragma: no cover - defensive conversion
            pass
    text = str(value).strip()
    return text or "Unknown"


_CHART_BUILDERS = {
    "table": _build_table_chart,
    "kpi": _build_kpi_chart,
    "bar": lambda columns, rows, mapping, options: _build_simple_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="bar",
    ),
    "stacked_bar": lambda columns, rows, mapping, options: _build_stacked_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="bar",
    ),
    "smooth_line": lambda columns, rows, mapping, options: _build_simple_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="line",
    ),
    "area": lambda columns, rows, mapping, options: _build_simple_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="line",
        area_style=True,
    ),
    "stacked_line": lambda columns, rows, mapping, options: _build_stacked_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="line",
    ),
    "stacked_area": lambda columns, rows, mapping, options: _build_stacked_axis_chart(
        columns,
        rows,
        mapping,
        options,
        series_type="line",
        area_style=True,
    ),
    "waterfall": _build_waterfall_chart,
    "mixed": _build_mixed_chart,
    "doughnut": _build_doughnut_chart,
    "scatter": _build_scatter_chart,
    "bubble": _build_bubble_chart,
    "heatmap": _build_heatmap_chart,
}
