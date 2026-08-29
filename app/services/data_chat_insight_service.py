"""Deterministic statistics behind the data-chat ``insight`` field.

Everything a user could check with a calculator is computed here from the actual
result values -- totals, spreads, outliers, correlation coefficients. The LLM is
only asked to narrate these numbers, never to invent them, so a stated figure
always matches the data that was returned alongside it.

Column roles are decided by parsing the values, not by reading column names: a
"Pincode" or "RollNo" column holds numbers but is an identifier, and averaging it
would be meaningless.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Identifier detection. Near-uniqueness alone is not enough: in an aggregated result
# ("revenue per city") every measure value is unique too. What separates a real
# identifier is that its whole-number values pack a contiguous range (101,102,103...)
# or barely vary at all (pincodes in one city), so no aggregate over them means anything.
IDENTIFIER_UNIQUE_RATIO = 0.95
IDENTIFIER_DENSITY_RATIO = 0.9
# Two scale-free properties, either of which marks a key:
#   1. The values hug their own magnitude -- std is tiny next to the mean. True of any
#      offset key range (101.., 5001.., pincodes, years) whatever its size.
#   2. The values are a complete run starting at 0 or 1 -- plain row numbering.
# A measured quantity satisfies neither: a count of 2..6 varies far too much for its
# size (ratio 0.40) and does not start at 1.
IDENTIFIER_MAX_SPREAD_RATIO = 0.1
IDENTIFIER_MIN_ROWS = 5
MIN_ROWS_FOR_CORRELATION = 3
MIN_ABS_CORRELATION = 0.1
MAX_CORRELATION_PAIRS = 10
MAX_HIGHLIGHTS_PER_METRIC = 3
MAX_CROSS_CATEGORY_LINKS = 3
MAX_CATEGORY_COLUMNS_PAIRED = 4
MAX_METRICS_PROFILED = 6
MAX_OUTLIERS_PER_METRIC = 5

_STRENGTH_BANDS = (
    (0.7, "strong"),
    (0.3, "moderate"),
)


def _clean_float(value: Any) -> float | int | None:
    """NaN/inf never survive into the response -- they are not valid JSON.

    Whole numbers come back as ints so a count of 6 is reported as 6, not 6.0. The
    narrative quotes these values verbatim, so the stored form is what the reader sees.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    rounded = round(number, 4)
    return int(rounded) if float(rounded).is_integer() else rounded


def _clean_percent(value: Any) -> float | int | None:
    """Percentages are read, not recomputed, so one decimal is enough.

    Four decimals on a share ("34.2371%") reads as false precision in a sentence the
    narrative quotes verbatim.
    """
    percent = _clean_float(value)
    if percent is None:
        return None
    rounded = round(float(percent), 1)
    return int(rounded) if float(rounded).is_integer() else rounded


def _format_number(value: Any) -> str:
    """Render a stat the way a reader expects: 42,000 rather than 42000.0."""
    if value is None:
        return "unknown"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}"


def _strength_label(coefficient: float) -> str:
    magnitude = abs(coefficient)
    for threshold, label in _STRENGTH_BANDS:
        if magnitude > threshold:
            return label
    return "weak"


def _looks_like_identifier(numeric: pd.Series, row_count: int) -> bool:
    """True for key-like numeric columns (ids, roll numbers, pincodes, years).

    Judged only from the values, so it holds for any dataset: whole numbers that are
    near-unique AND either barely vary relative to their own size, or form a complete
    run from 0/1. Aggregated measures fail both -- revenue spans orders of magnitude,
    and a count of 2..6 varies far too much for its size.

    Two genuinely ambiguous shapes exist. Counts that happen to run 1..N are read as
    row numbering, and a key range starting at 2 or higher with a wide spread is read
    as a measure. Neither is separable from values alone, so the rule prefers the
    reading that loses less: a spurious measure only adds an unused statistic.
    """
    if row_count < IDENTIFIER_MIN_ROWS or numeric.empty:
        return False
    if not bool((numeric == numeric.round()).all()):
        return False
    if float(numeric.nunique()) / float(row_count) < IDENTIFIER_UNIQUE_RATIO:
        return False

    mean = float(numeric.mean())
    if mean:
        std_dev = float(numeric.std()) if numeric.count() > 1 else 0.0
        if abs(std_dev / mean) < IDENTIFIER_MAX_SPREAD_RATIO:
            return True

    minimum = float(numeric.min())
    span = float(numeric.max()) - minimum + 1.0
    density = float(numeric.nunique()) / span if span > 0 else 0.0
    return minimum <= 1.0 and density >= IDENTIFIER_DENSITY_RATIO


def _classify_columns(frame: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    """Split result columns into (measures, labels, identifiers) by inspecting values."""
    measures: list[str] = []
    labels: list[str] = []
    identifiers: list[str] = []
    row_count = len(frame)

    for column in frame.columns:
        series = frame[column]
        non_null = series.dropna()
        if non_null.empty:
            labels.append(column)
            continue

        numeric = pd.to_numeric(non_null, errors="coerce")
        numeric_ratio = float(numeric.notna().sum()) / float(len(non_null))
        if numeric_ratio < 0.9:
            labels.append(column)
            continue

        if _looks_like_identifier(numeric.dropna(), row_count):
            identifiers.append(column)
            continue

        measures.append(column)

    return measures, labels, identifiers


def _profile_measure(frame: pd.DataFrame, column: str) -> dict[str, Any] | None:
    numeric = pd.to_numeric(frame[column], errors="coerce").dropna()
    if numeric.empty:
        return None

    total = _clean_float(numeric.sum())
    profile: dict[str, Any] = {
        "column": column,
        "count": int(numeric.count()),
        "total": total,
        "min": _clean_float(numeric.min()),
        "max": _clean_float(numeric.max()),
        "mean": _clean_float(numeric.mean()),
        "median": _clean_float(numeric.median()),
        # A single row has no spread; std would be NaN.
        "std_dev": _clean_float(numeric.std()) if numeric.count() > 1 else 0.0,
    }

    spread = profile["max"] - profile["min"] if None not in (profile["max"], profile["min"]) else None
    profile["range"] = _clean_float(spread) if spread is not None else None
    mean = profile["mean"]
    std_dev = profile["std_dev"]
    # Coefficient of variation says whether values cluster or scatter, which is what
    # makes a "high" value genuinely unusual rather than just the top of a flat list.
    profile["spread_ratio"] = (
        _clean_float(std_dev / mean) if mean not in (None, 0) and std_dev is not None else None
    )
    return profile


def _highlights(
    frame: pd.DataFrame,
    measure: str,
    label_column: str | None,
) -> list[dict[str, Any]]:
    """Highest and lowest rows for one measure, each with its share of the total."""
    numeric = pd.to_numeric(frame[measure], errors="coerce")
    valid = frame.loc[numeric.notna()].copy()
    if valid.empty:
        return []

    valid["__value"] = numeric.loc[numeric.notna()]
    total = float(valid["__value"].sum())
    ordered = valid.sort_values("__value", ascending=False)
    take = min(MAX_HIGHLIGHTS_PER_METRIC, len(ordered))

    entries: list[dict[str, Any]] = []
    seen_positions: set[int] = set()
    for kind, subset in (("high", ordered.head(take)), ("low", ordered.tail(take).iloc[::-1])):
        for position, (_, row) in enumerate(subset.iterrows()):
            key = hash((kind, position, str(row.get(label_column)) if label_column else position))
            if key in seen_positions:
                continue
            seen_positions.add(key)
            value = _clean_float(row["__value"])
            entries.append(
                {
                    "metric": measure,
                    "label": str(row[label_column]) if label_column else f"row {position + 1}",
                    "value": value,
                    "type": kind,
                    "share_of_total_pct": (
                        _clean_percent(value / total * 100) if total and value is not None else None
                    ),
                }
            )
    return entries


def _outliers(frame: pd.DataFrame, measure: str, label_column: str | None) -> list[dict[str, Any]]:
    """Values outside 1.5*IQR -- the usual definition of "unusually high/low"."""
    numeric = pd.to_numeric(frame[measure], errors="coerce").dropna()
    if numeric.count() < 4:
        return []

    q1 = float(numeric.quantile(0.25))
    q3 = float(numeric.quantile(0.75))
    iqr = q3 - q1
    if iqr <= 0:
        return []

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (numeric < lower) | (numeric > upper)
    flagged = frame.loc[numeric.index[mask]]

    entries: list[dict[str, Any]] = []
    for index, row in flagged.head(MAX_OUTLIERS_PER_METRIC).iterrows():
        value = _clean_float(numeric.loc[index])
        if value is None:
            continue
        entries.append(
            {
                "metric": measure,
                "label": str(row[label_column]) if label_column else f"row {index}",
                "value": value,
                "direction": "unusually high" if value > upper else "unusually low",
            }
        )
    return entries


def _correlations(frame: pd.DataFrame, measures: list[str]) -> list[dict[str, Any]]:
    """Pearson correlation for every measure pair, strongest first."""
    if len(measures) < 2 or len(frame) < MIN_ROWS_FOR_CORRELATION:
        return []

    numeric_frame = frame[measures].apply(pd.to_numeric, errors="coerce")
    # A constant column has zero variance, so its correlation is undefined, not zero.
    varying = [column for column in measures if numeric_frame[column].nunique(dropna=True) > 1]
    if len(varying) < 2:
        return []

    try:
        matrix = numeric_frame[varying].corr(method="pearson")
    except Exception:  # noqa: BLE001 - correlation is a bonus, never a hard failure
        logger.exception("Correlation computation failed for columns=%s", varying)
        return []

    pairs: list[dict[str, Any]] = []
    for i, left in enumerate(varying):
        for right in varying[i + 1:]:
            coefficient = _clean_float(matrix.loc[left, right])
            if coefficient is None or abs(coefficient) < MIN_ABS_CORRELATION:
                continue
            pairs.append(
                {
                    "columns": [left, right],
                    "coefficient": coefficient,
                    "direction": "positive" if coefficient > 0 else "negative",
                    "strength": _strength_label(coefficient),
                    # r squared: the share of one column's variation the other accounts for.
                    "variance_explained_pct": _clean_percent(coefficient * coefficient * 100),
                    "sample_size": int(numeric_frame[[left, right]].dropna().shape[0]),
                }
            )

    pairs.sort(key=lambda item: abs(item["coefficient"]), reverse=True)
    return pairs[:MAX_CORRELATION_PAIRS]


def _category_breakdown(frame: pd.DataFrame, label_column: str) -> dict[str, Any] | None:
    counts = frame[label_column].dropna().astype(str).value_counts()
    if counts.empty:
        return None
    total = int(counts.sum())
    top = counts.head(3)
    return {
        "column": label_column,
        "distinct_values": int(counts.size),
        "most_common": [
            {"value": str(value), "count": int(count), "share_pct": _clean_percent(count / total * 100)}
            for value, count in top.items()
        ],
        # High concentration is itself the explanation for a lopsided result.
        "top_3_share_pct": _clean_percent(float(top.sum()) / total * 100),
    }


def _distribution(
    frame: pd.DataFrame,
    measure: str,
    label_column: str | None,
) -> dict[str, Any] | None:
    """Top vs bottom performer for one measure, with the gap expressed as a multiple.

    Answers "is this dominated by one category or fairly balanced" and "how many times
    bigger is the leader" without the reader doing any arithmetic.
    """
    numeric = pd.to_numeric(frame[measure], errors="coerce")
    valid = frame.loc[numeric.notna()].copy()
    if valid.empty:
        return None

    valid["__value"] = numeric.loc[numeric.notna()]
    total = float(valid["__value"].sum())
    ordered = valid.sort_values("__value", ascending=False)
    top_row = ordered.iloc[0]
    bottom_row = ordered.iloc[-1]

    def _entry(row: pd.Series) -> dict[str, Any]:
        value = _clean_float(row["__value"])
        return {
            "label": str(row[label_column]) if label_column else "(unlabelled)",
            "value": value,
            "share_pct": _clean_percent(value / total * 100) if total and value is not None else None,
        }

    top = _entry(top_row)
    bottom = _entry(bottom_row)
    ratio = None
    if bottom["value"]:
        ratio = _clean_float(abs(top["value"] / bottom["value"]))

    top_share = top["share_pct"]
    return {
        "metric": measure,
        "distinct_labels": int(valid[label_column].nunique()) if label_column else len(valid),
        "total": _clean_float(total),
        "top": top,
        "bottom": bottom,
        "ratio_top_to_bottom": ratio,
        # A single category holding more than half the total is the pattern worth naming.
        "dominated_by_one": bool(top_share is not None and top_share > 50),
    }


def _cross_category_links(
    frame: pd.DataFrame,
    label_columns: list[str],
) -> list[dict[str, Any]]:
    """Value pairs from two different categorical columns that occur together most often.

    Needs at least two categorical columns, so an already-aggregated result grouped by a
    single dimension produces nothing -- correctly, since there is no pair to relate.
    """
    usable = label_columns[:MAX_CATEGORY_COLUMNS_PAIRED]
    if len(usable) < 2 or frame.empty:
        return []

    total = len(frame)
    links: list[dict[str, Any]] = []
    for index, left in enumerate(usable):
        for right in usable[index + 1:]:
            pair = frame[[left, right]].dropna()
            if pair.empty:
                continue
            counts = pair.astype(str).groupby([left, right]).size()
            if counts.empty:
                continue
            left_value, right_value = counts.idxmax()
            count = int(counts.max())
            links.append(
                {
                    "columns": [left, right],
                    "values": [str(left_value), str(right_value)],
                    "co_occurrence_count": count,
                    "co_occurrence_pct": _clean_percent(count / total * 100),
                }
            )

    links.sort(key=lambda item: item["co_occurrence_pct"] or 0, reverse=True)
    return links[:MAX_CROSS_CATEGORY_LINKS]


def compute_result_statistics(
    columns: list[str],
    rows: list[dict[str, Any]],
    total_rows: int | None = None,
) -> dict[str, Any]:
    """Deterministic profile of one query result, used to ground the insight narrative."""
    matched_rows = len(rows) if total_rows is None else int(total_rows)
    base: dict[str, Any] = {
        "rows_analysed": len(rows),
        "rows_matched": matched_rows,
        "sampled": matched_rows > len(rows),
        "measures": [],
        "highlights": [],
        "outliers": [],
        "correlations": [],
        "distributions": [],
        "cross_category_links": [],
        "breakdown": None,
    }
    if not rows:
        return base

    try:
        frame = pd.DataFrame(rows, columns=columns or None)
    except Exception:  # noqa: BLE001 - a malformed result must not break the query response
        logger.exception("Could not build a frame from the data-chat result")
        return base

    if frame.empty:
        return base

    measures, labels, identifiers = _classify_columns(frame)
    base["identifier_columns"] = identifiers
    base["label_columns"] = labels
    label_column = labels[0] if labels else None

    profiled = measures[:MAX_METRICS_PROFILED]
    base["measures"] = [
        profile
        for profile in (_profile_measure(frame, column) for column in profiled)
        if profile is not None
    ]
    for measure in profiled:
        base["highlights"].extend(_highlights(frame, measure, label_column))
        base["outliers"].extend(_outliers(frame, measure, label_column))
        distribution = _distribution(frame, measure, label_column)
        if distribution is not None:
            base["distributions"].append(distribution)

    base["correlations"] = _correlations(frame, measures)
    base["cross_category_links"] = _cross_category_links(frame, labels)
    if label_column:
        base["breakdown"] = _category_breakdown(frame, label_column)
    return base


def build_fallback_insight(statistics: dict[str, Any]) -> dict[str, Any]:
    """The same six sections, assembled from the numbers alone.

    Used when the LLM is unavailable or returns nothing, so the field stays genuinely
    useful instead of empty. Every sentence here is filled from a computed value.
    """
    measures = statistics.get("measures") or []
    distributions = statistics.get("distributions") or []
    correlations = statistics.get("correlations") or []
    links = statistics.get("cross_category_links") or []
    breakdown = statistics.get("breakdown") or {}

    primary = distributions[0] if distributions else None
    metric = primary["metric"] if primary else (measures[0]["column"] if measures else None)

    executive_summary = ""
    if primary:
        executive_summary = (
            f"{metric} varies across {primary['distinct_labels']} categories, "
            f"with {primary['top']['label']} leading at {_format_number(primary['top']['value'])}."
        )
    elif measures:
        profile = measures[0]
        executive_summary = (
            f"{profile['column']} ranges from {_format_number(profile['min'])} to "
            f"{_format_number(profile['max'])}, averaging {_format_number(profile['mean'])}."
        )
    else:
        executive_summary = (
            f"This result has {statistics.get('rows_analysed', 0)} row(s) and no numeric "
            "column to measure, so only the categories can be described."
        )

    observations: list[str] = []
    if primary:
        top, bottom = primary["top"], primary["bottom"]
        observations.append(
            f"{top['label']} has the highest {metric} ({_format_number(top['value'])}), "
            f"{_format_number(top['share_pct'])}% of the total."
        )
        observations.append(
            f"{bottom['label']} has the lowest {metric} ({_format_number(bottom['value'])}), "
            f"{_format_number(bottom['share_pct'])}% of the total."
        )
        observations.append(f"There are {primary['distinct_labels']} distinct categories.")
    for profile in measures[:2]:
        observations.append(
            f"{profile['column']} ranges from {_format_number(profile['min'])} to "
            f"{_format_number(profile['max'])}, averaging {_format_number(profile['mean'])} "
            f"(middle value {_format_number(profile['median'])})."
        )

    patterns: list[str] = []
    if primary:
        patterns.append(
            f"The spread of {metric} is dominated by a single category, "
            f"{primary['top']['label']}."
            if primary["dominated_by_one"]
            else f"The spread of {metric} is relatively balanced across categories."
        )
    if breakdown.get("top_3_share_pct") is not None:
        patterns.append(
            f"The top 3 values of {breakdown['column']} account for "
            f"{_format_number(breakdown['top_3_share_pct'])}% of all rows."
        )

    comparative: list[str] = []
    if primary and primary.get("ratio_top_to_bottom"):
        comparative.append(
            f"{primary['top']['label']} is about "
            f"{_format_number(primary['ratio_top_to_bottom'])} times the "
            f"{metric} of {primary['bottom']['label']}."
        )

    correlation_insights: list[str] = []
    for pair in correlations[:3]:
        left, right = pair["columns"]
        movement = "rises" if pair["direction"] == "positive" else "falls"
        correlation_insights.append(
            f"{left} and {right} show a {pair['strength']} {pair['direction']} relationship "
            f"(r = {pair['coefficient']}): as {left} goes up, {right} usually {movement}. "
            f"Changes in {left} account for about "
            f"{_format_number(pair['variance_explained_pct'])}% of the variation in {right}."
        )
    for link in links[:2]:
        left_column, right_column = link["columns"]
        left_value, right_value = link["values"]
        correlation_insights.append(
            f"{left_value} ({left_column}) most often appears together with "
            f"{right_value} ({right_column}), in "
            f"{_format_number(link['co_occurrence_pct'])}% of rows."
        )

    recommendations: list[str] = []
    if primary:
        recommendations.append(
            f"Look at {primary['top']['label']} first, since it carries the largest share of {metric}."
        )
        recommendations.append(
            f"Check why {primary['bottom']['label']} is lowest before deciding whether it needs action."
        )
    if correlations:
        left, right = correlations[0]["columns"]
        recommendations.append(
            f"Investigate the link between {left} and {right} before treating either as a lever."
        )

    return {
        "executive_summary": executive_summary,
        "data_observations": observations,
        "important_patterns": patterns,
        "comparative_analysis": comparative,
        "correlation_insights": correlation_insights,
        "actionable_recommendations": recommendations,
        "caveats": ["This description was generated from the numbers only, without an AI narrative."],
    }
