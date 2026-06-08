from __future__ import annotations

from typing import Any

from app.services.analysis_profile_service import DataSuggestion
from app.services.analysis_suggestion_match_service import classify_issue_category


def _normalize_column_name(value: str | None) -> str:
    return str(value or "").strip().lower()


def _resolve_profile_columns(profile: dict[str, Any], target_columns: list[str] | None) -> list[dict[str, Any]]:
    columns = profile.get("columns") if isinstance(profile.get("columns"), list) else []
    if not target_columns:
        return [column for column in columns if isinstance(column, dict)]

    target_names = {_normalize_column_name(column) for column in target_columns if _normalize_column_name(column)}
    matched_columns = [
        column
        for column in columns
        if isinstance(column, dict) and _normalize_column_name(column.get("name")) in target_names
    ]
    return matched_columns or [column for column in columns if isinstance(column, dict)]


def _has_positive_metric(column_profiles: list[dict[str, Any]], *metric_names: str) -> bool:
    for column in column_profiles:
        for metric_name in metric_names:
            try:
                if float(column.get(metric_name, 0.0)) > 0.0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _is_duplicate_suggestion_actionable(profile: dict[str, Any], target_columns: list[str] | None) -> bool:
    try:
        if float(profile.get("duplicate_row_percent", 0.0)) > 0.0:
            return True
    except (TypeError, ValueError):
        pass

    column_profiles = _resolve_profile_columns(profile, target_columns)
    return _has_positive_metric(column_profiles, "key_duplicate_percent")


def suggestion_is_actionable(profile: dict[str, Any], suggestion: DataSuggestion) -> bool:
    category = (
        classify_issue_category(suggestion.resolution_prompt, suggestion.cleaning_prompt_type)
        or classify_issue_category(suggestion.issue_description, suggestion.cleaning_prompt_type)
    )
    if not category:
        return True

    if category == "duplicate":
        return _is_duplicate_suggestion_actionable(profile, suggestion.target_columns)

    column_profiles = _resolve_profile_columns(profile, suggestion.target_columns)
    if not column_profiles:
        return True

    metric_map = {
        "missing": ("null_percent",),
        "email": ("invalid_email_percent",),
        "phone": ("invalid_phone_percent", "phone_format_inconsistency_percent"),
        "date": ("invalid_date_percent", "date_format_inconsistency_percent"),
        "age": ("invalid_age_percent", "age_format_inconsistency_percent"),
        "integer": ("invalid_integer_percent",),
        "float": ("invalid_float_percent",),
        "boolean": ("invalid_boolean_percent",),
        "numeric": ("invalid_numeric_percent", "numeric_format_inconsistency_percent"),
        "text": ("casing_inconsistency_percent", "whitespace_inconsistency_percent"),
        "schema_type": ("semantic_type_mismatch_percent",),
        "formatting": (
            "invalid_email_percent",
            "invalid_phone_percent",
            "phone_format_inconsistency_percent",
            "invalid_date_percent",
            "date_format_inconsistency_percent",
            "invalid_numeric_percent",
            "numeric_format_inconsistency_percent",
            "invalid_integer_percent",
            "invalid_float_percent",
            "invalid_boolean_percent",
            "invalid_age_percent",
            "age_format_inconsistency_percent",
            "casing_inconsistency_percent",
            "whitespace_inconsistency_percent",
        ),
    }
    metric_names = metric_map.get(category)
    if not metric_names:
        return True
    return _has_positive_metric(column_profiles, *metric_names)


def filter_actionable_suggestions(
    profile: dict[str, Any],
    suggestions: list[DataSuggestion],
) -> list[DataSuggestion]:
    return [suggestion for suggestion in suggestions if suggestion_is_actionable(profile, suggestion)]
