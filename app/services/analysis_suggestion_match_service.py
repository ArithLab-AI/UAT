import re

from app.services.analysis_profile_service import DataSuggestion


SUPPORTED_CLEANING_PROMPT_TYPES = {
    "age_normalization",
    "boolean_validation",
    "date_normalization",
    "duplicate_removal",
    "email_normalization",
    "float_validation",
    "header_type_normalization",
    "integer_validation",
    "missing_value_normalization",
    "numeric_normalization",
    "phone_normalization",
    "text_normalization",
}


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


def _normalize_targets(values: list[str] | None) -> set[str]:
    return {
        value.strip().lower()
        for value in (values or [])
        if isinstance(value, str) and value.strip()
    }


def normalize_cleaning_prompt_type(
    cleaning_prompt_type: str | None,
    *,
    resolution_prompt: str | None = None,
    issue_description: str | None = None,
) -> str | None:
    normalized_type = _normalize_text(cleaning_prompt_type).replace(" ", "_")
    if normalized_type:
        normalized_type = normalized_type.replace("-", "_")
    if normalized_type in SUPPORTED_CLEANING_PROMPT_TYPES:
        return normalized_type

    alias_map = {
        "date_format_normalization": "date_normalization",
        "missing_value_replacement": "missing_value_normalization",
        "missing_value_imputation": "missing_value_normalization",
        "deduplication": "duplicate_removal",
    }
    if normalized_type in alias_map:
        return alias_map[normalized_type]

    category = (
        classify_issue_category(resolution_prompt, cleaning_prompt_type)
        or classify_issue_category(issue_description, cleaning_prompt_type)
    )
    category_to_prompt_type = {
        "age": "age_normalization",
        "boolean": "boolean_validation",
        "date": "date_normalization",
        "duplicate": "duplicate_removal",
        "email": "email_normalization",
        "float": "float_validation",
        "integer": "integer_validation",
        "missing": "missing_value_normalization",
        "numeric": "numeric_normalization",
        "phone": "phone_normalization",
        "schema_type": "header_type_normalization",
        "text": "text_normalization",
    }
    return category_to_prompt_type.get(category)


def classify_issue_category(
    value: str | None,
    cleaning_prompt_type: str | None = None,
) -> str | None:
    normalized_type = _normalize_text(cleaning_prompt_type).replace(" ", "_")
    if normalized_type:
        normalized_type = normalized_type.replace("-", "_")
        type_to_category = {
            "age_normalization": "age",
            "boolean_validation": "boolean",
            "integer_validation": "integer",
            "float_validation": "float",
            "phone_normalization": "phone",
            "date_normalization": "date",
            "date_format_normalization": "date",
            "missing_value_normalization": "missing",
            "missing_value_replacement": "missing",
            "missing_value_imputation": "missing",
            "duplicate_removal": "duplicate",
            "deduplication": "duplicate",
            "text_normalization": "text",
            "numeric_normalization": "numeric",
            "email_normalization": "email",
            "header_type_normalization": "schema_type",
            "formatting": "formatting",
            "format_standardization": "formatting",
            "replacement": "formatting",
        }
        if normalized_type in type_to_category:
            return type_to_category[normalized_type]

    normalized = _normalize_text(value)
    if not normalized:
        return None

    category_terms = {
        "age": ("age", "ages", "age-like", "age values", "whole-number ages", "plausible ages", "spelled-out age"),
        "boolean": ("boolean", "true", "false", "1, and 0", "1 or 0"),
        "integer": ("strict integer", "valid integers", "non-integer", "integer-like"),
        "float": ("valid float", "float-like", "not valid floats"),
        "phone": ("phone", "mobile", "contact number", "country code", "phone-like"),
        "date": ("date", "dates", "datetime", "timestamp", "date format"),
        "missing": ("missing", "null", "blank", "empty", "placeholder"),
        "duplicate": ("duplicate", "duplicates", "deduplicate", "redundant copies"),
        "text": ("whitespace", "casing", "text normalization", "normalize text", "repeated text"),
        "numeric": ("numeric", "numbers", "currency", "separators", "parse cleanly as numbers"),
        "email": ("email", "e-mail", "mail address"),
        "schema_type": (
            "header semantics",
            "type guide",
            "text-like values",
            "text/string type",
            "column headers as the type guide",
            "non-text placeholders",
        ),
        "formatting": ("format", "formatting", "standard conventions", "standardize format"),
    }

    for category, terms in category_terms.items():
        if any(term in normalized for term in terms):
            return category
    return None


def suggestion_matches_cleaned_step(
    *,
    cleaned_prompt: str | None,
    cleaned_prompt_type: str | None,
    cleaned_target_columns: list[str] | None,
    suggestion: DataSuggestion,
) -> bool:
    normalized_cleaned_prompt = _normalize_text(cleaned_prompt)
    if not normalized_cleaned_prompt:
        return False

    normalized_suggestion_prompt = _normalize_text(suggestion.resolution_prompt)
    if normalized_suggestion_prompt == normalized_cleaned_prompt:
        return True

    cleaned_category = classify_issue_category(cleaned_prompt, cleaned_prompt_type)
    suggestion_category = (
        classify_issue_category(suggestion.resolution_prompt, suggestion.cleaning_prompt_type)
        or classify_issue_category(suggestion.issue_description, suggestion.cleaning_prompt_type)
    )
    if not cleaned_category or not suggestion_category or cleaned_category != suggestion_category:
        return False

    cleaned_targets = _normalize_targets(cleaned_target_columns)
    suggestion_targets = _normalize_targets(suggestion.target_columns)
    if not cleaned_targets or not suggestion_targets:
        return True
    return bool(cleaned_targets & suggestion_targets)


def split_matching_suggestions(
    *,
    cleaned_prompt: str | None,
    cleaned_prompt_type: str | None,
    cleaned_target_columns: list[str] | None,
    suggestions: list[DataSuggestion],
) -> tuple[list[DataSuggestion], list[DataSuggestion]]:
    matching: list[DataSuggestion] = []
    non_matching: list[DataSuggestion] = []
    for suggestion in suggestions:
        if suggestion_matches_cleaned_step(
            cleaned_prompt=cleaned_prompt,
            cleaned_prompt_type=cleaned_prompt_type,
            cleaned_target_columns=cleaned_target_columns,
            suggestion=suggestion,
        ):
            matching.append(suggestion)
        else:
            non_matching.append(suggestion)
    return matching, non_matching
