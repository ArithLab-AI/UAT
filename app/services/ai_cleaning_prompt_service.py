import ast
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from app.config.config import settings
from app.services.ai_cleaning_privacy_service import select_llm_safe_columns
from app.services.analysis_profile_service import (
    DATE_COLUMN_HINTS,
    DATE_OUTPUT_FORMAT,
    EMAIL_COLUMN_HINTS,
    NULL_OUTPUT_TOKEN,
    NUMERIC_COLUMN_HINTS,
    PHONE_COLUMN_HINTS,
    PHONE_PATTERN,
    TEXT_SEMANTIC_COLUMN_HINTS,
    _is_age_column_name,
    _is_boolean_column_name,
    _is_float_column_name,
    _is_integer_column_name,
    _parse_age_value,
    _parse_boolean_value,
    _parse_datetime_series,
    _parse_float_value,
    _parse_strict_integer_value,
    _to_snake_case,
)
from app.utils.openai_utils import get_openai_client

logger = logging.getLogger(__name__)

_DEFAULT_NULL_TOKENS = {
    "n/a",
    "na",
    "null",
    "none",
    "-",
    "--",
    "",
    "n.a.",
    "nil",
    "missing",
    "not available",
    "not provided",
    "blank",
    "#n/a",
    "nan",
    "tbd",
}

_SYSTEM_CLEAN_DATA_PROMPT = """
You are a strict, precise data cleaning AI assistant.
Your task is to take a batch of JSON rows from a dataset and apply a specific cleaning instruction provided by the user.
You must not alter any data that is not relevant to the provided instruction.

Rules:
1. Do not hallucinate, guess missing data, or alter meaning unless the prompt explicitly requires it.
2. Only apply changes that fulfill the user's prompt: "{user_prompt}"
3. Return only a valid JSON array of rows.
4. Preserve the exact number of rows and the exact same column keys in every row.
5. If the prompt asks for a literal placeholder such as "NaN", "NULL", or "N/A", write that exact string value, not JSON null.
6. If the instruction targets empty values, treat nulls, blank strings, and visibly empty cells as empty values.
"""

_USER_CLEAN_DATA_PROMPT = """
Here is a batch of rows in JSON format:
{batch_json}

Apply the cleaning instruction: "{user_prompt}"

Return strictly valid JSON containing the updated array of objects and nothing else.
"""

_clean_data_prompt_template = ChatPromptTemplate.from_messages(
    [
        ("system", _SYSTEM_CLEAN_DATA_PROMPT),
        ("user", _USER_CLEAN_DATA_PROMPT),
    ]
)


@dataclass(frozen=True)
class AICleaningPlan:
    strategy: str
    target_columns: list[str]
    reason: str | None = None

    @property
    def requires_ai(self) -> bool:
        return cleaning_strategy_uses_llm(self.strategy)


def cleaning_strategy_uses_llm(strategy: str | None) -> bool:
    return strategy in {"value_mapping_ai", "row_level_ai"}


def _coerce_python_payload(candidate: str) -> Any:
    payload = ast.literal_eval(candidate)
    if isinstance(payload, (dict, list)):
        return payload
    raise ValueError(f"Unsupported payload type: {type(payload)}")


def _extract_json_payload(raw_response: str) -> Any:
    candidates = []
    fenced_match = re.search(r"```json\s*(.*?)```", raw_response, re.DOTALL | re.IGNORECASE)
    if fenced_match:
        candidates.append(fenced_match.group(1).strip())

    stripped = raw_response.strip()
    if stripped:
        candidates.append(stripped)

    array_match = re.search(r"(\[\s*.*\s*\])", raw_response, re.DOTALL)
    if array_match:
        candidates.append(array_match.group(1).strip())

    object_match = re.search(r"(\{\s*.*\s*\})", raw_response, re.DOTALL)
    if object_match:
        candidates.append(object_match.group(1).strip())

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

        try:
            return _coerce_python_payload(candidate)
        except Exception:
            continue

    raise ValueError("Model response did not contain a valid JSON payload.")


def _normalize_cleaned_batch_payload(cleaned_batch: Any) -> Any:
    if isinstance(cleaned_batch, dict):
        for value in cleaned_batch.values():
            if isinstance(value, list):
                return value
    return cleaned_batch


def _build_cleaning_chain():
    model = (settings.UAT_AI_CLEANING_MODEL or settings.UAT_ANALYSIS_LLM_MODEL or "gpt-4.1-mini").strip()
    llm = get_openai_client().bind(
        model=model,
        temperature=float(settings.UAT_AI_CLEANING_TEMPERATURE),
        max_tokens=int(settings.UAT_AI_CLEANING_MAX_TOKENS),
    )
    return _clean_data_prompt_template | llm | StrOutputParser()


def build_cleaning_chain():
    return _build_cleaning_chain()


def prompt_has_deterministic_cleaning_steps(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    phone_terms = [
        "phone",
        "phones",
        "mobile",
        "contact number",
        "country code",
        "phone-like",
        "punctuation noise",
    ]
    text_terms = [
        "trim leading and trailing whitespace",
        "leading and trailing whitespace",
        "standardize casing",
        "standardize case",
        "trim whitespace",
        "strip spaces",
        "whitespace inconsistencies",
        "casing inconsistencies",
        "repeated text values",
        "text normalization",
        "normalize text",
        "whitespace",
    ]
    numeric_terms = [
        "numeric-like",
        "numeric",
        "numbers",
        "currency symbols",
        "separators",
        "parse cleanly as numbers",
        "formatting noise",
    ]
    return (
        _should_normalize_headers(user_prompt)
        or _prompt_mentions_missing_values(user_prompt)
        or _should_trim_whitespace(user_prompt)
        or prompt_removes_exact_duplicates(user_prompt)
        or _prompt_requests_date_normalization(normalized_prompt)
        or _prompt_requests_age_normalization(normalized_prompt)
        or _prompt_requests_type_validation(normalized_prompt)
        or _prompt_requests_header_type_normalization(normalized_prompt)
        or any(term in normalized_prompt for term in phone_terms)
        or any(term in normalized_prompt for term in text_terms)
        or any(term in normalized_prompt for term in numeric_terms)
    )


def prompt_removes_exact_duplicates(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    duplicate_terms = [
        "duplicate",
        "duplicates",
        "deduplicate",
        "dedup",
        "redundant copies",
    ]
    removal_terms = [
        "remove",
        "drop",
        "delete",
        "keep one",
        "canonical",
    ]
    return (
        any(term in normalized_prompt for term in duplicate_terms)
        and any(term in normalized_prompt for term in removal_terms)
    ) or "exact duplicate" in normalized_prompt


def _prompt_mentions_missing_values(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    missing_terms = ["missing", "null", "blank", "empty", "n/a", "placeholder"]
    return any(term in normalized_prompt for term in missing_terms)


def _apply_text_cleaning(
    df: pd.DataFrame,
    *,
    normalize_missing: bool = False,
    trim_whitespace: bool = False,
) -> pd.DataFrame:
    if not normalize_missing and not trim_whitespace:
        return df

    normalized_df = df.copy()
    for column in normalized_df.columns:
        series = normalized_df[column]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue

        def _transform_value(value):
            if not isinstance(value, str):
                return value

            transformed = value.strip() if trim_whitespace else value
            if normalize_missing and transformed.strip().lower() in _DEFAULT_NULL_TOKENS:
                return pd.NA
            return transformed

        normalized_df[column] = series.map(_transform_value)

    return normalized_df


def _remove_exact_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().reset_index(drop=True)


def _remove_exact_duplicate_rows_across_chunks(
    df: pd.DataFrame,
    seen_row_hashes: set[int],
) -> pd.DataFrame:
    if df.empty:
        return df

    comparable_df = df.astype(object).where(pd.notnull(df), NULL_OUTPUT_TOKEN)
    row_hashes = pd.util.hash_pandas_object(comparable_df.astype(str), index=False)

    keep_mask = []
    local_hashes: set[int] = set()
    for row_hash in row_hashes.tolist():
        if row_hash in seen_row_hashes or row_hash in local_hashes:
            keep_mask.append(False)
            continue
        keep_mask.append(True)
        local_hashes.add(row_hash)

    seen_row_hashes.update(local_hashes)
    return df.loc[keep_mask].reset_index(drop=True)


def _should_normalize_headers(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    header_terms = ["header", "headers", "column name", "column names", "snake_case"]
    return any(term in normalized_prompt for term in header_terms)


def _should_trim_whitespace(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    whitespace_terms = ["whitespace", "leading or trailing", "trim", "strip spaces"]
    return any(term in normalized_prompt for term in whitespace_terms)


def _has_semantic_value_cleaning_terms(normalized_prompt: str) -> bool:
    strong_semantic_terms = [
        "fix typo",
        "fix typos",
        "typo",
        "typos",
        "correct spelling",
        "spelling",
        "standardize spelling",
        "map values",
        "map to",
        "category mapping",
        "categorize",
        "category",
        "capitalize",
        "capitalization",
        "proper case",
        "title case",
        "lowercase",
        "uppercase",
        "harmonize",
        "translate",
        "rewrite",
        "expand abbreviations",
        "abbreviation",
        "name formatting",
        "format names",
        "address formatting",
        "format addresses",
    ]
    if any(term in normalized_prompt for term in strong_semantic_terms):
        return True

    deterministic_format_patterns = [
        r"\bstandardi[sz]e\b.{0,50}\bphone\b.{0,50}\bformat\b",
        r"\bstandardi[sz]e\b.{0,50}\bphone\b.{0,50}\bvalues?\b",
        r"\bnormalize\b.{0,50}\bphone\b.{0,50}\bvalues?\b",
        r"\bconvert\b.{0,50}\bdate\b.{0,50}\byyyy[-_/]mm[-_/]dd\b",
        r"\bconvert\b.{0,50}\bdate\b.{0,50}\bparseable\b",
        r"\bnormalize\b.{0,50}\bnumeric\b.{0,50}\bformatting noise\b",
    ]
    if any(re.search(pattern, normalized_prompt) for pattern in deterministic_format_patterns):
        return False

    generic_semantic_terms = ["normalize values", "standardize values", "canonical form"]
    if any(term in normalized_prompt for term in generic_semantic_terms):
        return True

    semantic_patterns = [
        r"\bstandardi[sz]e\b.{0,50}\b(values?|labels?|titles?|cities?|states?|countries?|emails?|addresses?)\b",
        r"\bnormalize\b.{0,50}\b(values?|labels?|titles?|cities?|states?|countries?|emails?|addresses?)\b",
        r"\bcorrect\b.{0,50}\b(values?|titles?|cities?|states?|countries?|emails?|addresses?)\b",
    ]
    return any(re.search(pattern, normalized_prompt) for pattern in semantic_patterns)


def _is_header_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    header_terms = ["header", "headers", "column name", "column names", "snake_case"]
    return any(term in normalized_prompt for term in header_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_header_type_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    return _prompt_requests_header_type_normalization(normalized_prompt) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_type_validation_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    return _prompt_requests_type_validation(normalized_prompt) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_missing_value_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    missing_terms = ["missing", "null", "blank", "empty", "n/a", "placeholder"]
    return any(term in normalized_prompt for term in missing_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_date_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    return _prompt_requests_date_normalization(normalized_prompt) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_age_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    return _prompt_requests_age_normalization(normalized_prompt) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_phone_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    phone_terms = [
        "phone",
        "phones",
        "mobile",
        "contact number",
        "country code",
        "phone-like",
        "punctuation noise",
    ]
    return any(term in normalized_prompt for term in phone_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _should_keep_only_valid_phone_rows(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    keep_terms = [
        "keep only valid",
        "only valid",
        "remove invalid",
        "drop invalid",
        "valid phone-like",
        "valid phone",
    ]
    return any(term in normalized_prompt for term in keep_terms)


def _is_text_normalization_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    text_terms = [
        "trim leading and trailing whitespace",
        "leading and trailing whitespace",
        "standardize casing",
        "standardize case",
        "trim whitespace",
        "strip spaces",
        "whitespace inconsistencies",
        "casing inconsistencies",
        "repeated text values",
        "text normalization",
        "normalize text",
        "whitespace",
    ]
    return any(term in normalized_prompt for term in text_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_numeric_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    numeric_terms = [
        "numeric-like",
        "numeric",
        "numbers",
        "currency symbols",
        "separators",
        "parse cleanly as numbers",
        "formatting noise",
    ]
    return any(term in normalized_prompt for term in numeric_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _is_duplicate_only_prompt(user_prompt: str) -> bool:
    normalized_prompt = user_prompt.strip().lower()
    duplicate_terms = ["duplicate", "duplicates", "deduplicate", "dedup", "redundant copies"]
    return any(term in normalized_prompt for term in duplicate_terms) and not _has_semantic_value_cleaning_terms(
        normalized_prompt
    )


def _requires_ai_cleaning(user_prompt: str) -> bool:
    deterministic_only = (
        _is_duplicate_only_prompt(user_prompt)
        or _is_missing_value_only_prompt(user_prompt)
        or _is_date_only_prompt(user_prompt)
        or _is_age_only_prompt(user_prompt)
        or _is_type_validation_only_prompt(user_prompt)
        or _is_phone_only_prompt(user_prompt)
        or _is_text_normalization_only_prompt(user_prompt)
        or _is_numeric_only_prompt(user_prompt)
        or _is_header_only_prompt(user_prompt)
        or _is_header_type_only_prompt(user_prompt)
    )
    return not deterministic_only


def _normalize_text_for_match(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _extract_target_columns_from_prompt(columns: list[str], user_prompt: str) -> set[str]:
    normalized_prompt = f" {_normalize_text_for_match(user_prompt)} "
    target_columns: set[str] = set()
    for column in columns:
        column_label = str(column).strip()
        if not column_label:
            continue

        variants = {
            column_label.lower(),
            column_label.lower().replace("_", " "),
            _to_snake_case(column_label).replace("_", " "),
            _normalize_text_for_match(column_label),
        }
        if any(variant and f" {variant} " in normalized_prompt for variant in variants):
            target_columns.add(column_label)
    return target_columns


def plan_ai_cleaning(
    df: pd.DataFrame,
    user_prompt: str,
    *,
    total_rows: int | None = None,
) -> AICleaningPlan:
    columns = [str(column) for column in df.columns]
    target_columns = sorted(_extract_target_columns_from_prompt(columns, user_prompt))

    if not _requires_ai_cleaning(user_prompt):
        return AICleaningPlan(strategy="deterministic", target_columns=target_columns)

    candidate_columns = target_columns or columns
    safe_columns = select_llm_safe_columns(df, candidate_columns)
    if not safe_columns:
        return AICleaningPlan(
            strategy="privacy_blocked",
            target_columns=target_columns,
            reason=(
                "All candidate columns for AI cleaning are privacy-blocked. "
                "Use deterministic cleaning or choose non-sensitive columns."
            ),
        )

    is_large_dataset = total_rows is not None and total_rows >= settings.UAT_AI_ROW_LEVEL_LARGE_DATASET_THRESHOLD
    if target_columns and len(safe_columns) <= settings.UAT_AI_VALUE_CLEAN_MAX_COLUMNS:
        return AICleaningPlan(strategy="value_mapping_ai", target_columns=target_columns)

    if is_large_dataset:
        return AICleaningPlan(
            strategy="unsupported_large_ai",
            target_columns=target_columns,
            reason=(
                "For large datasets, AI cleaning prompts must mention target column names explicitly "
                f"and keep the scope to at most {settings.UAT_AI_VALUE_CLEAN_MAX_COLUMNS} non-sensitive columns."
            ),
        )

    return AICleaningPlan(strategy="row_level_ai", target_columns=target_columns)


def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df.columns = [_to_snake_case(str(column)) for column in normalized_df.columns]
    return normalized_df


def _is_semantic_text_candidate_column(column_name: str) -> bool:
    normalized_name = column_name.strip().lower()
    if not any(hint in normalized_name for hint in TEXT_SEMANTIC_COLUMN_HINTS):
        return False

    excluded_hints = EMAIL_COLUMN_HINTS + PHONE_COLUMN_HINTS + DATE_COLUMN_HINTS + NUMERIC_COLUMN_HINTS
    return not any(hint in normalized_name for hint in excluded_hints)


def _normalize_header_type_columns(
    df: pd.DataFrame,
    *,
    target_columns: set[str] | None = None,
) -> pd.DataFrame:
    normalized_df = df.copy()
    scoped_columns = set(target_columns or set())
    for column in normalized_df.columns:
        column_name = str(column)
        if scoped_columns and column_name not in scoped_columns:
            continue
        if not _is_semantic_text_candidate_column(column_name):
            continue

        series = normalized_df[column]
        if not (
            pd.api.types.is_object_dtype(series)
            or pd.api.types.is_string_dtype(series)
            or pd.api.types.is_numeric_dtype(series)
            or pd.api.types.is_bool_dtype(series)
        ):
            continue

        string_series = series.astype("string")
        trimmed_series = string_series.str.strip()
        non_empty_mask = series.notna() & trimmed_series.ne("")
        if not non_empty_mask.any():
            continue

        text_like_mask = trimmed_series.str.contains(r"[A-Za-z]", regex=True, na=False)
        updated_series = string_series.copy()
        valid_text_mask = non_empty_mask & text_like_mask
        invalid_text_mask = non_empty_mask & ~text_like_mask
        if valid_text_mask.any():
            updated_series.loc[valid_text_mask] = trimmed_series.loc[valid_text_mask]
        if invalid_text_mask.any():
            updated_series.loc[invalid_text_mask] = NULL_OUTPUT_TOKEN
        normalized_df[column] = updated_series

    return normalized_df


def _infer_expected_validation_type_for_cleaning(column_name: str, series: pd.Series) -> str:
    string_series = series.astype("string")
    non_empty_values = string_series[series.notna()].str.strip()
    non_empty_values = non_empty_values[non_empty_values.ne("")]
    if non_empty_values.empty:
        return "string"

    normalized_name = column_name.strip().lower()
    if any(hint in normalized_name for hint in EMAIL_COLUMN_HINTS) or any(
        hint in normalized_name for hint in PHONE_COLUMN_HINTS
    ):
        return "string"

    boolean_ratio = float(non_empty_values.map(_parse_boolean_value).notna().mean())
    integer_parser = _parse_age_value if _is_age_column_name(column_name) else _parse_strict_integer_value
    integer_ratio = float(non_empty_values.map(integer_parser).notna().mean())
    float_ratio = float(non_empty_values.map(_parse_float_value).notna().mean())
    date_ratio = float(_parse_datetime_series(non_empty_values).notna().mean())

    if _is_boolean_column_name(column_name) or boolean_ratio >= 0.6:
        return "boolean"
    if any(hint in normalized_name for hint in DATE_COLUMN_HINTS) or date_ratio >= 0.6:
        return "date"
    if _is_age_column_name(column_name) or _is_integer_column_name(column_name) or integer_ratio >= 0.6:
        return "integer"
    if _is_float_column_name(column_name) or any(hint in normalized_name for hint in NUMERIC_COLUMN_HINTS) or float_ratio >= 0.6:
        return "float"
    return "string"


def _apply_schema_type_validation(
    df: pd.DataFrame,
    *,
    target_columns: set[str] | None = None,
) -> pd.DataFrame:
    validated_df = df.copy()
    scoped_columns = set(target_columns or set())
    for column in validated_df.columns:
        column_name = str(column)
        if scoped_columns and column_name not in scoped_columns:
            continue

        series = validated_df[column]
        expected_type = _infer_expected_validation_type_for_cleaning(column_name, series)
        if expected_type == "string":
            string_series = series.astype("string")
            non_null_mask = series.notna()
            string_series.loc[non_null_mask] = string_series.loc[non_null_mask].str.strip()
            validated_df[column] = string_series
            continue

        if expected_type == "integer":
            integer_parser = _parse_age_value if _is_age_column_name(column_name) else _parse_strict_integer_value
            validated_df[column] = pd.Series([integer_parser(value) for value in series.tolist()], index=series.index, dtype="object")
            continue

        if expected_type == "float":
            validated_df[column] = pd.Series([_parse_float_value(value) for value in series.tolist()], index=series.index, dtype="object")
            continue

        if expected_type == "boolean":
            validated_df[column] = pd.Series([_parse_boolean_value(value) for value in series.tolist()], index=series.index, dtype="object")
            continue

        if expected_type == "date":
            string_series = series.astype("string")
            non_empty_mask = series.notna() & string_series.str.strip().ne("")
            parsed_dates = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
            if non_empty_mask.any():
                parsed_dates.loc[non_empty_mask] = _parse_datetime_series(string_series[non_empty_mask])
            validated_df[column] = parsed_dates

    return validated_df


def _prompt_requests_date_normalization(normalized_prompt: str) -> bool:
    subject_terms = ["date", "dates", "datetime", "timestamp", "date/time"]
    action_terms = ["format", "convert", "normalize", "standardize", "date strings", "date values", "invalid date", "invalid dates"]
    format_pattern = r"(%[a-zA-Z][-_/:%a-zA-Z]*)|(yyyy[-_/]mm[-_/]dd)|(dd[-_/]mm[-_/]yyyy)|(mm[-_/]dd[-_/]yyyy)"
    return any(term in normalized_prompt for term in subject_terms) and (
        any(term in normalized_prompt for term in action_terms) or bool(re.search(format_pattern, normalized_prompt))
    )


def _prompt_requests_age_normalization(normalized_prompt: str) -> bool:
    has_subject = bool(re.search(r"\bage\b|\bages\b|\byears?\s+old\b", normalized_prompt))
    action_terms = [
        "normalize",
        "standardize",
        "convert",
        "integer",
        "whole-number",
        "whole number",
        "numeric",
        "plausible",
        "negative",
        "invalid age",
        "out of range",
        "spelled-out",
        "replace",
    ]
    return has_subject and any(term in normalized_prompt for term in action_terms)


def _prompt_requests_type_validation(normalized_prompt: str) -> bool:
    subject_patterns = [
        r"\btype validation\b",
        r"\bvalidate column types?\b",
        r"\bschema validation\b",
        r"\bstrict mode\b",
        r"\bstring columns?\b",
        r"\binteger columns?\b",
        r"\bfloat columns?\b",
        r"\bboolean columns?\b",
        r"\bdate columns?\b",
    ]
    action_terms = [
        "convert everything to string",
        "keep only valid integers",
        "keep only valid float",
        "accept only",
        "do not auto-convert strings",
        "replace with nan",
        "replace invalid",
        "coerce",
        "nat",
    ]
    return any(re.search(pattern, normalized_prompt) for pattern in subject_patterns) or any(
        term in normalized_prompt for term in action_terms
    )


def _prompt_requests_header_type_normalization(normalized_prompt: str) -> bool:
    subject_terms = [
        "header semantics",
        "column headers as the type guide",
        "type guide",
        "column type",
        "column types",
        "data type",
        "data types",
        "schema",
        "string type",
        "str type",
        "text/string type",
    ]
    action_terms = [
        "normalize",
        "standardize",
        "convert",
        "enforce",
        "match",
        "consistency",
        "string",
        "text-like",
        "trimmed strings",
        "non-text",
        "wrong type",
        "only numbers",
    ]
    return any(term in normalized_prompt for term in subject_terms) and any(
        term in normalized_prompt for term in action_terms
    )


def _extract_requested_date_format(user_prompt: str) -> str | None:
    quoted_match = re.search(r"""['"](%[A-Za-z0-9_\-\/:% ]+)['"]""", user_prompt)
    if quoted_match:
        return quoted_match.group(1).strip()

    normalized_prompt = user_prompt.strip().lower()
    alias_map = {
        "yyyy-mm-dd": "%Y-%m-%d",
        "yyyy/mm/dd": "%Y/%m/%d",
        "dd-mm-yyyy": "%d-%m-%Y",
        "dd/mm/yyyy": "%d/%m/%Y",
        "mm-dd-yyyy": "%m-%d-%Y",
        "mm/dd/yyyy": "%m/%d/%Y",
    }
    for alias, strftime_format in alias_map.items():
        if alias in normalized_prompt:
            return strftime_format
    return None


def _extract_invalid_date_replacement(user_prompt: str) -> str | None:
    quoted_match = re.search(
        r"""replace\s+invalid\s+dates?\s+with\s+['"]([^'"]+)['"]""",
        user_prompt,
        flags=re.IGNORECASE,
    )
    if quoted_match:
        return quoted_match.group(1).strip()

    bare_match = re.search(
        r"""replace\s+invalid\s+dates?\s+with\s+([A-Za-z0-9_./%-]+)""",
        user_prompt,
        flags=re.IGNORECASE,
    )
    return bare_match.group(1).strip() if bare_match else None


def _extract_invalid_age_replacement(user_prompt: str) -> str | None:
    quoted_match = re.search(
        r"""replace\s+(?:invalid|negative|impossible|non-age)\s+ages?\s+with\s+['"]([^'"]+)['"]""",
        user_prompt,
        flags=re.IGNORECASE,
    )
    if quoted_match:
        return quoted_match.group(1).strip()

    bare_match = re.search(
        r"""replace\s+(?:invalid|negative|impossible|non-age)\s+ages?\s+with\s+([A-Za-z0-9_./%-]+)""",
        user_prompt,
        flags=re.IGNORECASE,
    )
    return bare_match.group(1).strip() if bare_match else None


def _normalize_date_columns(
    df: pd.DataFrame,
    *,
    user_prompt: str | None = None,
    target_columns: set[str] | None = None,
) -> pd.DataFrame:
    normalized_df = df.copy()
    requested_format = _extract_requested_date_format(user_prompt or "") or DATE_OUTPUT_FORMAT
    invalid_replacement = _extract_invalid_date_replacement(user_prompt or "")
    scoped_columns = set(target_columns or set())

    for column in normalized_df.columns:
        column_name = str(column)
        if scoped_columns and column_name not in scoped_columns:
            continue

        series = normalized_df[column]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue

        string_series = series.astype("string")
        non_empty_mask = series.notna() & string_series.str.strip().ne("")
        series_non_null = string_series[non_empty_mask]
        if series_non_null.empty:
            continue

        try:
            parsed = pd.to_datetime(series_non_null, errors="coerce", format="mixed", utc=False)
        except (TypeError, ValueError):
            parsed = pd.to_datetime(series_non_null, errors="coerce", utc=False)

        if not scoped_columns and parsed.notna().mean() < 0.5:
            continue
        if parsed.notna().sum() == 0:
            continue

        formatted = parsed.dt.strftime(requested_format)
        updated_series = series.copy()
        updated_series.loc[formatted.index] = formatted
        if invalid_replacement is not None:
            invalid_index = parsed.index[parsed.isna()]
            if len(invalid_index) > 0:
                updated_series.loc[invalid_index] = invalid_replacement
        normalized_df[column] = updated_series

    return normalized_df


def _normalize_age_columns(
    df: pd.DataFrame,
    *,
    user_prompt: str | None = None,
    target_columns: set[str] | None = None,
) -> pd.DataFrame:
    normalized_df = df.copy()
    invalid_replacement = _extract_invalid_age_replacement(user_prompt or "") or NULL_OUTPUT_TOKEN
    scoped_columns = set(target_columns or set())

    for column in normalized_df.columns:
        column_name = str(column)
        if scoped_columns and column_name not in scoped_columns:
            continue
        if not scoped_columns and not _is_age_column_name(column_name):
            continue

        series = normalized_df[column]
        if not (
            pd.api.types.is_object_dtype(series)
            or pd.api.types.is_string_dtype(series)
            or pd.api.types.is_numeric_dtype(series)
            or pd.api.types.is_bool_dtype(series)
        ):
            continue

        updated_values: list[Any] = []
        changed = False
        for value in series.tolist():
            if pd.isna(value):
                updated_values.append(value)
                continue

            text = str(value).strip()
            if not text:
                updated_values.append(value)
                continue

            parsed_age = _parse_age_value(value)
            if parsed_age is None:
                updated_values.append(invalid_replacement)
                changed = True
                continue

            normalized_age = str(parsed_age)
            updated_values.append(normalized_age)
            if normalized_age != text:
                changed = True

        if changed:
            normalized_df[column] = updated_values

    return normalized_df


def _is_phone_candidate_column(column_name: str, series: pd.Series) -> bool:
    normalized_name = column_name.strip().lower()
    if any(hint in normalized_name for hint in PHONE_COLUMN_HINTS):
        return True

    string_series = series.astype("string")
    non_null_values = string_series[series.notna()].str.strip()
    if non_null_values.empty:
        return False

    phone_digits = non_null_values.str.replace(r"\D+", "", regex=True)
    return float(phone_digits.str.fullmatch(PHONE_PATTERN, na=False).mean()) >= 0.6


def _normalize_phone_value(value: Any) -> tuple[Any, bool]:
    if pd.isna(value):
        return value, True

    text = str(value).strip()
    if not text:
        return value, True

    digits = re.sub(r"\D+", "", text)
    if not digits or not PHONE_PATTERN.fullmatch(digits):
        return value, False

    has_explicit_country_code = text.startswith("+") or len(digits) > 10
    return (f"+{digits}" if has_explicit_country_code else digits), True


def _normalize_phone_columns(df: pd.DataFrame, *, keep_only_valid_rows: bool = False) -> pd.DataFrame:
    normalized_df = df.copy()
    keep_mask = pd.Series(True, index=normalized_df.index)
    for column in normalized_df.columns:
        series = normalized_df[column]
        if not (
            pd.api.types.is_object_dtype(series)
            or pd.api.types.is_string_dtype(series)
            or pd.api.types.is_numeric_dtype(series)
        ):
            continue
        if not _is_phone_candidate_column(str(column), series):
            continue

        normalized_values: list[Any] = []
        valid_flags: list[bool] = []
        for value in series.tolist():
            normalized_value, is_valid = _normalize_phone_value(value)
            normalized_values.append(normalized_value)
            valid_flags.append(is_valid)

        normalized_df[column] = normalized_values
        if keep_only_valid_rows:
            non_empty_mask = series.notna() & series.astype("string").str.strip().ne("")
            valid_series = pd.Series(valid_flags, index=normalized_df.index)
            keep_mask &= (~non_empty_mask) | valid_series

    return normalized_df.loc[keep_mask].reset_index(drop=True) if keep_only_valid_rows else normalized_df


def _is_moderate_cardinality_text_column(series: pd.Series) -> bool:
    string_series = series.astype("string")
    non_null_values = string_series[series.notna()].str.strip()
    if non_null_values.empty:
        return False

    unique_count = int(non_null_values.nunique(dropna=True))
    unique_ratio = unique_count / max(len(non_null_values), 1)
    average_length = float(non_null_values.str.len().fillna(0).mean())
    return unique_count <= 200 and unique_ratio <= 0.5 and average_length <= 80


def _canonical_text_variant(values: pd.Series) -> str:
    trimmed_values = values.astype("string").dropna().str.strip()
    trimmed_values = trimmed_values[trimmed_values.ne("")]
    if trimmed_values.empty:
        return ""

    counts = trimmed_values.value_counts()
    return str(counts.index[0]) if not counts.empty else str(trimmed_values.iloc[0])


def _normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = _apply_text_cleaning(df, trim_whitespace=True)
    for column in normalized_df.columns:
        series = normalized_df[column]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            continue
        if not _is_moderate_cardinality_text_column(series):
            continue

        string_series = series.astype("string")
        trimmed_series = string_series.str.strip()
        normalized_keys = trimmed_series.str.lower()
        non_empty_mask = trimmed_series.notna() & trimmed_series.ne("")
        if not non_empty_mask.any():
            continue

        canonical_map: dict[str, str] = {}
        for key, group in trimmed_series[non_empty_mask].groupby(normalized_keys[non_empty_mask]):
            if key is pd.NA or key is None or str(key).strip() == "":
                continue
            canonical_map[str(key)] = _canonical_text_variant(group)

        normalized_df[column] = [
            canonical_map.get(str(key), value) if pd.notna(key) else value
            for value, key in zip(trimmed_series.tolist(), normalized_keys.tolist())
        ]

    return normalized_df


def _is_numeric_candidate_column(column_name: str, series: pd.Series) -> bool:
    normalized_name = column_name.strip().lower()
    if any(hint in normalized_name for hint in NUMERIC_COLUMN_HINTS):
        return True

    string_series = series.astype("string")
    non_null_values = string_series[series.notna()].str.strip()
    if non_null_values.empty:
        return False

    cleaned_values = non_null_values.str.replace(r"[,\s$₹€£%]", "", regex=True)
    parsed = pd.to_numeric(cleaned_values, errors="coerce")
    return float(parsed.notna().mean()) >= 0.6


def _format_numeric_value(value: float) -> str:
    if pd.isna(value):
        return ""
    return str(int(value)) if float(value).is_integer() else format(float(value), "f").rstrip("0").rstrip(".")


def _normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    for column in normalized_df.columns:
        series = normalized_df[column]
        if not (
            pd.api.types.is_object_dtype(series)
            or pd.api.types.is_string_dtype(series)
            or pd.api.types.is_numeric_dtype(series)
        ):
            continue
        if not _is_numeric_candidate_column(str(column), series):
            continue

        string_series = series.astype("string")
        non_empty_mask = series.notna() & string_series.str.strip().ne("")
        if not non_empty_mask.any():
            continue

        cleaned_values = string_series.str.replace(r"[,\s$₹€£%]", "", regex=True)
        parsed = pd.to_numeric(cleaned_values.where(non_empty_mask), errors="coerce")
        formatted = parsed.map(_format_numeric_value)

        updated_series = string_series.copy()
        valid_mask = non_empty_mask & parsed.notna()
        updated_series.loc[valid_mask] = formatted.loc[valid_mask]
        normalized_df[column] = updated_series

    return normalized_df


def _merge_cleaned_row(original_row: dict[str, Any], cleaned_row: Any, target_columns: set[str]) -> dict[str, Any]:
    if not isinstance(cleaned_row, dict):
        return dict(original_row)

    merged = dict(original_row)
    columns_to_apply = target_columns or set(original_row.keys())
    for column in columns_to_apply:
        if column in cleaned_row:
            merged[column] = cleaned_row[column]
    return merged


def _build_ai_view_row(original_row: dict[str, Any], ai_columns: list[str]) -> dict[str, Any]:
    return {column: original_row.get(column) for column in ai_columns} if ai_columns else dict(original_row)


def _compute_record_hashes(records: list[dict[str, Any]], *, columns: list[str]) -> list[int]:
    if not records:
        return []
    comparable_df = pd.DataFrame(records, columns=columns)
    comparable_df = comparable_df.astype(object).where(pd.notnull(comparable_df), NULL_OUTPUT_TOKEN)
    return pd.util.hash_pandas_object(comparable_df.astype(str), index=False).astype("uint64").tolist()


def _invoke_cleaning_batch(chain, batch_json: str, user_prompt: str, expected_rows: int) -> list[dict[str, Any]]:
    retry_prompts = [
        user_prompt,
        (
            f"{user_prompt}\n\n"
            f"Important: return only a JSON array with exactly {expected_rows} objects, keep all original keys, "
            'and if the instruction asks for a literal placeholder like NaN then output the string value "NaN".'
        ),
    ]
    last_error: Exception | None = None
    for prompt in retry_prompts:
        try:
            raw_response = chain.invoke({"batch_json": batch_json, "user_prompt": prompt})
            cleaned_batch = _normalize_cleaned_batch_payload(_extract_json_payload(raw_response))
            if not isinstance(cleaned_batch, list):
                raise ValueError(f"Expected a list of JSON objects, got {type(cleaned_batch)}")
            if len(cleaned_batch) != expected_rows:
                raise ValueError(f"Batch size mismatch. Expected {expected_rows}, got {len(cleaned_batch)}")
            return cleaned_batch
        except Exception as exc:
            last_error = exc
    raise last_error if last_error is not None else ValueError("Cleaning batch failed")


def _invoke_cleaning_batch_with_fallback(chain, batch_records: list[dict[str, Any]], user_prompt: str) -> list[dict[str, Any]]:
    expected_rows = len(batch_records)
    if expected_rows == 0:
        return []

    batch_json = json.dumps(batch_records, ensure_ascii=False, separators=(",", ":"))
    try:
        return _invoke_cleaning_batch(chain=chain, batch_json=batch_json, user_prompt=user_prompt, expected_rows=expected_rows)
    except Exception:
        if expected_rows == 1:
            return [dict(batch_records[0])]

        mid = expected_rows // 2
        left_records = batch_records[:mid]
        right_records = batch_records[mid:]
        try:
            left_cleaned = _invoke_cleaning_batch_with_fallback(chain, left_records, user_prompt)
        except Exception:
            left_cleaned = [dict(row) for row in left_records]
        try:
            right_cleaned = _invoke_cleaning_batch_with_fallback(chain, right_records, user_prompt)
        except Exception:
            right_cleaned = [dict(row) for row in right_records]
        return left_cleaned + right_cleaned


def _resolve_ai_columns_for_chunk(
    df: pd.DataFrame,
    user_prompt: str,
    plan: AICleaningPlan | None = None,
) -> tuple[set[str], list[str], list[str]]:
    target_columns = _extract_target_columns_from_prompt([str(column) for column in df.columns], user_prompt)
    if plan is not None and plan.target_columns:
        target_columns |= set(plan.target_columns)

    candidate_ai_columns = (
        [column for column in df.columns if str(column) in target_columns]
        if target_columns
        else [str(column) for column in df.columns]
    )
    requested_ai_columns = list(candidate_ai_columns)
    ai_columns = select_llm_safe_columns(df, candidate_ai_columns)
    return target_columns, requested_ai_columns, ai_columns


def _clean_ai_value_batch(*, column: str, batch_values: list[str], user_prompt: str, chain=None) -> list[tuple[str, Any]]:
    active_chain = chain or _build_cleaning_chain()
    batch_records = [{column: value} for value in batch_values]
    cleaned_batch = _invoke_cleaning_batch_with_fallback(chain=active_chain, batch_records=batch_records, user_prompt=user_prompt)

    resolved_pairs: list[tuple[str, Any]] = []
    for original_record, cleaned_record in zip(batch_records, cleaned_batch):
        original_value = original_record[column]
        mapped_value = cleaned_record.get(column, original_value) if isinstance(cleaned_record, dict) else original_value
        resolved_pairs.append((str(original_value), mapped_value))
    return resolved_pairs


def _clean_column_values_with_ai(
    df: pd.DataFrame,
    *,
    ai_columns: list[str],
    user_prompt: str,
    chain,
    ai_value_cache: dict[Any, Any] | None = None,
) -> pd.DataFrame:
    cleaned_df = df.copy()
    batch_size = max(1, settings.UAT_AI_VALUE_BATCH_SIZE)
    parallelism = max(1, settings.UAT_AI_VALUE_BATCH_PARALLELISM)

    for column in ai_columns:
        if column not in cleaned_df.columns:
            continue

        series = cleaned_df[column]
        string_series = series.astype("string")
        non_empty_values = string_series[series.notna()].str.strip()
        non_empty_values = non_empty_values[non_empty_values.ne("")]
        unique_values = non_empty_values.drop_duplicates().tolist()
        if not unique_values:
            continue

        unresolved_values = [
            value for value in unique_values if ai_value_cache is None or f"{column}::{value}" not in ai_value_cache
        ]
        if len(unresolved_values) > settings.UAT_AI_VALUE_CLEAN_MAX_UNIQUE_VALUES:
            raise ValueError(
                f"AI cleaning for column '{column}' is too large for a scoped value-mapping pass. "
                "Narrow the prompt further or use a deterministic cleaning instruction."
            )

        value_map: dict[str, Any] = {}
        if ai_value_cache is not None:
            for value in unique_values:
                cache_key = f"{column}::{value}"
                if cache_key in ai_value_cache:
                    value_map[value] = ai_value_cache[cache_key]

        unresolved_batches = [
            unresolved_values[start:start + batch_size]
            for start in range(0, len(unresolved_values), batch_size)
        ]
        use_parallel_batches = parallelism > 1 and len(unresolved_batches) > 1
        resolved_pairs: list[tuple[str, Any]] = []

        if use_parallel_batches:
            max_workers = min(parallelism, len(unresolved_batches))
            try:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(
                            _clean_ai_value_batch,
                            column=column,
                            batch_values=batch_values,
                            user_prompt=user_prompt,
                        )
                        for batch_values in unresolved_batches
                    ]
                    for future in as_completed(futures):
                        resolved_pairs.extend(future.result())
            except Exception as exc:
                logger.warning(
                    "Parallel AI value cleaning failed for column '%s'; falling back to sequential batches: %s",
                    column,
                    exc,
                )
                resolved_pairs = []
                use_parallel_batches = False

        if not use_parallel_batches:
            for batch_values in unresolved_batches:
                resolved_pairs.extend(
                    _clean_ai_value_batch(
                        column=column,
                        batch_values=batch_values,
                        user_prompt=user_prompt,
                        chain=chain,
                    )
                )

        for original_value, mapped_value in resolved_pairs:
            value_map[original_value] = mapped_value
            if ai_value_cache is not None:
                ai_value_cache[f"{column}::{original_value}"] = mapped_value

        cleaned_df[column] = [
            value_map.get(str(value).strip(), value) if pd.notna(value) and str(value).strip() else value
            for value in series.tolist()
        ]

    return cleaned_df


def clean_dataframe_chunk(
    df: pd.DataFrame,
    user_prompt: str,
    *,
    chain=None,
    plan: AICleaningPlan | None = None,
    seen_row_hashes: set[int] | None = None,
    ai_row_cache: dict[Any, Any] | None = None,
) -> pd.DataFrame:
    should_normalize_missing = _prompt_mentions_missing_values(user_prompt)
    should_trim_whitespace = _should_trim_whitespace(user_prompt)
    target_columns = _extract_target_columns_from_prompt([str(column) for column in df.columns], user_prompt)
    cleaned_df = _apply_text_cleaning(
        df,
        normalize_missing=should_normalize_missing,
        trim_whitespace=should_trim_whitespace,
    )

    if _is_type_validation_only_prompt(user_prompt):
        cleaned_df = _apply_schema_type_validation(cleaned_df, target_columns=target_columns)
    if _is_header_type_only_prompt(user_prompt):
        cleaned_df = _normalize_header_type_columns(cleaned_df, target_columns=target_columns)
    if _should_normalize_headers(user_prompt):
        cleaned_df = _normalize_headers(cleaned_df)
    if _is_date_only_prompt(user_prompt):
        cleaned_df = _normalize_date_columns(cleaned_df, user_prompt=user_prompt, target_columns=target_columns)
    if _is_age_only_prompt(user_prompt):
        cleaned_df = _normalize_age_columns(cleaned_df, user_prompt=user_prompt, target_columns=target_columns)
    if _is_phone_only_prompt(user_prompt):
        cleaned_df = _normalize_phone_columns(
            cleaned_df,
            keep_only_valid_rows=_should_keep_only_valid_phone_rows(user_prompt),
        )
    if _is_text_normalization_only_prompt(user_prompt):
        cleaned_df = _normalize_text_columns(cleaned_df)
    if _is_numeric_only_prompt(user_prompt):
        cleaned_df = _normalize_numeric_columns(cleaned_df)
    if prompt_removes_exact_duplicates(user_prompt):
        cleaned_df = (
            _remove_exact_duplicate_rows(cleaned_df)
            if seen_row_hashes is None
            else _remove_exact_duplicate_rows_across_chunks(cleaned_df, seen_row_hashes)
        )

    active_plan = plan or plan_ai_cleaning(cleaned_df, user_prompt, total_rows=len(cleaned_df))
    if cleaned_df.empty or not active_plan.requires_ai:
        return cleaned_df

    if active_plan.strategy == "unsupported_large_ai":
        raise ValueError(active_plan.reason or "Unsupported large-dataset AI cleaning prompt.")

    active_chain = chain or _build_cleaning_chain()
    batch_size = min(settings.UAT_AI_BATCH_SIZE, max(10, 1200 // max(len(cleaned_df.columns), 1)))
    target_columns, requested_ai_columns, ai_columns = _resolve_ai_columns_for_chunk(
        cleaned_df,
        user_prompt,
        active_plan,
    )
    if not ai_columns:
        if requested_ai_columns:
            logger.info(
                "Skipping raw LLM cleaning because all candidate columns were privacy-blocked: %s",
                requested_ai_columns,
            )
        return cleaned_df

    if active_plan.strategy == "value_mapping_ai":
        return _clean_column_values_with_ai(
            cleaned_df,
            ai_columns=ai_columns,
            user_prompt=user_prompt,
            chain=active_chain,
            ai_value_cache=ai_row_cache,
        )

    df_clean = cleaned_df.astype(object).where(pd.notnull(cleaned_df), None)
    records = df_clean.to_dict(orient="records")
    ai_records = [_build_ai_view_row(record, ai_columns) for record in records]
    row_hashes = _compute_record_hashes(ai_records, columns=ai_columns)
    resolved_fragments: list[dict[str, Any] | None] = [None] * len(records)
    positions_by_hash: dict[int, list[int]] = {}
    unique_records: list[dict[str, Any]] = []
    unique_hashes: list[int] = []

    for index, (record, row_hash) in enumerate(zip(ai_records, row_hashes)):
        if ai_row_cache is not None and row_hash in ai_row_cache:
            resolved_fragments[index] = dict(ai_row_cache[row_hash])
            continue

        positions = positions_by_hash.setdefault(row_hash, [])
        positions.append(index)
        if len(positions) == 1:
            unique_records.append(record)
            unique_hashes.append(row_hash)

    for start in range(0, len(unique_records), batch_size):
        batch = unique_records[start:start + batch_size]
        batch_hashes = unique_hashes[start:start + batch_size]
        try:
            cleaned_batch = _invoke_cleaning_batch_with_fallback(
                chain=active_chain,
                batch_records=batch,
                user_prompt=user_prompt,
            )
        except Exception as exc:
            logger.warning("AI cleaning batch failed; falling back to original rows: %s", exc)
            cleaned_batch = batch

        merged_batch = [
            _merge_cleaned_row(original_row=original_row, cleaned_row=cleaned_row, target_columns=target_columns)
            for original_row, cleaned_row in zip(batch, cleaned_batch)
        ]
        for row_hash, merged_row in zip(batch_hashes, merged_batch):
            if ai_row_cache is not None:
                ai_row_cache[row_hash] = dict(merged_row)
            for row_index in positions_by_hash.get(row_hash, []):
                resolved_fragments[row_index] = dict(merged_row)

    final_rows = [
        _merge_cleaned_row(
            original_row=records[index],
            cleaned_row=resolved_fragments[index] if resolved_fragments[index] is not None else ai_records[index],
            target_columns=target_columns,
        )
        for index in range(len(records))
    ]
    return pd.DataFrame(final_rows, columns=cleaned_df.columns)
