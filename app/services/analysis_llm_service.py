import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

from app.config.config import settings
from app.services.analysis_profile_service import (
    DATE_OUTPUT_FORMAT,
    NULL_OUTPUT_TOKEN,
    DataSuggestion,
)
from app.services.analysis_suggestion_match_service import (
    SUPPORTED_CLEANING_PROMPT_TYPES,
    normalize_cleaning_prompt_type,
)
from app.utils.openai_utils import get_openai_client

logger = logging.getLogger(__name__)

DEFAULT_ANALYSIS_SUGGESTION_LIMIT = 10
LARGE_ANALYSIS_SUGGESTION_LIMIT = 15
VERY_LARGE_ANALYSIS_SUGGESTION_LIMIT = 20
LARGE_ANALYSIS_ROW_THRESHOLD = 25_000
VERY_LARGE_ANALYSIS_ROW_THRESHOLD = 100_000
SUPPORTED_CLEANING_PROMPT_TYPE_LIST = ", ".join(sorted(SUPPORTED_CLEANING_PROMPT_TYPES))

ANALYSIS_SYSTEM_PROMPT_TEMPLATE = (
    "You are a strict data quality issue summarizer. "
    "Return JSON only with this exact schema: "
    '{{"suggestions":[{{"issue_description":"...","priority":"High|Medium|Low","resolution_prompt":"...","cleaning_prompt_type":"...","target_columns":["..."]}}]}}. '
    "Do not output markdown. Do not output prose. Do not compute quality_score. "
    "Use only the provided deterministic profile metrics. "
    "Return at most {max_suggestions} unique suggestions. "
    "Each resolution_prompt must be directly reusable as a cleaning instruction. "
    f"cleaning_prompt_type must be one of: {SUPPORTED_CLEANING_PROMPT_TYPE_LIST}. "
    "When the issue is localized, explicitly name the affected columns. "
    f"For date normalization prefer the format {DATE_OUTPUT_FORMAT}. "
    f"For missing-value normalization prefer the placeholder token '{NULL_OUTPUT_TOKEN}'."
)


@dataclass(frozen=True)
class AnalysisLLMConfig:
    provider: str
    model: str
    temperature: float
    max_tokens: int


def _analysis_suggestion_limit(profile: dict[str, Any]) -> int:
    row_count = max(0, int(profile.get("row_count_sampled", 0) or 0))
    if row_count >= VERY_LARGE_ANALYSIS_ROW_THRESHOLD:
        return VERY_LARGE_ANALYSIS_SUGGESTION_LIMIT
    if row_count >= LARGE_ANALYSIS_ROW_THRESHOLD:
        return LARGE_ANALYSIS_SUGGESTION_LIMIT
    return DEFAULT_ANALYSIS_SUGGESTION_LIMIT


def get_analysis_llm_config(
    provider_override: str | None = None,
    model_override: str | None = None,
) -> AnalysisLLMConfig:
    provider = (provider_override or settings.UAT_ANALYSIS_LLM_PROVIDER or "openai").strip().lower()
    if provider != "openai":
        raise ValueError("llm_provider must be 'openai'")

    model = (model_override or settings.UAT_ANALYSIS_LLM_MODEL or "gpt-4.1-mini").strip()
    if not model:
        raise ValueError("Set UAT_ANALYSIS_LLM_MODEL to a valid OpenAI model ID.")

    return AnalysisLLMConfig(
        provider=provider,
        model=model,
        temperature=float(settings.UAT_ANALYSIS_LLM_TEMPERATURE),
        max_tokens=int(settings.UAT_ANALYSIS_LLM_MAX_TOKENS),
    )


def _extract_json_payload(raw_response: str) -> Any:
    candidates = []
    stripped = raw_response.strip()
    if stripped:
        candidates.append(stripped)

    fenced_match = re.search(r"```json\s*(.*?)```", raw_response, re.DOTALL | re.IGNORECASE)
    if fenced_match:
        candidates.append(fenced_match.group(1).strip())

    object_match = re.search(r"(\{\s*.*\s*\})", raw_response, re.DOTALL)
    if object_match:
        candidates.append(object_match.group(1).strip())

    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    raise ValueError("Model response did not contain a valid JSON payload.")


def _normalize_suggestion(item: dict[str, Any]) -> DataSuggestion | None:
    issue_description = str(item.get("issue_description", "")).strip()
    priority = str(item.get("priority", "")).strip()
    resolution_prompt = str(item.get("resolution_prompt", "")).strip()
    if not issue_description or not priority or not resolution_prompt:
        return None

    target_columns = item.get("target_columns") or []
    if not isinstance(target_columns, list):
        target_columns = []

    cleaning_prompt_type = normalize_cleaning_prompt_type(
        item.get("cleaning_prompt_type"),
        resolution_prompt=resolution_prompt,
        issue_description=issue_description,
    )

    return DataSuggestion(
        issue_description=issue_description,
        priority=priority,
        resolution_prompt=resolution_prompt,
        cleaning_prompt_type=cleaning_prompt_type,
        target_columns=[str(column).strip() for column in target_columns if str(column).strip()],
    )


def _dedupe_suggestions(suggestions: list[DataSuggestion], *, max_suggestions: int) -> list[DataSuggestion]:
    unique: list[DataSuggestion] = []
    seen: set[tuple[str, str]] = set()

    for suggestion in suggestions:
        key = (
            re.sub(r"\s+", " ", suggestion.issue_description.strip().lower()),
            re.sub(r"\s+", " ", suggestion.resolution_prompt.strip().lower()),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(suggestion)
        if len(unique) >= max(1, max_suggestions):
            break
    return unique


def _build_profile_payload(profile: dict[str, Any], *, max_suggestions: int) -> dict[str, Any]:
    return {
        "row_count_sampled": profile.get("row_count_sampled", 0),
        "quality_score": profile.get("quality_score", 0),
        "max_suggestions": max_suggestions,
        "preferred_date_output_format": DATE_OUTPUT_FORMAT,
        "preferred_null_output_token": NULL_OUTPUT_TOKEN,
        "duplicate_row_percent": profile.get("duplicate_row_percent", 0.0),
        "columns": profile.get("columns", []),
        "dataset_issues": profile.get("dataset_issues", []),
        "sample_rows": profile.get("sample_rows", [])[:5],
    }


def _build_user_prompt(profile: dict[str, Any], *, max_suggestions: int) -> str:
    payload = _build_profile_payload(profile, max_suggestions=max_suggestions)
    return f"Profile JSON:\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"


def _call_openai(config: AnalysisLLMConfig, *, system_prompt: str, user_prompt: str) -> str:
    client = get_openai_client().bind(
        model=config.model,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
    )
    response = client.invoke(
        [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
    )

    raw_text = str(getattr(response, "content", "") or "").strip()
    if not raw_text:
        raise RuntimeError("OpenAI returned an empty response.")
    return raw_text


def generate_llm_suggestions(
    profile: dict[str, Any],
    *,
    provider_override: str | None = None,
    model_override: str | None = None,
) -> tuple[list[DataSuggestion], AnalysisLLMConfig]:
    config = get_analysis_llm_config(provider_override=provider_override, model_override=model_override)
    max_suggestions = _analysis_suggestion_limit(profile)
    system_prompt = ANALYSIS_SYSTEM_PROMPT_TEMPLATE.format(max_suggestions=max_suggestions)
    user_prompt = _build_user_prompt(profile, max_suggestions=max_suggestions)
    raw_response = _call_openai(config, system_prompt=system_prompt, user_prompt=user_prompt)

    payload = _extract_json_payload(raw_response)
    suggestions_payload = payload.get("suggestions")
    if not isinstance(suggestions_payload, list):
        raise ValueError("The model response must contain a 'suggestions' array.")

    suggestions = []
    for item in suggestions_payload:
        if not isinstance(item, dict):
            continue
        suggestion = _normalize_suggestion(item)
        if suggestion is not None:
            suggestions.append(suggestion)

    suggestions = _dedupe_suggestions(suggestions, max_suggestions=max_suggestions)
    if not suggestions:
        raise ValueError("The model response did not contain any usable suggestions.")
    return suggestions, config
