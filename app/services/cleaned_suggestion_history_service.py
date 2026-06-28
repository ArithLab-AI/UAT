from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.models.ai_cleaning_models import AICleaningJobDetail
from app.models.analysis_models import AnalysisSuggestion
from app.models.auth_models import User
from app.services.analysis_profile_service import DataSuggestion
from app.services.analysis_suggestion_match_service import (
    normalize_cleaning_prompt_type,
    split_matching_suggestions,
)


@dataclass(frozen=True)
class AppliedCleaningContext:
    source_suggestion_id: str | None
    prompt: str
    prompt_type: str | None
    target_columns: list[str]


@dataclass(frozen=True)
class AppliedCleaningResolution:
    context: AppliedCleaningContext
    resolved: bool
    match_count: int


def _normalize_target_columns(values: list[str] | None) -> list[str]:
    return [str(value).strip() for value in (values or []) if str(value).strip()]


def get_applied_cleaning_contexts(
    db: Session,
    *,
    current_user: User,
    detail: AICleaningJobDetail,
    steps_applied: list[dict] | None,
) -> list[AppliedCleaningContext]:
    raw_entries: list[dict] = []
    suggestion_ids: set[str] = set()

    for step in steps_applied or []:
        if not isinstance(step, dict):
            continue
        if step.get("status") not in {None, "applied"}:
            continue
        details = step.get("details")
        if not isinstance(details, dict):
            continue

        suggestion_id = str(details.get("source_suggestion_id") or "").strip() or None
        prompt = str(details.get("prompt") or "").strip()
        prompt_type = str(details.get("cleaning_prompt_type") or "").strip() or None
        target_columns = details.get("target_columns") if isinstance(details.get("target_columns"), list) else []
        if not suggestion_id and not prompt:
            continue

        raw_entries.append(
            {
                "suggestion_id": suggestion_id,
                "prompt": prompt,
                "prompt_type": prompt_type,
                "target_columns": _normalize_target_columns(target_columns),
            }
        )
        if suggestion_id:
            suggestion_ids.add(suggestion_id)

    if detail.source_suggestion_id or detail.prompt:
        fallback_suggestion_id = str(detail.source_suggestion_id or "").strip() or None
        raw_entries.append(
            {
                "suggestion_id": fallback_suggestion_id,
                "prompt": str(detail.prompt or "").strip(),
                "prompt_type": None,
                "target_columns": _normalize_target_columns(list(detail.target_columns or [])),
            }
        )
        if fallback_suggestion_id:
            suggestion_ids.add(fallback_suggestion_id)

    suggestion_metadata: dict[str, tuple[str | None, list[str], str | None, str | None]] = {}
    if suggestion_ids:
        records = (
            db.query(
                AnalysisSuggestion.id,
                AnalysisSuggestion.cleaning_prompt_type,
                AnalysisSuggestion.target_columns,
                AnalysisSuggestion.resolution_prompt,
                AnalysisSuggestion.issue_description,
            )
            .filter(
                AnalysisSuggestion.id.in_(sorted(suggestion_ids)),
                AnalysisSuggestion.created_by_user_id == current_user.id,
            )
            .all()
        )
        suggestion_metadata = {
            str(record[0]): (
                str(record[1]).strip() if record[1] else None,
                _normalize_target_columns(list(record[2] or [])),
                str(record[3]).strip() if record[3] else None,
                str(record[4]).strip() if record[4] else None,
            )
            for record in records
        }

    contexts: list[AppliedCleaningContext] = []
    seen_keys: set[tuple[str | None, str, str | None, tuple[str, ...]]] = set()
    for entry in raw_entries:
        suggestion_id = entry["suggestion_id"]
        prompt = entry["prompt"]
        prompt_type = entry["prompt_type"]
        target_columns = entry["target_columns"]
        metadata = suggestion_metadata.get(suggestion_id or "")
        if metadata is not None:
            stored_prompt_type, stored_target_columns, stored_prompt, stored_issue = metadata
            prompt = prompt or stored_prompt or ""
            prompt_type = normalize_cleaning_prompt_type(
                prompt_type or stored_prompt_type,
                resolution_prompt=prompt or stored_prompt,
                issue_description=stored_issue,
            )
            target_columns = target_columns or stored_target_columns
        else:
            prompt_type = normalize_cleaning_prompt_type(prompt_type, resolution_prompt=prompt)

        if not prompt:
            continue

        key = (
            suggestion_id,
            prompt,
            prompt_type,
            tuple(target_columns),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        contexts.append(
            AppliedCleaningContext(
                source_suggestion_id=suggestion_id,
                prompt=prompt,
                prompt_type=prompt_type,
                target_columns=target_columns,
            )
        )

    return contexts


def filter_resolved_suggestions_from_history(
    *,
    suggestions: list[DataSuggestion],
    verification_suggestions: list[DataSuggestion],
    contexts: list[AppliedCleaningContext],
) -> tuple[list[DataSuggestion], list[AppliedCleaningResolution]]:
    filtered_suggestions = list(suggestions)
    resolutions: list[AppliedCleaningResolution] = []

    for context in contexts:
        matching_verification, _ = split_matching_suggestions(
            cleaned_prompt=context.prompt,
            cleaned_prompt_type=context.prompt_type,
            cleaned_target_columns=context.target_columns,
            suggestions=verification_suggestions,
        )
        resolution = AppliedCleaningResolution(
            context=context,
            resolved=len(matching_verification) == 0,
            match_count=len(matching_verification),
        )
        resolutions.append(resolution)
        if resolution.resolved:
            _, filtered_suggestions = split_matching_suggestions(
                cleaned_prompt=context.prompt,
                cleaned_prompt_type=context.prompt_type,
                cleaned_target_columns=context.target_columns,
                suggestions=filtered_suggestions,
            )

    return filtered_suggestions, resolutions
