import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any, Iterable

import pandas as pd
from sqlalchemy.orm import Session

from app.db.database import Base, engine
from app.models.ai_cleaning_models import AICleaningJobDetail
from app.models.auth_models import User
from app.models.analysis_models import AnalysisSuggestion, DatasetAnalysis
from app.models.cleaning_models import CleaningJob
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.services.analysis_actionable_suggestion_service import filter_actionable_suggestions
from app.services.ai_cleaning_quality_service import (
    cap_quality_score_by_suggestions,
    coalesce_priority_clean_quality_score,
)
from app.services.analysis_suggestion_match_service import (
    classify_issue_category,
    normalize_cleaning_prompt_type,
    split_matching_suggestions,
)
from app.services.analysis_suggestion_title_service import build_suggestion_title
from app.services.analysis_llm_service import generate_llm_suggestions
from app.services.analysis_profile_service import (
    DataSuggestion,
    build_dataset_profile_from_chunks,
    build_date_imputation_suggestions,
    build_numeric_imputation_suggestions,
    compute_quality_score,
    generate_rule_based_suggestions,
)
from app.utils.object_storage import get_object_storage_service
from app.utils.openai_utils import format_openai_exception
from app.utils.responses import error_response

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetSource:
    dataset_id: int
    dataset_type: str
    is_clean: bool
    dataset_name: str
    file_name: str
    raw_file_name: str
    table_name: str
    storage_key: str
    ai_cleaning_job_id: str | None = None


@lru_cache(maxsize=1)
def _ensure_analysis_tables() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[DatasetAnalysis.__table__, AnalysisSuggestion.__table__],
    )


def _analysis_chunksize() -> int:
    raw_value = os.getenv("UAT_ANALYSIS_CHUNK_SIZE", "5000").strip()
    try:
        return max(500, int(raw_value))
    except ValueError:
        return 5000


def _parse_csv_iter(path: str, chunksize: int, *, preserve_placeholders: bool = False) -> Iterable[pd.DataFrame]:
    read_kwargs = {"dtype": object, "chunksize": chunksize}
    if preserve_placeholders:
        read_kwargs["keep_default_na"] = False

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            yield from pd.read_csv(path, encoding=encoding, **read_kwargs)
            return
        except UnicodeDecodeError:
            continue

    yield from pd.read_csv(path, **read_kwargs)


def _to_suggestion_dict(suggestion: DataSuggestion, *, suggestion_id: str | None = None) -> dict:
    return {
        "id": suggestion_id,
        "title": build_suggestion_title(
            cleaning_prompt_type=suggestion.cleaning_prompt_type,
            issue_description=suggestion.issue_description,
        ),
        "issue_description": suggestion.issue_description,
        "priority": suggestion.priority,
        "resolution_prompt": suggestion.resolution_prompt,
        "cleaning_prompt_type": suggestion.cleaning_prompt_type,
        "target_columns": suggestion.target_columns or [],
    }


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_column_name(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_suggestion_category_key(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    category_aliases = {
        "missing": "missing_values",
        "duplicate": "duplicate",
        "date": "date",
        "email": "email",
        "phone": "phone",
        "age": "age",
        "boolean": "boolean",
        "integer": "integer",
        "float": "float",
        "numeric": "numeric",
        "text": "text",
        "schema_type": "schema_type",
        "formatting": "formatting",
        "header_type_normalization": "schema_type",
        "missing_value_normalization": "missing_values",
        "duplicate_removal": "duplicate",
        "date_normalization": "date",
        "phone_normalization": "phone",
        "email_normalization": "email",
        "age_normalization": "age",
        "boolean_validation": "boolean",
        "integer_validation": "integer",
        "float_validation": "float",
        "numeric_normalization": "numeric",
        "text_normalization": "text",
    }
    return category_aliases.get(normalized, normalized or "other")


def _resolve_suggestion_category_key(suggestion_payload: dict) -> str:
    resolution_prompt = _optional_text(suggestion_payload.get("resolution_prompt"))
    issue_description = _optional_text(suggestion_payload.get("issue_description"))
    prompt_type = normalize_cleaning_prompt_type(
        _optional_text(suggestion_payload.get("cleaning_prompt_type")),
        resolution_prompt=resolution_prompt,
        issue_description=issue_description,
    )
    category = (
        classify_issue_category(resolution_prompt, prompt_type)
        or classify_issue_category(issue_description, prompt_type)
        or prompt_type
        or "other"
    )
    return _normalize_suggestion_category_key(category)


def _build_categorized_suggestions(suggestion_payloads: list[dict]) -> dict[str, list[dict]]:
    categorized_suggestions: dict[str, list[dict]] = {}

    for suggestion_payload in suggestion_payloads:
        category_key = _resolve_suggestion_category_key(suggestion_payload)
        categorized_suggestions.setdefault(category_key, []).append(suggestion_payload)

    return categorized_suggestions


# Categories that must never be surfaced as cleaning suggestions.
# email / phone: sensitive columns whose data must not be altered — only "missing"
#   detection is allowed on them (handled implicitly since we keep the missing category).
# date: date-format normalization is suppressed (date_imputation is kept).
_SUPPRESSED_SUGGESTION_CATEGORIES = {"email", "phone", "date"}


def _suggestion_issue_category(suggestion: DataSuggestion) -> str:
    resolution_prompt = _optional_text(suggestion.resolution_prompt)
    issue_description = _optional_text(suggestion.issue_description)
    prompt_type = normalize_cleaning_prompt_type(
        _optional_text(suggestion.cleaning_prompt_type),
        resolution_prompt=resolution_prompt,
        issue_description=issue_description,
    )
    return (
        classify_issue_category(resolution_prompt, prompt_type)
        or classify_issue_category(issue_description, prompt_type)
        or prompt_type
        or "other"
    )


def _apply_suggestion_output_policies(
    suggestions: list[DataSuggestion],
) -> list[DataSuggestion]:
    """Enforce suggestion visibility rules just before persistence/output.

    1. Drop ``email`` and ``phone`` suggestions so sensitive column data is never
       altered (missing-value suggestions on those columns are still allowed).
    2. Drop ``date`` format-normalization suggestions (``date_imputation`` is kept).
    3. Remove columns already covered by a numeric-imputation suggestion from any
       ``missing`` suggestion so the same numeric columns are not surfaced twice.
    4. Deduplicate within each category. Two suggestions of the same category that
       touch the same column are redundant (same kind of fix on the same data),
       even if their wording differs or their column groupings only overlap. Each
       column is kept by the first suggestion of that category; later suggestions
       lose the already-claimed columns and are dropped once nothing is left.
       Column-less category suggestions (e.g. duplicate-row removal) are kept once.
    """
    categories = [_suggestion_issue_category(suggestion) for suggestion in suggestions]

    numeric_imputation_columns = {
        _normalize_column_name(column)
        for suggestion, category in zip(suggestions, categories)
        if category == "numeric_imputation"
        for column in (suggestion.target_columns or [])
        if _normalize_column_name(column)
    }

    claimed_columns_by_category: dict[str, set[str]] = {}
    seen_columnless_categories: set[str] = set()

    filtered: list[DataSuggestion] = []
    for suggestion, category in zip(suggestions, categories):
        if category in _SUPPRESSED_SUGGESTION_CATEGORIES:
            continue

        original_columns = suggestion.target_columns or []
        working_columns = list(original_columns)

        # Cross-category rule: numeric-imputation columns must not reappear as
        # generic missing-value suggestions.
        if category == "missing" and numeric_imputation_columns:
            working_columns = [
                column
                for column in working_columns
                if _normalize_column_name(column) not in numeric_imputation_columns
            ]
            if original_columns and not working_columns:
                continue

        # Column-less suggestions (e.g. duplicate-row removal): keep one per category.
        if not original_columns:
            if category in seen_columnless_categories:
                continue
            seen_columnless_categories.add(category)
            filtered.append(suggestion)
            continue

        # Within-category dedup: drop columns already claimed by an earlier
        # suggestion of the same category (catches exact and overlapping duplicates).
        claimed_columns = claimed_columns_by_category.setdefault(category, set())
        remaining_columns = [
            column
            for column in working_columns
            if _normalize_column_name(column) not in claimed_columns
        ]
        if not remaining_columns:
            continue
        claimed_columns.update(_normalize_column_name(column) for column in remaining_columns)

        if remaining_columns != original_columns:
            suggestion = replace(suggestion, target_columns=remaining_columns)
        filtered.append(suggestion)

    return filtered


@contextmanager
def _download_dataset_file(storage_key: str, dataset_key: str):
    storage = get_object_storage_service()
    fd, temp_path = tempfile.mkstemp(prefix=f"{dataset_key}_", suffix=".csv")
    os.close(fd)
    try:
        restored = storage.download_file(storage_key, temp_path)
        if not restored:
            raise error_response(
                status_code=404,
                detail=f"Dataset data for '{dataset_key}' was not found in object storage",
            )
        yield temp_path
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _resolve_dataset_source(db: Session, current_user: User, dataset_type: str, dataset_id: int) -> DatasetSource:
    if dataset_type == "uploaded":
        dataset = (
            db.query(CsvUploadedDataset)
            .filter(
                CsvUploadedDataset.id == dataset_id,
                CsvUploadedDataset.created_by_user_id == current_user.id,
            )
            .first()
        )
        if dataset is None:
            raise error_response(status_code=404, detail="Uploaded dataset not found")
        return DatasetSource(
            dataset_id=dataset.id,
            dataset_type="uploaded",
            is_clean=False,
            dataset_name=dataset.name,
            file_name=dataset.file_name,
            raw_file_name=dataset.file_name,
            table_name=dataset.table_name,
            storage_key=dataset.storage_key,
        )

    dataset = (
        db.query(CsvMergedDataset)
        .filter(
            CsvMergedDataset.id == dataset_id,
            CsvMergedDataset.created_by_user_id == current_user.id,
        )
        .first()
    )
    if dataset is None:
        raise error_response(status_code=404, detail="Merged dataset not found")
    return DatasetSource(
        dataset_id=dataset.id,
        dataset_type="merged",
        is_clean=False,
        dataset_name=dataset.name,
        file_name=f"{dataset.name}.csv",
        raw_file_name=f"{dataset.name}.csv",
        table_name=dataset.table_name,
        storage_key=dataset.storage_key,
    )


def _resolve_clean_dataset_source(
    db: Session,
    current_user: User,
    dataset_type: str,
    dataset_id: int,
) -> DatasetSource:
    raw_source = _resolve_dataset_source(db, current_user, dataset_type, dataset_id)
    cleaned_record = (
        db.query(AICleaningJobDetail, CleaningJob)
        .join(CleaningJob, CleaningJob.id == AICleaningJobDetail.job_id)
        .filter(
            AICleaningJobDetail.created_by_user_id == current_user.id,
            AICleaningJobDetail.source_dataset_id == dataset_id,
            AICleaningJobDetail.source_dataset_type == dataset_type,
            AICleaningJobDetail.cleaned_storage_key.isnot(None),
            CleaningJob.ai_cleaning_type.is_(True),
        )
        .order_by(AICleaningJobDetail.updated_at.desc())
        .first()
    )
    if cleaned_record is None:
        raise error_response(
            status_code=404,
            detail="AI cleaned data not found for the provided dataset. Run /ai-cleaning first.",
        )

    detail, job = cleaned_record
    cleaned_file_name = job.output_filename or detail.source_file_name or f"{detail.job_id}_cleaned.csv"
    return DatasetSource(
        dataset_id=raw_source.dataset_id,
        dataset_type=raw_source.dataset_type,
        is_clean=True,
        dataset_name=raw_source.dataset_name,
        file_name=cleaned_file_name,
        raw_file_name=raw_source.file_name,
        table_name=f"{raw_source.table_name}_clean",
        storage_key=detail.cleaned_storage_key,
        ai_cleaning_job_id=detail.job_id,
    )


def _resolve_analysis_source(
    db: Session,
    current_user: User,
    *,
    dataset_type: str,
    dataset_id: int,
    is_clean: bool,
) -> DatasetSource:
    if is_clean:
        return _resolve_clean_dataset_source(db, current_user, dataset_type, dataset_id)
    return _resolve_dataset_source(db, current_user, dataset_type, dataset_id)


def _build_profile_for_dataset(source: DatasetSource) -> dict:
    chunksize = _analysis_chunksize()
    with _download_dataset_file(source.storage_key, source.table_name) as file_path:
        chunks = _parse_csv_iter(file_path, chunksize=chunksize, preserve_placeholders=source.is_clean)
        return build_dataset_profile_from_chunks(chunks)


def _get_latest_raw_analysis_score(db: Session, current_user: User, source: DatasetSource) -> int | None:
    record = (
        db.query(DatasetAnalysis.quality_score)
        .filter(
            DatasetAnalysis.created_by_user_id == current_user.id,
            DatasetAnalysis.source_dataset_id == source.dataset_id,
            DatasetAnalysis.source_type == source.dataset_type,
            DatasetAnalysis.file_name == source.raw_file_name,
        )
        .order_by(DatasetAnalysis.updated_at.desc(), DatasetAnalysis.created_at.desc())
        .first()
    )
    return int(record[0]) if record is not None and record[0] is not None else None


def _resolve_ai_cleaning_detail_priority(
    db: Session,
    current_user: User,
    detail: AICleaningJobDetail,
) -> str | None:
    if detail.source_suggestion_priority:
        return detail.source_suggestion_priority

    if not detail.source_suggestion_id:
        return None

    suggestion = (
        db.query(AnalysisSuggestion.priority)
        .filter(
            AnalysisSuggestion.id == detail.source_suggestion_id,
            AnalysisSuggestion.created_by_user_id == current_user.id,
        )
        .first()
    )
    if suggestion is not None and suggestion[0]:
        return str(suggestion[0])

    for item in ((detail.analysis or {}).get("suggestions") or []):
        if not isinstance(item, dict) or item.get("id") != detail.source_suggestion_id:
            continue
        priority = str(item.get("priority") or "").strip()
        if priority:
            return priority

    return None


def _resolve_ai_cleaning_detail_prompt_metadata(
    db: Session,
    current_user: User,
    detail: AICleaningJobDetail,
) -> tuple[str | None, list[str]]:
    if detail.source_suggestion_id:
        suggestion = (
            db.query(
                AnalysisSuggestion.cleaning_prompt_type,
                AnalysisSuggestion.target_columns,
                AnalysisSuggestion.resolution_prompt,
                AnalysisSuggestion.issue_description,
            )
            .filter(
                AnalysisSuggestion.id == detail.source_suggestion_id,
                AnalysisSuggestion.created_by_user_id == current_user.id,
            )
            .first()
        )
        if suggestion is not None:
            prompt_type = normalize_cleaning_prompt_type(
                suggestion[0],
                resolution_prompt=str(suggestion[2]).strip() if suggestion[2] else None,
                issue_description=str(suggestion[3]).strip() if suggestion[3] else None,
            )
            target_columns = list(suggestion[1] or [])
            return prompt_type, [str(column).strip() for column in target_columns if str(column).strip()]

    for item in ((detail.analysis or {}).get("suggestions") or []):
        if not isinstance(item, dict) or item.get("id") != detail.source_suggestion_id:
            continue
        prompt_type = normalize_cleaning_prompt_type(
            item.get("cleaning_prompt_type"),
            resolution_prompt=str(item.get("resolution_prompt") or "").strip() or None,
            issue_description=str(item.get("issue_description") or "").strip() or None,
        )
        target_columns = item.get("target_columns") if isinstance(item.get("target_columns"), list) else []
        return prompt_type, [str(column).strip() for column in target_columns if str(column).strip()]

    return None, list(detail.target_columns or [])


def _persist_analysis_result(
    db: Session,
    *,
    current_user: User,
    source: DatasetSource,
    quality_score: int,
    llm_used: bool,
    llm_provider: str | None,
    llm_model: str | None,
    suggestion_source: str,
    message: str,
    dataset_profile: dict,
    suggestions: list[DataSuggestion],
) -> tuple[DatasetAnalysis, list[AnalysisSuggestion]]:
    _ensure_analysis_tables()

    analysis = DatasetAnalysis(
        source_dataset_id=source.dataset_id,
        source_type=source.dataset_type,
        created_by_user_id=current_user.id,
        dataset_name=source.dataset_name,
        file_name=source.file_name,
        quality_score=quality_score,
        llm_used=llm_used,
        llm_provider=llm_provider,
        llm_model=llm_model,
        suggestion_source=suggestion_source,
        message=message,
        dataset_profile=dataset_profile,
    )
    db.add(analysis)
    db.flush()

    suggestion_rows: list[AnalysisSuggestion] = []
    for suggestion in suggestions:
        suggestion_row = AnalysisSuggestion(
            analysis_id=analysis.id,
            source_dataset_id=source.dataset_id,
            source_type=source.dataset_type,
            created_by_user_id=current_user.id,
            issue_description=suggestion.issue_description,
            priority=suggestion.priority,
            resolution_prompt=suggestion.resolution_prompt,
            cleaning_prompt_type=suggestion.cleaning_prompt_type,
            target_columns=suggestion.target_columns or [],
        )
        db.add(suggestion_row)
        suggestion_rows.append(suggestion_row)

    db.commit()
    db.refresh(analysis)
    for suggestion_row in suggestion_rows:
        db.refresh(suggestion_row)
    return analysis, suggestion_rows


_IMPUTATION_PROMPT_TYPES = {"numeric_missing_imputation", "date_missing_imputation"}


def _merge_imputation_suggestions(
    suggestions: list[DataSuggestion],
    profile: dict[str, Any],
) -> list[DataSuggestion]:
    """Always surface deterministic imputation suggestions: numeric mean/median fill
    and date/datetime most-frequent (mode) fill.

    These are appended regardless of whether suggestions came from the LLM or the
    rule-based generator, so the fill options are offered consistently. Suggestions
    already covering the same columns with the same fill type are skipped to avoid
    duplicates.
    """
    imputation_suggestions = build_numeric_imputation_suggestions(profile) + build_date_imputation_suggestions(profile)
    if not imputation_suggestions:
        return suggestions

    covered_keys = {
        (suggestion.cleaning_prompt_type or "", frozenset(suggestion.target_columns or []))
        for suggestion in suggestions
        if (suggestion.cleaning_prompt_type or "") in _IMPUTATION_PROMPT_TYPES
    }
    merged = list(suggestions)
    for suggestion in imputation_suggestions:
        key = (suggestion.cleaning_prompt_type or "", frozenset(suggestion.target_columns or []))
        if key in covered_keys:
            continue
        covered_keys.add(key)
        merged.append(suggestion)
    return merged


def run_dataset_analysis(
    db: Session,
    *,
    current_user: User,
    dataset_id: int,
    dataset_type: str,
    is_clean: bool,
    use_llm: bool,
    llm_provider: str | None = None,
    llm_model: str | None = None,
) -> dict:
    _ensure_analysis_tables()
    source = _resolve_analysis_source(
        db,
        current_user,
        dataset_type=dataset_type,
        dataset_id=dataset_id,
        is_clean=is_clean,
    )
    dataset_profile = _build_profile_for_dataset(source)
    base_quality_score = compute_quality_score(dataset_profile)
    quality_score = base_quality_score
    clean_detail: AICleaningJobDetail | None = None
    raw_quality_score: int | None = None
    verification_rule_suggestions: list[DataSuggestion] | None = None
    if source.is_clean and source.ai_cleaning_job_id:
        clean_detail = (
            db.query(AICleaningJobDetail)
            .filter(
                AICleaningJobDetail.job_id == source.ai_cleaning_job_id,
                AICleaningJobDetail.created_by_user_id == current_user.id,
            )
            .first()
        )
        if clean_detail is not None:
            raw_quality_score = _get_latest_raw_analysis_score(db, current_user, source)
    enriched_profile = {**dataset_profile, "quality_score": base_quality_score}
    if source.is_clean:
        verification_rule_suggestions = generate_rule_based_suggestions(enriched_profile)

    suggestions: list[DataSuggestion]
    llm_used = False
    suggestion_source = "rule_based"
    resolved_provider: str | None = None
    resolved_model: str | None = None

    def _finalize_quality_score(current_suggestions: list[DataSuggestion]) -> int:
        suggestion_aligned_score = cap_quality_score_by_suggestions(
            base_quality_score,
            suggestions=current_suggestions,
        )
        if source.is_clean and clean_detail is not None:
            return coalesce_priority_clean_quality_score(
                suggestion_aligned_score,
                raw_score=raw_quality_score,
                previous_clean_score=clean_detail.quality_score,
                changes_detected=bool(clean_detail.changes_detected),
                suggestion_priority=_resolve_ai_cleaning_detail_priority(db, current_user, clean_detail),
            )
        return suggestion_aligned_score

    def _build_clean_verification(current_suggestions: list[DataSuggestion]) -> tuple[str | None, bool | None, int | None, int | None]:
        source_suggestion_id = clean_detail.source_suggestion_id if clean_detail is not None else None
        source_suggestion_resolved: bool | None = None
        source_suggestion_match_count: int | None = None
        if source.is_clean and clean_detail is not None and clean_detail.prompt:
            source_prompt_type, source_target_columns = _resolve_ai_cleaning_detail_prompt_metadata(
                db,
                current_user,
                clean_detail,
            )
            verification_suggestions = verification_rule_suggestions or current_suggestions
            matching_suggestions, _ = split_matching_suggestions(
                cleaned_prompt=clean_detail.prompt,
                cleaned_prompt_type=source_prompt_type,
                cleaned_target_columns=source_target_columns,
                suggestions=verification_suggestions,
            )
            source_suggestion_match_count = len(matching_suggestions)
            source_suggestion_resolved = source_suggestion_match_count == 0

        quality_score_delta = (
            quality_score - raw_quality_score
            if source.is_clean and raw_quality_score is not None
            else None
        )
        return (
            source_suggestion_id,
            source_suggestion_resolved,
            source_suggestion_match_count,
            quality_score_delta,
        )

    if use_llm:
        try:
            suggestions, llm_config = generate_llm_suggestions(
                enriched_profile,
                provider_override=llm_provider,
                model_override=llm_model,
            )
            llm_used = True
            suggestion_source = "llm"
            resolved_provider = llm_config.provider
            resolved_model = llm_config.model
            filtered_suggestions = filter_actionable_suggestions(dataset_profile, suggestions)
            if filtered_suggestions:
                suggestions = filtered_suggestions
            else:
                suggestions = generate_rule_based_suggestions(enriched_profile)
                suggestion_source = "rule_based"
        except Exception as exc:
            failure_detail = format_openai_exception(exc)
            logger.exception(
                "Dataset analysis LLM suggestion generation failed in run_dataset_analysis -> "
                "generate_llm_suggestions for dataset_id=%s dataset_type=%s user_id=%s "
                "provider=%s model=%s detail=%s",
                source.dataset_id,
                source.dataset_type,
                current_user.id,
                llm_provider or "default",
                llm_model or "default",
                failure_detail,
            )
            suggestions = generate_rule_based_suggestions(enriched_profile)
            suggestion_source = "rule_based"
            resolved_provider = llm_provider
            resolved_model = llm_model
            message = (
                "LLM suggestion generation failed in run_dataset_analysis/generate_llm_suggestions "
                f"and rule-based suggestions were used instead: {failure_detail}"
            )
            (
                source_suggestion_id,
                source_suggestion_resolved,
                source_suggestion_match_count,
                quality_score_delta,
            ) = _build_clean_verification(suggestions)
            quality_score = _finalize_quality_score(suggestions)
            quality_score_delta = (
                quality_score - raw_quality_score
                if source.is_clean and raw_quality_score is not None
                else None
            )
            if clean_detail is not None:
                clean_detail.quality_score = quality_score
            if source.is_clean and source_suggestion_resolved is not None:
                verification_note = (
                    "Selected cleaned suggestion is no longer detected in cleaned analysis."
                    if source_suggestion_resolved
                    else f"Selected cleaned suggestion still has {source_suggestion_match_count} matching issue(s) in cleaned analysis."
                )
                message = f"{message} {verification_note}"
            suggestions = _apply_suggestion_output_policies(suggestions)
            analysis, suggestion_rows = _persist_analysis_result(
                db,
                current_user=current_user,
                source=source,
                quality_score=quality_score,
                llm_used=False,
                llm_provider=resolved_provider,
                llm_model=resolved_model,
                suggestion_source=suggestion_source,
                message=message,
                dataset_profile=dataset_profile,
                suggestions=suggestions,
            )
            suggestion_payloads = [
                _to_suggestion_dict(suggestion, suggestion_id=suggestion_row.id)
                for suggestion, suggestion_row in zip(suggestions, suggestion_rows)
            ]
            return {
                "analysis_id": analysis.id,
                "dataset_id": source.dataset_id,
                "dataset_type": source.dataset_type,
                "is_clean": source.is_clean,
                "dataset_name": source.dataset_name,
                "file_name": source.file_name,
                "quality_score": quality_score,
                "llm_used": False,
                "suggestion_source": suggestion_source,
                "llm_provider": resolved_provider,
                "llm_model": resolved_model,
                "message": message,
                "source_suggestion_id": source_suggestion_id,
                "source_suggestion_resolved": source_suggestion_resolved,
                "source_suggestion_match_count": source_suggestion_match_count,
                "quality_score_delta": quality_score_delta,
                "suggestions": _build_categorized_suggestions(suggestion_payloads),
                "dataset_profile": dataset_profile,
            }
    else:
        suggestions = generate_rule_based_suggestions(enriched_profile)

    suggestions = filter_actionable_suggestions(dataset_profile, suggestions)
    suggestions = _merge_imputation_suggestions(suggestions, enriched_profile)

    (
        source_suggestion_id,
        source_suggestion_resolved,
        source_suggestion_match_count,
        quality_score_delta,
    ) = _build_clean_verification(suggestions)
    if source.is_clean and source_suggestion_resolved:
        source_prompt_type, source_target_columns = _resolve_ai_cleaning_detail_prompt_metadata(
            db,
            current_user,
            clean_detail,
        )
        _, suggestions = split_matching_suggestions(
            cleaned_prompt=clean_detail.prompt if clean_detail is not None else None,
            cleaned_prompt_type=source_prompt_type,
            cleaned_target_columns=source_target_columns,
            suggestions=suggestions,
        )
    quality_score = _finalize_quality_score(suggestions)
    quality_score_delta = (
        quality_score - raw_quality_score
        if source.is_clean and raw_quality_score is not None
        else None
    )
    if clean_detail is not None:
        clean_detail.quality_score = quality_score
    message = (
        "Analysis completed successfully."
        if llm_used
        else "Analysis completed successfully using rule-based suggestions."
    )
    if source.is_clean and source_suggestion_resolved is not None:
        verification_note = (
            "Selected cleaned suggestion is no longer detected in cleaned analysis."
            if source_suggestion_resolved
            else f"Selected cleaned suggestion still has {source_suggestion_match_count} matching issue(s) in cleaned analysis."
        )
        message = f"{message} {verification_note}"
    suggestions = _apply_suggestion_output_policies(suggestions)
    analysis, suggestion_rows = _persist_analysis_result(
        db,
        current_user=current_user,
        source=source,
        quality_score=quality_score,
        llm_used=llm_used,
        llm_provider=resolved_provider,
        llm_model=resolved_model,
        suggestion_source=suggestion_source,
        message=message,
        dataset_profile=dataset_profile,
        suggestions=suggestions,
    )
    suggestion_payloads = [
        _to_suggestion_dict(suggestion, suggestion_id=suggestion_row.id)
        for suggestion, suggestion_row in zip(suggestions, suggestion_rows)
    ]
    return {
        "analysis_id": analysis.id,
        "dataset_id": source.dataset_id,
        "dataset_type": source.dataset_type,
        "is_clean": source.is_clean,
        "dataset_name": source.dataset_name,
        "file_name": source.file_name,
        "quality_score": quality_score,
        "llm_used": llm_used,
        "suggestion_source": suggestion_source,
        "llm_provider": resolved_provider,
        "llm_model": resolved_model,
        "message": message,
        "source_suggestion_id": source_suggestion_id,
        "source_suggestion_resolved": source_suggestion_resolved,
        "source_suggestion_match_count": source_suggestion_match_count,
        "quality_score_delta": quality_score_delta,
        "suggestions": _build_categorized_suggestions(suggestion_payloads),
        "dataset_profile": dataset_profile,
    }


def get_dataset_analysis_suggestions(
    db: Session,
    *,
    current_user: User,
    analysis_id: str,
) -> dict:
    analysis = (
        db.query(DatasetAnalysis)
        .filter(
            DatasetAnalysis.id == analysis_id,
            DatasetAnalysis.created_by_user_id == current_user.id,
        )
        .first()
    )
    if analysis is None:
        raise error_response(status_code=404, detail="Dataset analysis not found")

    suggestion_rows = (
        db.query(AnalysisSuggestion)
        .filter(
            AnalysisSuggestion.analysis_id == analysis.id,
            AnalysisSuggestion.created_by_user_id == current_user.id,
        )
        .order_by(AnalysisSuggestion.created_at.asc(), AnalysisSuggestion.id.asc())
        .all()
    )
    suggestion_payloads = [
        {
            "id": suggestion.id,
            "title": build_suggestion_title(
                cleaning_prompt_type=suggestion.cleaning_prompt_type,
                issue_description=suggestion.issue_description,
            ),
            "issue_description": suggestion.issue_description,
            "priority": suggestion.priority,
            "resolution_prompt": suggestion.resolution_prompt,
            "cleaning_prompt_type": suggestion.cleaning_prompt_type,
            "target_columns": suggestion.target_columns or [],
        }
        for suggestion in suggestion_rows
    ]

    return {
        "analysis_id": analysis.id,
        "dataset_id": analysis.source_dataset_id,
        "dataset_type": analysis.source_type,
        "dataset_name": analysis.dataset_name,
        "file_name": analysis.file_name,
        "quality_score": analysis.quality_score,
        "llm_used": analysis.llm_used,
        "suggestion_source": analysis.suggestion_source,
        "llm_provider": analysis.llm_provider,
        "llm_model": analysis.llm_model,
        "message": analysis.message,
        "suggestions": _build_categorized_suggestions(suggestion_payloads),
    }


def get_latest_dataset_analysis_suggestions(
    db: Session,
    *,
    current_user: User,
    dataset_id: int,
    dataset_type: str,
    is_clean: bool,
) -> dict:
    source = _resolve_analysis_source(
        db,
        current_user,
        dataset_type=dataset_type,
        dataset_id=dataset_id,
        is_clean=is_clean,
    )
    analysis = (
        db.query(DatasetAnalysis)
        .filter(
            DatasetAnalysis.created_by_user_id == current_user.id,
            DatasetAnalysis.source_dataset_id == source.dataset_id,
            DatasetAnalysis.source_type == source.dataset_type,
            DatasetAnalysis.file_name == source.file_name,
        )
        .order_by(DatasetAnalysis.updated_at.desc(), DatasetAnalysis.created_at.desc())
        .first()
    )
    if analysis is None:
        raise error_response(status_code=404, detail="Dataset analysis not found for the provided dataset")

    return get_dataset_analysis_suggestions(
        db,
        current_user=current_user,
        analysis_id=analysis.id,
    )
