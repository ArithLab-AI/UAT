import logging
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

import pandas as pd
from sqlalchemy.orm import Session

from app.db.database import Base, engine
from app.models.ai_cleaning_models import AICleaningJobDetail
from app.models.auth_models import User
from app.models.analysis_models import AnalysisSuggestion, DatasetAnalysis
from app.models.cleaning_models import CleaningJob
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.services.analysis_llm_service import generate_llm_suggestions
from app.services.analysis_profile_service import (
    DataSuggestion,
    build_dataset_profile_from_chunks,
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
    table_name: str
    storage_key: str


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


def _parse_csv_iter(path: str, chunksize: int) -> Iterable[pd.DataFrame]:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            yield from pd.read_csv(path, dtype=object, chunksize=chunksize, encoding=encoding)
            return
        except UnicodeDecodeError:
            continue

    yield from pd.read_csv(path, dtype=object, chunksize=chunksize)


def _to_suggestion_dict(suggestion: DataSuggestion, *, suggestion_id: str | None = None) -> dict:
    return {
        "id": suggestion_id,
        "issue_description": suggestion.issue_description,
        "priority": suggestion.priority,
        "resolution_prompt": suggestion.resolution_prompt,
        "cleaning_prompt_type": suggestion.cleaning_prompt_type,
        "target_columns": suggestion.target_columns or [],
    }


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
        table_name=f"{raw_source.table_name}_clean",
        storage_key=detail.cleaned_storage_key,
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
        chunks = _parse_csv_iter(file_path, chunksize=chunksize)
        return build_dataset_profile_from_chunks(chunks)


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
    quality_score = compute_quality_score(dataset_profile)
    enriched_profile = {**dataset_profile, "quality_score": quality_score}

    suggestions: list[DataSuggestion]
    llm_used = False
    suggestion_source = "rule_based"
    resolved_provider: str | None = None
    resolved_model: str | None = None

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
                "suggestions": [
                    _to_suggestion_dict(suggestion, suggestion_id=suggestion_row.id)
                    for suggestion, suggestion_row in zip(suggestions, suggestion_rows)
                ],
                "dataset_profile": dataset_profile,
            }
    else:
        suggestions = generate_rule_based_suggestions(enriched_profile)

    message = (
        "Analysis completed successfully."
        if llm_used
        else "Analysis completed successfully using rule-based suggestions."
    )
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
        "suggestions": [
            _to_suggestion_dict(suggestion, suggestion_id=suggestion_row.id)
            for suggestion, suggestion_row in zip(suggestions, suggestion_rows)
        ],
        "dataset_profile": dataset_profile,
    }
