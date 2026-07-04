import csv
import os
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.config.config import settings
from app.db.database import Base, SessionLocal, engine
from app.models.ai_cleaning_models import AICleaningJobDetail
from app.models.analysis_models import AnalysisSuggestion, DatasetAnalysis
from app.models.auth_models import User
from app.models.cleaning_models import CleaningJob
from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.services.analysis_actionable_suggestion_service import filter_actionable_suggestions
from app.services.ai_cleaning_prompt_service import (
    AICleaningPlan,
    accumulate_mode_imputation_stats,
    accumulate_numeric_imputation_stats,
    build_cleaning_chain,
    clean_dataframe_chunk,
    cleaning_strategy_uses_llm,
    finalize_mode_imputation_values,
    finalize_numeric_imputation_values,
    plan_ai_cleaning,
    numeric_imputation_strategy,
    prompt_has_deterministic_cleaning_steps,
    prompt_removes_exact_duplicates,
    prompt_requests_mode_imputation,
    prompt_requests_numeric_imputation,
)
from app.services.ai_cleaning_quality_service import (
    cap_quality_score_by_suggestions,
    coalesce_priority_clean_quality_score,
)
from app.services.analysis_llm_service import generate_llm_suggestions
from app.services.analysis_profile_service import (
    DataSuggestion,
    build_dataset_profile_from_chunks,
    compute_quality_score,
    generate_rule_based_suggestions,
)
from app.services.analysis_suggestion_match_service import (
    normalize_cleaning_prompt_type,
    split_matching_suggestions,
)
from app.services.analysis_suggestion_title_service import build_suggestion_title
from app.utils.object_storage import get_object_storage_service
from app.utils.responses import error_response


@dataclass(frozen=True)
class ResolvedAICleaningSource:
    dataset_id: int
    dataset_type: str
    dataset_name: str
    file_name: str
    storage_key: str
    file_url: str
    source_type: str
    source_ai_job_id: str | None = None


@lru_cache(maxsize=1)
def _ensure_ai_cleaning_tables() -> None:
    Base.metadata.create_all(bind=engine, tables=[AICleaningJobDetail.__table__])


def _ai_cleaned_output_key(job_id: str) -> str:
    return f"ai_cleaning/{job_id}/cleaned.csv"


def _ai_cleaned_download_url(job_id: str) -> str:
    return f"/ai-cleaning/{job_id}/download"


def _cleaning_chunksize() -> int:
    return max(100, int(settings.CHUNK_SIZE))


def _detect_csv_options(file_path: str) -> dict[str, str]:
    with open(file_path, "rb") as raw_file:
        raw_sample = raw_file.read(4096)

    encoding = "utf-8-sig"
    try:
        sample = raw_sample.decode(encoding)
    except UnicodeDecodeError:
        encoding = "latin1"
        sample = raw_sample.decode(encoding, errors="ignore")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ","

    return {"encoding": encoding, "delimiter": delimiter}


def _read_csv(file_path: str, encoding: str, delimiter: str, **kwargs):
    base_kwargs = {"sep": delimiter, "encoding": encoding, **kwargs}
    for candidate_encoding in (encoding, "latin1", "utf-8-sig"):
        try:
            return pd.read_csv(file_path, **{**base_kwargs, "encoding": candidate_encoding})
        except UnicodeDecodeError:
            continue
    return pd.read_csv(file_path, sep=delimiter, **kwargs)


def _parse_csv_iter(path: str, chunksize: int, *, preserve_placeholders: bool = False) -> Iterable[pd.DataFrame]:
    csv_options = _detect_csv_options(path)
    read_kwargs = {"dtype": object, "chunksize": chunksize}
    if preserve_placeholders:
        read_kwargs["keep_default_na"] = False
    yield from _read_csv(
        path,
        csv_options["encoding"],
        csv_options["delimiter"],
        **read_kwargs,
    )


def _read_csv_sample(path: str, *, nrows: int, preserve_placeholders: bool = False) -> pd.DataFrame:
    csv_options = _detect_csv_options(path)
    read_kwargs = {"dtype": object, "nrows": nrows}
    if preserve_placeholders:
        read_kwargs["keep_default_na"] = False
    return _read_csv(
        path,
        csv_options["encoding"],
        csv_options["delimiter"],
        **read_kwargs,
    )


def _count_csv_rows(path: str, *, limit: int | None = None) -> int:
    csv_options = _detect_csv_options(path)
    row_count = 0
    with open(path, "r", encoding=csv_options["encoding"], newline="") as csv_file:
        reader = csv.reader(csv_file, delimiter=csv_options["delimiter"])
        next(reader, None)
        for _ in reader:
            row_count += 1
            if limit is not None and row_count >= limit:
                break
    return row_count


@contextmanager
def _download_storage_key(storage_key: str, prefix: str):
    fd, file_path = tempfile.mkstemp(prefix=f"{prefix}_", suffix=".csv")
    os.close(fd)
    try:
        restored = get_object_storage_service().download_file(storage_key, file_path)
        if not restored:
            raise error_response(
                status_code=404,
                detail=f"Dataset data for storage key '{storage_key}' was not found in object storage",
            )
        yield file_path
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def _resolve_raw_source(db: Session, current_user: User, dataset_id: int, dataset_type: str) -> ResolvedAICleaningSource:
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
        return ResolvedAICleaningSource(
            dataset_id=dataset.id,
            dataset_type="uploaded",
            dataset_name=dataset.name,
            file_name=dataset.file_name,
            storage_key=dataset.storage_key,
            file_url=dataset.file_url,
            source_type="raw",
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
    return ResolvedAICleaningSource(
        dataset_id=dataset.id,
        dataset_type="merged",
        dataset_name=dataset.name,
        file_name=f"{dataset.name}.csv",
        storage_key=dataset.storage_key,
        file_url=dataset.file_url,
        source_type="raw",
    )


def _resolve_ai_source(
    db: Session,
    current_user: User,
    *,
    dataset_id: int,
    dataset_type: str,
    source_ai_job_id: str | None,
) -> ResolvedAICleaningSource:
    if not source_ai_job_id:
        return _resolve_raw_source(db, current_user, dataset_id, dataset_type)

    detail = (
        db.query(AICleaningJobDetail)
        .filter(
            AICleaningJobDetail.job_id == source_ai_job_id,
            AICleaningJobDetail.created_by_user_id == current_user.id,
        )
        .first()
    )
    if detail is None:
        raise error_response(status_code=404, detail="Source AI cleaning job not found")
    if detail.source_dataset_id != dataset_id or detail.source_dataset_type != dataset_type:
        raise error_response(
            status_code=400,
            detail="source_ai_job_id does not belong to the provided dataset_id and dataset_type",
        )
    if not detail.cleaned_storage_key:
        raise error_response(status_code=409, detail="Source AI cleaning output is not available")

    return ResolvedAICleaningSource(
        dataset_id=detail.source_dataset_id,
        dataset_type=detail.source_dataset_type,
        dataset_name=detail.source_dataset_name or "",
        file_name=detail.source_file_name or f"{detail.job_id}_cleaned.csv",
        storage_key=detail.cleaned_storage_key,
        file_url=detail.cleaned_file_path or "",
        source_type="clean",
        source_ai_job_id=source_ai_job_id,
    )


def _get_reusable_ai_job(
    db: Session,
    current_user: User,
    *,
    dataset_id: int,
    dataset_type: str,
    source_ai_job_id: str | None,
) -> tuple[CleaningJob, AICleaningJobDetail] | None:
    # Each AI cleaning run is an independent job by default: a fresh job_id cleaned
    # from the raw source. A previous job is reused ONLY when the client explicitly
    # asks to continue one via source_ai_job_id. This avoids a second suggestion
    # silently reusing (and resetting) the previous suggestion's job.
    if not source_ai_job_id:
        return None

    record = (
        db.query(CleaningJob, AICleaningJobDetail)
        .join(AICleaningJobDetail, AICleaningJobDetail.job_id == CleaningJob.id)
        .filter(
            CleaningJob.ai_cleaning_type.is_(True),
            AICleaningJobDetail.created_by_user_id == current_user.id,
            AICleaningJobDetail.source_dataset_id == dataset_id,
            AICleaningJobDetail.source_dataset_type == dataset_type,
            AICleaningJobDetail.job_id == source_ai_job_id,
        )
        .first()
    )
    if record is None:
        raise error_response(status_code=404, detail="Source AI cleaning job not found")
    return record


def _resolve_ai_cleaning_prompt(
    db: Session,
    current_user: User,
    *,
    dataset_id: int,
    dataset_type: str,
    suggestion_id: str,
    source_job_detail: AICleaningJobDetail | None,
) -> tuple[str, str, str | None, str | None, list[str]]:
    suggestion = (
        db.query(AnalysisSuggestion)
        .filter(
            AnalysisSuggestion.id == suggestion_id,
            AnalysisSuggestion.created_by_user_id == current_user.id,
        )
        .first()
    )
    if suggestion is not None:
        if suggestion.source_dataset_id != dataset_id or suggestion.source_type != dataset_type:
            raise error_response(
                status_code=400,
                detail="suggestion_id does not belong to the provided dataset_id and dataset_type",
            )
        return (
            suggestion.resolution_prompt,
            suggestion.id,
            suggestion.priority,
            normalize_cleaning_prompt_type(
                suggestion.cleaning_prompt_type,
                resolution_prompt=suggestion.resolution_prompt,
                issue_description=suggestion.issue_description,
            ),
            list(suggestion.target_columns or []),
        )

    if source_job_detail is not None:
        if source_job_detail.source_dataset_id != dataset_id or source_job_detail.source_dataset_type != dataset_type:
            raise error_response(
                status_code=400,
                detail="suggestion_id does not belong to the provided dataset_id and dataset_type",
            )
        for item in ((source_job_detail.analysis or {}).get("suggestions") or []):
            if not isinstance(item, dict) or item.get("id") != suggestion_id:
                continue
            resolution_prompt = str(item.get("resolution_prompt") or "").strip()
            if resolution_prompt:
                priority = str(item.get("priority") or "").strip() or None
                cleaning_prompt_type = normalize_cleaning_prompt_type(
                    item.get("cleaning_prompt_type"),
                    resolution_prompt=resolution_prompt,
                    issue_description=str(item.get("issue_description") or "").strip() or None,
                )
                target_columns = item.get("target_columns") if isinstance(item.get("target_columns"), list) else []
                return resolution_prompt, suggestion_id, priority, cleaning_prompt_type, [
                    str(column).strip() for column in target_columns if str(column).strip()
                ]

    raise error_response(status_code=404, detail="Analysis suggestion not found")


def _get_latest_raw_analysis_score(
    db: Session,
    current_user: User,
    *,
    dataset_id: int,
    dataset_type: str,
) -> int | None:
    try:
        raw_source = _resolve_raw_source(db, current_user, dataset_id, dataset_type)
    except HTTPException:
        return None

    record = (
        db.query(DatasetAnalysis.quality_score)
        .filter(
            DatasetAnalysis.created_by_user_id == current_user.id,
            DatasetAnalysis.source_dataset_id == dataset_id,
            DatasetAnalysis.source_type == dataset_type,
            DatasetAnalysis.file_name == raw_source.file_name,
        )
        .order_by(DatasetAnalysis.updated_at.desc(), DatasetAnalysis.created_at.desc())
        .first()
    )
    return int(record[0]) if record is not None and record[0] is not None else None


def _resolve_source_suggestion_priority(
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


def _resolve_source_suggestion_prompt_metadata(
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


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return _sanitize_json_value(value.item())
    return value


def _dataframe_to_json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    cleaned_df = df.astype(object).where(pd.notnull(df), None)
    return _sanitize_json_value(cleaned_df.to_dict(orient="records"))


def _dataframes_match(left_df: pd.DataFrame, right_df: pd.DataFrame) -> bool:
    if list(left_df.columns) != list(right_df.columns):
        return False
    if len(left_df.index) != len(right_df.index):
        return False

    left_values = left_df.astype(object).where(pd.notnull(left_df), "").astype(str)
    right_values = right_df.astype(object).where(pd.notnull(right_df), "").astype(str)
    return left_values.equals(right_values)


def _build_clean_message(
    *,
    source_type: str,
    strategy: str,
    target_columns: list[str],
    cleaned_rows: int,
    preview_rows_returned: int,
    preview_limited: bool,
    changes_detected: bool,
) -> str:
    source_label = "original dataset" if source_type == "raw" else "previous AI-cleaned output"
    target_label = ", ".join(target_columns) if target_columns else "all columns"
    preview_note = (
        f"API response includes the first {preview_rows_returned} row(s) only"
        if preview_limited
        else f"API response includes {preview_rows_returned} row(s)"
    )
    status_note = "Changes were detected." if changes_detected else "Prompt was processed, but no data changes were detected."
    return (
        f"{status_note} Source: {source_label}. Strategy: {strategy}. "
        f"Target columns: {target_label}. {preview_note} out of {cleaned_rows} cleaned row(s)."
    )


def _build_cleaning_result(
    source_path: str,
    *,
    job_id: str,
    prompt: str,
    source_type: str,
    source_file_name: str,
    prompt_type_hint: str | None = None,
    target_columns_hint: list[str] | None = None,
) -> dict[str, Any]:
    preview_rows: list[dict[str, Any]] = []
    input_total_rows = 0
    total_rows = 0
    wrote_header = False
    changes_detected = False
    rows_before = 0
    rows_after = 0
    columns_before = 0
    columns_after = 0
    preserve_placeholders = source_type == "clean"

    sample_rows = max(1, int(settings.UAT_AI_PLANNER_SAMPLE_ROWS))
    planner_sample_df = _read_csv_sample(source_path, nrows=sample_rows, preserve_placeholders=preserve_placeholders)
    cleaning_plan = plan_ai_cleaning(
        planner_sample_df,
        prompt,
        prompt_type_hint=prompt_type_hint,
        target_columns_hint=target_columns_hint,
    )
    if cleaning_plan.strategy == "row_level_ai":
        row_limit = settings.UAT_AI_ROW_LEVEL_LARGE_DATASET_THRESHOLD + 1
        total_rows_hint = _count_csv_rows(source_path, limit=row_limit)
        cleaning_plan = plan_ai_cleaning(
            planner_sample_df,
            prompt,
            total_rows=total_rows_hint,
            prompt_type_hint=prompt_type_hint,
            target_columns_hint=target_columns_hint,
        )
        if cleaning_plan.strategy == "unsupported_large_ai":
            raise error_response(status_code=400, detail=cleaning_plan.reason or "Unsupported large-dataset AI cleaning prompt.")
    if cleaning_plan.strategy == "privacy_blocked":
        if prompt_has_deterministic_cleaning_steps(prompt):
            cleaning_plan = AICleaningPlan(
                strategy="deterministic_privacy_fallback",
                target_columns=cleaning_plan.target_columns,
                reason=cleaning_plan.reason,
            )
        else:
            raise error_response(status_code=400, detail=cleaning_plan.reason or "AI cleaning is blocked for the requested columns.")

    requires_ai_cleaning = cleaning_plan.requires_ai
    chain = build_cleaning_chain() if requires_ai_cleaning else None
    ai_row_cache = {} if requires_ai_cleaning else None
    seen_row_hashes = set() if prompt_removes_exact_duplicates(prompt) else None
    effective_chunksize = settings.CHUNK_SIZE if requires_ai_cleaning else max(settings.CHUNK_SIZE, settings.CHUNK_SIZE * 10)
    output_filename = f"ai_cleaned_{Path(source_file_name).stem}.csv"

    # Mean/median imputation needs whole-column statistics, but cleaning runs
    # chunk-by-chunk. Pre-pass the file once to compute each numeric column's fill
    # value, then hand it to every chunk so missing cells are filled consistently.
    imputation_values: dict[str, Any] | None = None
    normalized_prompt_type = normalize_cleaning_prompt_type(prompt_type_hint) if prompt_type_hint else None
    wants_numeric_imputation = (
        prompt_requests_numeric_imputation(prompt) or normalized_prompt_type == "numeric_missing_imputation"
    )
    wants_mode_imputation = (
        prompt_requests_mode_imputation(prompt) or normalized_prompt_type == "date_missing_imputation"
    )
    if wants_numeric_imputation or wants_mode_imputation:
        imputation_targets = set(cleaning_plan.target_columns or [])
        numeric_accumulator: dict[str, Any] = {}
        mode_accumulator: dict[str, Any] = {}
        for impute_chunk_df in _parse_csv_iter(
            source_path,
            chunksize=effective_chunksize,
            preserve_placeholders=preserve_placeholders,
        ):
            if wants_numeric_imputation:
                accumulate_numeric_imputation_stats(impute_chunk_df, imputation_targets, numeric_accumulator)
            if wants_mode_imputation:
                accumulate_mode_imputation_stats(impute_chunk_df, imputation_targets, mode_accumulator)
        imputation_values = {}
        if wants_numeric_imputation:
            imputation_values.update(
                finalize_numeric_imputation_values(
                    numeric_accumulator, strategy=numeric_imputation_strategy(prompt)
                )
            )
        if wants_mode_imputation:
            # Numeric mean/median wins if a column somehow qualifies for both.
            for column_name, fill_value in finalize_mode_imputation_values(mode_accumulator).items():
                imputation_values.setdefault(column_name, fill_value)

    fd, output_path = tempfile.mkstemp(prefix=f"{job_id}_cleaned_", suffix=".csv")
    os.close(fd)

    try:
        with open(output_path, "w", encoding="utf-8", newline="") as output_file:
            for chunk_df in _parse_csv_iter(
                source_path,
                chunksize=effective_chunksize,
                preserve_placeholders=preserve_placeholders,
            ):
                if input_total_rows == 0:
                    columns_before = len(chunk_df.columns)
                input_total_rows += len(chunk_df)
                cleaned_chunk = clean_dataframe_chunk(
                    chunk_df,
                    prompt,
                    chain=chain,
                    plan=cleaning_plan,
                    prompt_type_hint=prompt_type_hint,
                    target_columns_hint=target_columns_hint,
                    seen_row_hashes=seen_row_hashes,
                    ai_row_cache=ai_row_cache,
                    imputation_values=imputation_values,
                )
                if columns_after == 0:
                    columns_after = len(cleaned_chunk.columns)
                if not changes_detected and not _dataframes_match(chunk_df, cleaned_chunk):
                    changes_detected = True

                preview_remaining = int(settings.UAT_AI_CLEAN_PREVIEW_ROWS) - len(preview_rows)
                if preview_remaining > 0 and not cleaned_chunk.empty:
                    preview_rows.extend(_dataframe_to_json_records(cleaned_chunk.head(preview_remaining)))

                cleaned_chunk.to_csv(output_file, header=not wrote_header, index=False)
                wrote_header = True
                total_rows += len(cleaned_chunk)

            rows_before = input_total_rows
            rows_after = total_rows

            if not wrote_header:
                empty_df = clean_dataframe_chunk(
                    _read_csv_sample(source_path, nrows=0, preserve_placeholders=preserve_placeholders),
                    prompt,
                    chain=chain,
                    plan=cleaning_plan,
                    prompt_type_hint=prompt_type_hint,
                    target_columns_hint=target_columns_hint,
                    seen_row_hashes=seen_row_hashes,
                    ai_row_cache=ai_row_cache,
                    imputation_values=imputation_values,
                )
                empty_df.to_csv(output_file, header=True, index=False)
                columns_before = len(empty_df.columns)
                columns_after = len(empty_df.columns)

        cleaned_storage_key = _ai_cleaned_output_key(job_id)
        cleaned_file_path = get_object_storage_service().upload_file(output_path, cleaned_storage_key)
        preview_rows_returned = len(preview_rows)
        preview_limited = total_rows > preview_rows_returned
        llm_used = cleaning_strategy_uses_llm(cleaning_plan.strategy)
        message = _build_clean_message(
            source_type=source_type,
            strategy=cleaning_plan.strategy,
            target_columns=cleaning_plan.target_columns,
            cleaned_rows=total_rows,
            preview_rows_returned=preview_rows_returned,
            preview_limited=preview_limited,
            changes_detected=changes_detected,
        )

        return {
            "source_type": source_type,
            "cleaning_strategy": cleaning_plan.strategy,
            "llm_used": llm_used,
            "target_columns": cleaning_plan.target_columns,
            "cleaned_storage_key": cleaned_storage_key,
            "cleaned_file_path": cleaned_file_path,
            "cleaned_data": preview_rows,
            "cleaned_rows": total_rows,
            "preview_rows_returned": preview_rows_returned,
            "preview_limited": preview_limited,
            "changes_detected": changes_detected,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "columns_before": columns_before,
            "columns_after": columns_after,
            "output_filename": output_filename,
            "message": message,
            "steps_applied": [
                {
                    "step": "ai_prompt_cleaning",
                    "status": "applied",
                    "details": {
                        "cleaning_strategy": cleaning_plan.strategy,
                        "target_columns": cleaning_plan.target_columns,
                        "llm_used": llm_used,
                    },
                }
            ],
            "cleaning_summary": {
                "total_steps": 1,
                "applied": 1,
                "skipped": 0,
                "errors": 0,
                "changes_detected": changes_detected,
            },
        }
    finally:
        if os.path.exists(output_path):
            os.remove(output_path)


def _resolve_cleaned_file_url(job: CleaningJob, detail: AICleaningJobDetail) -> str | None:
    """Presigned, directly-downloadable HTTPS URL for the cleaned file.

    Mirrors the ``/csv-datasets`` ``clean_file_url`` behaviour: sign the cleaned S3
    key so the client can download straight from the bucket. Returns ``None`` while
    the job is still processing (no cleaned file exists yet) so the field is omitted
    from the response; falls back to the internal download route only if a cleaned
    file exists but storage signing is unavailable."""
    storage_ref = detail.cleaned_storage_key or detail.cleaned_file_path or job.s3_cleaned_url
    if not storage_ref:
        return None
    presigned = get_object_storage_service().presigned_download_url(storage_ref)
    if presigned:
        return presigned
    return _ai_cleaned_download_url(job.id)


def _serialize_ai_cleaning_detail(job: CleaningJob, detail: AICleaningJobDetail) -> dict[str, Any]:
    return {
        "job_id": job.id,
        "source_dataset_id": detail.source_dataset_id,
        "source_dataset_type": detail.source_dataset_type,
        "source_suggestion_id": detail.source_suggestion_id,
        "source_ai_job_id": detail.source_ai_job_id,
        "ai_cleaning_type": bool(job.ai_cleaning_type),
        "status": job.status,
        "prompt": detail.prompt,
        "source_type": detail.source_type or "raw",
        "cleaning_strategy": detail.cleaning_strategy,
        "llm_used": bool(detail.llm_used),
        "target_columns": detail.target_columns or [],
        "cleaned_file_url": _resolve_cleaned_file_url(job, detail),
        "message": detail.message or job.error_message,
        "cleaned_rows": detail.cleaned_rows or 0,
        "preview_rows_returned": detail.preview_rows_returned or 0,
        "preview_limited": bool(detail.preview_limited),
        "changes_detected": bool(detail.changes_detected),
        "cleaned_data": detail.cleaned_data or [],
        "source_dataset_name": detail.source_dataset_name,
        "source_file_name": detail.source_file_name,
        "cleaned_file_path": detail.cleaned_file_path,
        "quality_score": detail.quality_score,
        "analysis": detail.analysis,
        "created_at": detail.created_at.isoformat() if detail.created_at else None,
        "updated_at": detail.updated_at.isoformat() if detail.updated_at else None,
    }


def run_ai_cleaning(
    db: Session,
    *,
    current_user: User,
    dataset_id: int,
    dataset_type: str,
    suggestion_id: str,
    source_ai_job_id: str | None = None,
    custom_prompt: str | None = None,
) -> dict[str, Any]:
    _ensure_ai_cleaning_tables()
    reusable_job_record = _get_reusable_ai_job(
        db,
        current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_ai_job_id=source_ai_job_id,
    )
    reusable_job: CleaningJob | None = reusable_job_record[0] if reusable_job_record is not None else None
    reusable_detail: AICleaningJobDetail | None = reusable_job_record[1] if reusable_job_record is not None else None
    source = _resolve_ai_source(
        db,
        current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_ai_job_id=reusable_detail.job_id if reusable_detail is not None else source_ai_job_id,
    )
    (
        resolved_prompt,
        resolved_suggestion_id,
        resolved_suggestion_priority,
        resolved_cleaning_prompt_type,
        resolved_target_columns,
    ) = _resolve_ai_cleaning_prompt(
        db,
        current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        suggestion_id=suggestion_id,
        source_job_detail=reusable_detail,
    )

    if custom_prompt:
        resolved_prompt = custom_prompt

    if reusable_job is None or reusable_detail is None:
        job_id = str(uuid.uuid4())
        job = CleaningJob(
            id=job_id,
            original_filename=source.file_name,
            file_type="csv",
            file_size_bytes=0,
            source_dataset_id=source.dataset_id,
            s3_upload_url=source.file_url,
            download_url=_ai_cleaned_download_url(job_id),
            status="processing",
            progress_pct=5,
            current_step=1,
            total_steps=3,
            current_step_name="Preparing AI cleaning",
            ai_cleaning_type=True,
        )
        detail = AICleaningJobDetail(
            job_id=job_id,
            created_by_user_id=current_user.id,
            source_dataset_id=source.dataset_id,
            source_dataset_type=source.dataset_type,
            source_dataset_name=source.dataset_name,
            source_file_name=source.file_name,
            source_storage_key=source.storage_key,
            source_file_url=source.file_url,
            source_suggestion_id=resolved_suggestion_id,
            source_suggestion_priority=resolved_suggestion_priority,
            source_ai_job_id=source.source_ai_job_id,
            source_type=source.source_type,
            prompt=resolved_prompt,
            message="AI cleaning started",
        )
        db.add(job)
        db.add(detail)
    else:
        job = reusable_job
        detail = reusable_detail
        job_id = job.id
        job.original_filename = source.file_name
        job.file_type = "csv"
        job.file_size_bytes = 0
        job.source_dataset_id = source.dataset_id
        job.s3_upload_url = source.file_url
        job.download_url = _ai_cleaned_download_url(job_id)
        job.status = "processing"
        job.progress_pct = 5
        job.current_step = 1
        job.total_steps = 3
        job.current_step_name = "Preparing AI cleaning"
        job.error_message = None
        job.completed_at = None

        detail.source_dataset_id = source.dataset_id
        detail.source_dataset_type = source.dataset_type
        detail.source_dataset_name = source.dataset_name
        detail.source_file_name = source.file_name
        detail.source_storage_key = source.storage_key
        detail.source_file_url = source.file_url
        detail.source_suggestion_id = resolved_suggestion_id
        detail.source_suggestion_priority = resolved_suggestion_priority
        detail.source_ai_job_id = source.source_ai_job_id
        detail.source_type = source.source_type
        detail.prompt = resolved_prompt
        detail.cleaning_strategy = None
        detail.llm_used = False
        detail.target_columns = None
        detail.cleaned_rows = None
        detail.preview_rows_returned = None
        detail.preview_limited = False
        detail.changes_detected = False
        detail.quality_score = None
        detail.analysis = None
        detail.suggestion_source = None
        detail.llm_provider = None
        detail.llm_model = None
        detail.message = "AI cleaning started"
    db.commit()
    db.refresh(job)
    db.refresh(detail)

    # Heavy AI cleaning (LLM over the whole dataset) runs in a background thread so
    # the request returns immediately. On large datasets this can take minutes, which
    # would otherwise blow past the platform's fixed request timeout (App Runner: 120s)
    # and surface as a 504. The client polls GET /ai-cleaning/job/{job_id} for status.
    threading.Thread(
        target=_run_ai_cleaning_worker,
        kwargs={
            "job_id": job_id,
            "storage_key": source.storage_key,
            "source_dataset_id": source.dataset_id,
            "source_dataset_type": source.dataset_type,
            "source_file_name": source.file_name,
            "source_type": source.source_type,
            "resolved_prompt": resolved_prompt,
            "resolved_suggestion_id": resolved_suggestion_id,
            "resolved_cleaning_prompt_type": resolved_cleaning_prompt_type,
            "resolved_target_columns": resolved_target_columns,
        },
        daemon=True,
    ).start()

    return _serialize_ai_cleaning_detail(job, detail)


def _run_ai_cleaning_worker(
    *,
    job_id: str,
    storage_key: str,
    source_dataset_id: int,
    source_dataset_type: str,
    source_file_name: str,
    source_type: str,
    resolved_prompt: str,
    resolved_suggestion_id: str | None,
    resolved_cleaning_prompt_type: Any,
    resolved_target_columns: Any,
) -> None:
    """Background worker: download source, run AI cleaning, finalize the job.

    Runs in its own thread with its own DB session (never share the request session
    across threads). Any failure is recorded on the job as status='failed'.
    """
    db = SessionLocal()
    started_at = time.time()
    try:
        with _download_storage_key(storage_key, prefix=f"{source_dataset_type}_{source_dataset_id}") as source_path:
            source_file_size = os.path.getsize(source_path)
            job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
            detail = db.query(AICleaningJobDetail).filter(AICleaningJobDetail.job_id == job_id).first()
            if job is None or detail is None:
                return

            job.file_size_bytes = source_file_size
            job.progress_pct = 25
            job.current_step_name = "Running AI cleaning"
            job.current_step = 2
            db.commit()

            result = _build_cleaning_result(
                source_path,
                job_id=job_id,
                prompt=resolved_prompt,
                source_type=source_type,
                source_file_name=source_file_name,
                prompt_type_hint=resolved_cleaning_prompt_type,
                target_columns_hint=resolved_target_columns,
            )

            previous_steps = list(job.steps_applied or [])
            latest_steps = result["steps_applied"]
            for step in latest_steps:
                if not isinstance(step, dict):
                    continue
                details = step.setdefault("details", {})
                if isinstance(details, dict):
                    details["source_suggestion_id"] = resolved_suggestion_id
                    details["prompt"] = resolved_prompt
                    details["source_type"] = source_type
            job.steps_applied = previous_steps + latest_steps

            job.status = "completed"
            job.progress_pct = 100
            job.current_step = 3
            job.current_step_name = "Done"
            job.rows_before = result["rows_before"]
            job.rows_after = result["rows_after"]
            job.columns_before = result["columns_before"]
            job.columns_after = result["columns_after"]
            job.cleaning_summary = {
                **result["cleaning_summary"],
                "total_steps": len(job.steps_applied),
                "applied": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "applied"),
                "skipped": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "skipped"),
                "errors": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "error"),
                "latest_run_changes_detected": result["changes_detected"],
            }
            job.duration_seconds = round(time.time() - started_at, 3)
            job.s3_cleaned_url = result["cleaned_file_path"]
            job.output_filename = result["output_filename"]
            job.completed_at = datetime.now(timezone.utc)

            detail.source_type = result["source_type"]
            detail.cleaning_strategy = result["cleaning_strategy"]
            detail.llm_used = result["llm_used"]
            detail.target_columns = result["target_columns"]
            detail.cleaned_storage_key = result["cleaned_storage_key"]
            detail.cleaned_file_path = result["cleaned_file_path"]
            detail.cleaned_data = result["cleaned_data"]
            detail.cleaned_rows = result["cleaned_rows"]
            detail.preview_rows_returned = result["preview_rows_returned"]
            detail.preview_limited = result["preview_limited"]
            detail.changes_detected = result["changes_detected"]
            detail.message = result["message"]

            db.commit()
    except Exception as exc:
        db.rollback()
        job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
        detail = db.query(AICleaningJobDetail).filter(AICleaningJobDetail.job_id == job_id).first()
        if job is not None:
            job.status = "failed"
            job.error_message = str(exc)
            job.duration_seconds = round(time.time() - started_at, 3)
        if detail is not None:
            detail.message = str(exc)
        db.commit()
    finally:
        db.close()


@dataclass(frozen=True)
class _BatchCleaningStep:
    suggestion_id: str
    prompt: str
    priority: str | None
    prompt_type: str | None
    target_columns: tuple[str, ...]


_PRIORITY_ORDER = {"high": 3, "medium": 2, "low": 1}


def _highest_priority(priorities: list[str | None]) -> str | None:
    best: str | None = None
    best_rank = -1
    for priority in priorities:
        rank = _PRIORITY_ORDER.get(str(priority or "").strip().lower(), 0)
        if rank > best_rank:
            best_rank = rank
            best = priority
    return best


def _build_batch_cleaning_result(
    source_path: str,
    *,
    job_id: str,
    steps: list[_BatchCleaningStep],
    source_type: str,
    source_file_name: str,
) -> dict[str, Any]:
    """Apply several cleaning suggestions in a SINGLE streaming pass over the file.

    The source is read once in chunks; every suggestion is applied to each chunk
    in order (each on top of the previous suggestion's output); the result is
    written once and uploaded once. Compared to cleaning one suggestion at a time
    (download -> stats pass -> clean pass -> upload, repeated N times), this keeps
    batch cleaning fast on large datasets (hundreds of thousands of rows) because
    the file is scanned only once for cleaning plus at most one shared pass to
    compute imputation statistics.
    """
    preserve_placeholders = source_type == "clean"

    sample_rows = max(1, int(settings.UAT_AI_PLANNER_SAMPLE_ROWS))
    planner_sample_df = _read_csv_sample(source_path, nrows=sample_rows, preserve_placeholders=preserve_placeholders)
    row_limit = settings.UAT_AI_ROW_LEVEL_LARGE_DATASET_THRESHOLD + 1
    total_rows_hint = _count_csv_rows(source_path, limit=row_limit)

    planned: list[tuple[_BatchCleaningStep, AICleaningPlan]] = []
    for step in steps:
        plan = plan_ai_cleaning(
            planner_sample_df,
            step.prompt,
            total_rows=total_rows_hint,
            prompt_type_hint=step.prompt_type,
            target_columns_hint=list(step.target_columns),
        )
        if plan.strategy == "privacy_blocked":
            if prompt_has_deterministic_cleaning_steps(step.prompt):
                plan = AICleaningPlan(
                    strategy="deterministic_privacy_fallback",
                    target_columns=plan.target_columns,
                    reason=plan.reason,
                )
            else:
                raise error_response(
                    status_code=400,
                    detail=plan.reason or "AI cleaning is blocked for the requested columns.",
                )
        if plan.strategy == "unsupported_large_ai":
            raise error_response(
                status_code=400,
                detail=plan.reason or "Unsupported large-dataset AI cleaning prompt.",
            )
        planned.append((step, plan))

    any_requires_ai = any(plan.requires_ai for _, plan in planned)
    effective_chunksize = (
        settings.CHUNK_SIZE if any_requires_ai else max(settings.CHUNK_SIZE, settings.CHUNK_SIZE * 10)
    )

    # One shared imputation pre-pass for every imputation suggestion, instead of
    # a separate full scan per suggestion. Mean/median needs whole-column stats.
    numeric_targets: set[str] = set()
    mode_targets: set[str] = set()
    step_impute_kinds: list[tuple[bool, bool]] = []
    for step, plan in planned:
        normalized_type = normalize_cleaning_prompt_type(step.prompt_type) if step.prompt_type else None
        wants_numeric = (
            prompt_requests_numeric_imputation(step.prompt) or normalized_type == "numeric_missing_imputation"
        )
        wants_mode = (
            prompt_requests_mode_imputation(step.prompt) or normalized_type == "date_missing_imputation"
        )
        step_impute_kinds.append((wants_numeric, wants_mode))
        step_targets = set(plan.target_columns or [])
        if wants_numeric:
            numeric_targets |= step_targets
        if wants_mode:
            mode_targets |= step_targets

    imputation_values: dict[str, Any] | None = None
    if numeric_targets or mode_targets:
        numeric_accumulator: dict[str, Any] = {}
        mode_accumulator: dict[str, Any] = {}
        for impute_chunk_df in _parse_csv_iter(
            source_path,
            chunksize=effective_chunksize,
            preserve_placeholders=preserve_placeholders,
        ):
            if numeric_targets:
                accumulate_numeric_imputation_stats(impute_chunk_df, numeric_targets, numeric_accumulator)
            if mode_targets:
                accumulate_mode_imputation_stats(impute_chunk_df, mode_targets, mode_accumulator)
        imputation_values = {}
        for (step, plan), (wants_numeric, wants_mode) in zip(planned, step_impute_kinds):
            step_targets = set(plan.target_columns or [])
            if wants_numeric:
                numeric_subset = {
                    column: numeric_accumulator[column]
                    for column in step_targets
                    if column in numeric_accumulator
                }
                imputation_values.update(
                    finalize_numeric_imputation_values(
                        numeric_subset, strategy=numeric_imputation_strategy(step.prompt)
                    )
                )
            if wants_mode:
                mode_subset = {
                    column: mode_accumulator[column]
                    for column in step_targets
                    if column in mode_accumulator
                }
                for column_name, fill_value in finalize_mode_imputation_values(mode_subset).items():
                    imputation_values.setdefault(column_name, fill_value)

    # Per-step runtime state. The LLM chain is stateless and shared; value caches
    # and cross-chunk duplicate hashes must stay isolated per suggestion.
    chain = build_cleaning_chain() if any_requires_ai else None
    ai_caches: dict[int, dict[Any, Any]] = {}
    seen_hashes: dict[int, set[int]] = {}
    for index, (step, plan) in enumerate(planned):
        if plan.requires_ai:
            ai_caches[index] = {}
        if prompt_removes_exact_duplicates(step.prompt):
            seen_hashes[index] = set()

    def _apply_all_steps(chunk_df: pd.DataFrame) -> pd.DataFrame:
        cleaned = chunk_df
        for index, (step, plan) in enumerate(planned):
            cleaned = clean_dataframe_chunk(
                cleaned,
                step.prompt,
                chain=chain,
                plan=plan,
                prompt_type_hint=step.prompt_type,
                target_columns_hint=list(step.target_columns),
                seen_row_hashes=seen_hashes.get(index),
                ai_row_cache=ai_caches.get(index),
                imputation_values=imputation_values,
            )
        return cleaned

    preview_rows: list[dict[str, Any]] = []
    input_total_rows = 0
    total_rows = 0
    wrote_header = False
    changes_detected = False
    columns_before = 0
    columns_after = 0
    output_filename = f"ai_cleaned_{Path(source_file_name).stem}.csv"

    fd, output_path = tempfile.mkstemp(prefix=f"{job_id}_cleaned_", suffix=".csv")
    os.close(fd)

    try:
        with open(output_path, "w", encoding="utf-8", newline="") as output_file:
            for chunk_df in _parse_csv_iter(
                source_path,
                chunksize=effective_chunksize,
                preserve_placeholders=preserve_placeholders,
            ):
                if input_total_rows == 0:
                    columns_before = len(chunk_df.columns)
                input_total_rows += len(chunk_df)
                cleaned_chunk = _apply_all_steps(chunk_df)
                if columns_after == 0:
                    columns_after = len(cleaned_chunk.columns)
                if not changes_detected and not _dataframes_match(chunk_df, cleaned_chunk):
                    changes_detected = True

                preview_remaining = int(settings.UAT_AI_CLEAN_PREVIEW_ROWS) - len(preview_rows)
                if preview_remaining > 0 and not cleaned_chunk.empty:
                    preview_rows.extend(_dataframe_to_json_records(cleaned_chunk.head(preview_remaining)))

                cleaned_chunk.to_csv(output_file, header=not wrote_header, index=False)
                wrote_header = True
                total_rows += len(cleaned_chunk)

            if not wrote_header:
                empty_df = _apply_all_steps(
                    _read_csv_sample(source_path, nrows=0, preserve_placeholders=preserve_placeholders)
                )
                empty_df.to_csv(output_file, header=True, index=False)
                columns_before = len(empty_df.columns)
                columns_after = len(empty_df.columns)

        cleaned_storage_key = _ai_cleaned_output_key(job_id)
        cleaned_file_path = get_object_storage_service().upload_file(output_path, cleaned_storage_key)
        preview_rows_returned = len(preview_rows)
        preview_limited = total_rows > preview_rows_returned

        union_target_columns = sorted({column for _, plan in planned for column in (plan.target_columns or [])})
        llm_used = any(cleaning_strategy_uses_llm(plan.strategy) for _, plan in planned)
        distinct_strategies = list(dict.fromkeys(plan.strategy for _, plan in planned))

        status_note = (
            "Changes were detected."
            if changes_detected
            else "Prompts were processed, but no data changes were detected."
        )
        source_label = "original dataset" if source_type == "raw" else "previous AI-cleaned output"
        target_label = ", ".join(union_target_columns) if union_target_columns else "all columns"
        message = (
            f"{status_note} Applied {len(planned)} suggestion(s) in a single pass. "
            f"Source: {source_label}. Target columns: {target_label}. "
            f"API response includes {preview_rows_returned} of {total_rows} cleaned row(s)."
        )

        steps_applied = [
            {
                "step": "ai_prompt_cleaning",
                "status": "applied",
                "details": {
                    "cleaning_strategy": plan.strategy,
                    "target_columns": plan.target_columns,
                    "llm_used": cleaning_strategy_uses_llm(plan.strategy),
                    "source_suggestion_id": step.suggestion_id,
                    "prompt": step.prompt,
                    "source_type": source_type,
                },
            }
            for step, plan in planned
        ]

        return {
            "source_type": source_type,
            "cleaning_strategy": ",".join(distinct_strategies) or "deterministic",
            "llm_used": llm_used,
            "target_columns": union_target_columns,
            "cleaned_storage_key": cleaned_storage_key,
            "cleaned_file_path": cleaned_file_path,
            "cleaned_data": preview_rows,
            "cleaned_rows": total_rows,
            "preview_rows_returned": preview_rows_returned,
            "preview_limited": preview_limited,
            "changes_detected": changes_detected,
            "rows_before": input_total_rows,
            "rows_after": total_rows,
            "columns_before": columns_before,
            "columns_after": columns_after,
            "output_filename": output_filename,
            "prompt": "\n".join(f"- {step.prompt}" for step, _ in planned),
            "message": message,
            "steps_applied": steps_applied,
            "cleaning_summary": {
                "total_steps": len(steps_applied),
                "applied": len(steps_applied),
                "skipped": 0,
                "errors": 0,
                "changes_detected": changes_detected,
            },
        }
    finally:
        if os.path.exists(output_path):
            os.remove(output_path)


def run_ai_cleaning_batch(
    db: Session,
    *,
    current_user: User,
    dataset_id: int,
    dataset_type: str,
    suggestion_ids: list[str],
    source_ai_job_id: str | None = None,
) -> dict[str, Any]:
    """Clean every selected suggestion in ONE request and ONE streaming pass.

    All suggestions are applied on top of each other into a single cleaned file
    and a single AI cleaning job, reading the source only once. Used to clean an
    entire suggestion category at once: the frontend passes all suggestion IDs of
    that category in ``suggestion_ids``.
    """
    _ensure_ai_cleaning_tables()
    ordered_suggestion_ids = [
        suggestion_id for suggestion_id in dict.fromkeys(suggestion_ids) if suggestion_id
    ]
    if not ordered_suggestion_ids:
        raise error_response(status_code=400, detail="At least one suggestion_id is required")

    reusable_job_record = _get_reusable_ai_job(
        db,
        current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_ai_job_id=source_ai_job_id,
    )
    reusable_job: CleaningJob | None = reusable_job_record[0] if reusable_job_record is not None else None
    reusable_detail: AICleaningJobDetail | None = reusable_job_record[1] if reusable_job_record is not None else None
    source = _resolve_ai_source(
        db,
        current_user,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_ai_job_id=reusable_detail.job_id if reusable_detail is not None else source_ai_job_id,
    )

    steps: list[_BatchCleaningStep] = []
    for suggestion_id in ordered_suggestion_ids:
        (
            resolved_prompt,
            resolved_suggestion_id,
            resolved_priority,
            resolved_prompt_type,
            resolved_target_columns,
        ) = _resolve_ai_cleaning_prompt(
            db,
            current_user,
            dataset_id=dataset_id,
            dataset_type=dataset_type,
            suggestion_id=suggestion_id,
            source_job_detail=reusable_detail,
        )
        steps.append(
            _BatchCleaningStep(
                suggestion_id=resolved_suggestion_id,
                prompt=resolved_prompt,
                priority=resolved_priority,
                prompt_type=resolved_prompt_type,
                target_columns=tuple(resolved_target_columns),
            )
        )

    applied_suggestion_ids = [step.suggestion_id for step in steps]
    combined_prompt = "\n".join(f"- {step.prompt}" for step in steps)
    combined_priority = _highest_priority([step.priority for step in steps])

    if reusable_job is None or reusable_detail is None:
        job_id = str(uuid.uuid4())
        job = CleaningJob(
            id=job_id,
            original_filename=source.file_name,
            file_type="csv",
            file_size_bytes=0,
            source_dataset_id=source.dataset_id,
            s3_upload_url=source.file_url,
            download_url=_ai_cleaned_download_url(job_id),
            status="processing",
            progress_pct=5,
            current_step=1,
            total_steps=3,
            current_step_name="Preparing AI cleaning",
            ai_cleaning_type=True,
        )
        detail = AICleaningJobDetail(
            job_id=job_id,
            created_by_user_id=current_user.id,
            source_dataset_id=source.dataset_id,
            source_dataset_type=source.dataset_type,
            source_dataset_name=source.dataset_name,
            source_file_name=source.file_name,
            source_storage_key=source.storage_key,
            source_file_url=source.file_url,
            source_suggestion_id=applied_suggestion_ids[0],
            source_suggestion_priority=combined_priority,
            source_ai_job_id=source.source_ai_job_id,
            source_type=source.source_type,
            prompt=combined_prompt,
            message="AI cleaning started",
        )
        db.add(job)
        db.add(detail)
    else:
        job = reusable_job
        detail = reusable_detail
        job_id = job.id
        job.original_filename = source.file_name
        job.file_type = "csv"
        job.file_size_bytes = 0
        job.source_dataset_id = source.dataset_id
        job.s3_upload_url = source.file_url
        job.download_url = _ai_cleaned_download_url(job_id)
        job.status = "processing"
        job.progress_pct = 5
        job.current_step = 1
        job.total_steps = 3
        job.current_step_name = "Preparing AI cleaning"
        job.error_message = None
        job.completed_at = None

        detail.source_dataset_id = source.dataset_id
        detail.source_dataset_type = source.dataset_type
        detail.source_dataset_name = source.dataset_name
        detail.source_file_name = source.file_name
        detail.source_storage_key = source.storage_key
        detail.source_file_url = source.file_url
        detail.source_suggestion_id = applied_suggestion_ids[0]
        detail.source_suggestion_priority = combined_priority
        detail.source_ai_job_id = source.source_ai_job_id
        detail.source_type = source.source_type
        detail.prompt = combined_prompt
        detail.cleaning_strategy = None
        detail.llm_used = False
        detail.target_columns = None
        detail.cleaned_rows = None
        detail.preview_rows_returned = None
        detail.preview_limited = False
        detail.changes_detected = False
        detail.quality_score = None
        detail.analysis = None
        detail.suggestion_source = None
        detail.llm_provider = None
        detail.llm_model = None
        detail.message = "AI cleaning started"
    db.commit()
    db.refresh(job)
    db.refresh(detail)

    # Batch AI cleaning runs one streaming pass over the whole file applying every
    # suggestion. On large datasets this exceeds the platform request timeout, so it
    # runs in a background thread; the client polls GET /ai-cleaning/job/{job_id}.
    threading.Thread(
        target=_run_ai_cleaning_batch_worker,
        kwargs={
            "job_id": job_id,
            "storage_key": source.storage_key,
            "source_dataset_id": source.dataset_id,
            "source_dataset_type": source.dataset_type,
            "source_file_name": source.file_name,
            "source_type": source.source_type,
            "steps": steps,
        },
        daemon=True,
    ).start()

    serialized = _serialize_ai_cleaning_detail(job, detail)
    serialized["applied_suggestion_ids"] = applied_suggestion_ids
    return serialized


def _run_ai_cleaning_batch_worker(
    *,
    job_id: str,
    storage_key: str,
    source_dataset_id: int,
    source_dataset_type: str,
    source_file_name: str,
    source_type: str,
    steps: list[_BatchCleaningStep],
) -> None:
    """Background worker for batch AI cleaning. Own thread, own DB session."""
    db = SessionLocal()
    started_at = time.time()
    try:
        with _download_storage_key(storage_key, prefix=f"{source_dataset_type}_{source_dataset_id}") as source_path:
            source_file_size = os.path.getsize(source_path)
            job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
            detail = db.query(AICleaningJobDetail).filter(AICleaningJobDetail.job_id == job_id).first()
            if job is None or detail is None:
                return

            job.file_size_bytes = source_file_size
            job.progress_pct = 25
            job.current_step_name = "Running AI cleaning"
            job.current_step = 2
            db.commit()

            result = _build_batch_cleaning_result(
                source_path,
                job_id=job_id,
                steps=steps,
                source_type=source_type,
                source_file_name=source_file_name,
            )

            previous_steps = list(job.steps_applied or [])
            job.steps_applied = previous_steps + result["steps_applied"]

            job.status = "completed"
            job.progress_pct = 100
            job.current_step = 3
            job.current_step_name = "Done"
            job.rows_before = result["rows_before"]
            job.rows_after = result["rows_after"]
            job.columns_before = result["columns_before"]
            job.columns_after = result["columns_after"]
            job.cleaning_summary = {
                **result["cleaning_summary"],
                "total_steps": len(job.steps_applied),
                "applied": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "applied"),
                "skipped": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "skipped"),
                "errors": sum(1 for item in job.steps_applied if isinstance(item, dict) and item.get("status") == "error"),
                "latest_run_changes_detected": result["changes_detected"],
            }
            job.duration_seconds = round(time.time() - started_at, 3)
            job.s3_cleaned_url = result["cleaned_file_path"]
            job.output_filename = result["output_filename"]
            job.completed_at = datetime.now(timezone.utc)

            detail.source_type = result["source_type"]
            detail.cleaning_strategy = result["cleaning_strategy"]
            detail.llm_used = result["llm_used"]
            detail.target_columns = result["target_columns"]
            detail.cleaned_storage_key = result["cleaned_storage_key"]
            detail.cleaned_file_path = result["cleaned_file_path"]
            detail.cleaned_data = result["cleaned_data"]
            detail.cleaned_rows = result["cleaned_rows"]
            detail.preview_rows_returned = result["preview_rows_returned"]
            detail.preview_limited = result["preview_limited"]
            detail.changes_detected = result["changes_detected"]
            detail.prompt = result["prompt"]
            detail.message = result["message"]

            db.commit()
    except Exception as exc:
        db.rollback()
        job = db.query(CleaningJob).filter(CleaningJob.id == job_id).first()
        detail = db.query(AICleaningJobDetail).filter(AICleaningJobDetail.job_id == job_id).first()
        if job is not None:
            job.status = "failed"
            job.error_message = str(exc)
            job.duration_seconds = round(time.time() - started_at, 3)
        if detail is not None:
            detail.message = str(exc)
        db.commit()
    finally:
        db.close()


def get_ai_cleaning_detail(
    db: Session,
    *,
    current_user: User,
    dataset_id: int,
    dataset_type: str | None = None,
) -> dict[str, Any]:
    query = (
        db.query(CleaningJob, AICleaningJobDetail)
        .join(AICleaningJobDetail, AICleaningJobDetail.job_id == CleaningJob.id)
        .filter(
            CleaningJob.ai_cleaning_type.is_(True),
            AICleaningJobDetail.created_by_user_id == current_user.id,
            AICleaningJobDetail.source_dataset_id == dataset_id,
        )
    )
    if dataset_type:
        query = query.filter(AICleaningJobDetail.source_dataset_type == dataset_type)

    record = (
        query
        .order_by(AICleaningJobDetail.updated_at.desc(), AICleaningJobDetail.created_at.desc())
        .first()
    )
    if record is None:
        raise error_response(status_code=404, detail="AI cleaning data not found for the provided dataset")

    job, detail = record
    return _serialize_ai_cleaning_detail(job, detail)


def get_ai_cleaning_detail_by_job_id(db: Session, *, current_user: User, job_id: str) -> dict[str, Any]:
    detail = (
        db.query(AICleaningJobDetail)
        .filter(
            AICleaningJobDetail.job_id == job_id,
            AICleaningJobDetail.created_by_user_id == current_user.id,
        )
        .first()
    )
    if detail is None:
        raise error_response(status_code=404, detail="AI cleaning job not found")

    job = db.query(CleaningJob).filter(CleaningJob.id == job_id, CleaningJob.ai_cleaning_type.is_(True)).first()
    if job is None:
        raise error_response(status_code=404, detail="AI cleaning job record not found")

    return _serialize_ai_cleaning_detail(job, detail)


def get_ai_cleaned_download_payload(
    db: Session,
    *,
    current_user: User,
    job_id: str,
) -> tuple[str, str]:
    detail = (
        db.query(AICleaningJobDetail)
        .filter(
            AICleaningJobDetail.job_id == job_id,
            AICleaningJobDetail.created_by_user_id == current_user.id,
        )
        .first()
    )
    if detail is None or not detail.cleaned_storage_key:
        raise error_response(status_code=404, detail="AI cleaned file not found")

    fd, path = tempfile.mkstemp(prefix=f"{job_id}_ai_cleaned_", suffix=".csv")
    os.close(fd)
    restored = get_object_storage_service().download_file(detail.cleaned_storage_key, path)
    if not restored:
        if os.path.exists(path):
            os.remove(path)
        raise error_response(status_code=404, detail="AI cleaned file is unavailable in object storage")

    filename = f"{job_id}_ai_cleaned.csv"
    return path, filename


def _to_suggestion_dict(suggestion: DataSuggestion, *, suggestion_id: str | None = None) -> dict[str, Any]:
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


def _build_profile_for_storage_key(storage_key: str) -> dict[str, Any]:
    with _download_storage_key(storage_key, prefix="ai_analysis") as file_path:
        return build_dataset_profile_from_chunks(
            _parse_csv_iter(
                file_path,
                chunksize=max(_cleaning_chunksize(), 5000),
                preserve_placeholders=True,
            )
        )


def analyze_ai_cleaning_output(
    db: Session,
    *,
    current_user: User,
    job_id: str,
    use_llm: bool,
    llm_provider: str | None = None,
    llm_model: str | None = None,
) -> dict[str, Any]:
    _ensure_ai_cleaning_tables()
    detail = (
        db.query(AICleaningJobDetail)
        .filter(
            AICleaningJobDetail.job_id == job_id,
            AICleaningJobDetail.created_by_user_id == current_user.id,
        )
        .first()
    )
    if detail is None:
        raise error_response(status_code=404, detail="AI cleaning job not found")
    if not detail.cleaned_storage_key:
        raise error_response(status_code=409, detail="AI cleaned output is not available for analysis")

    dataset_profile = _build_profile_for_storage_key(detail.cleaned_storage_key)
    base_quality_score = compute_quality_score(dataset_profile)
    raw_quality_score = _get_latest_raw_analysis_score(
        db,
        current_user,
        dataset_id=detail.source_dataset_id,
        dataset_type=detail.source_dataset_type,
    )
    enriched_profile = {**dataset_profile, "quality_score": base_quality_score}
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
        return coalesce_priority_clean_quality_score(
            suggestion_aligned_score,
            raw_score=raw_quality_score,
            previous_clean_score=detail.quality_score,
            changes_detected=bool(detail.changes_detected),
            suggestion_priority=_resolve_source_suggestion_priority(db, current_user, detail),
        )

    def _build_clean_verification(current_suggestions: list[DataSuggestion]) -> tuple[bool | None, int | None, int | None]:
        if not detail.prompt:
            return None, None, None

        source_prompt_type, source_target_columns = _resolve_source_suggestion_prompt_metadata(
            db,
            current_user,
            detail,
        )
        matching_suggestions, _ = split_matching_suggestions(
            cleaned_prompt=detail.prompt,
            cleaned_prompt_type=source_prompt_type,
            cleaned_target_columns=source_target_columns,
            suggestions=verification_rule_suggestions or current_suggestions,
        )
        source_suggestion_match_count = len(matching_suggestions)
        source_suggestion_resolved = source_suggestion_match_count == 0
        return source_suggestion_resolved, source_suggestion_match_count, None

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
            message = "AI-cleaned dataset analysis completed successfully."
        except Exception as exc:
            suggestions = generate_rule_based_suggestions(enriched_profile)
            resolved_provider = llm_provider
            resolved_model = llm_model
            message = (
                f"LLM suggestion generation failed and rule-based suggestions were used instead: {exc}"
            )
    else:
        suggestions = generate_rule_based_suggestions(enriched_profile)
        message = "AI-cleaned dataset analysis completed successfully using rule-based suggestions."

    suggestions = filter_actionable_suggestions(dataset_profile, suggestions)

    (
        source_suggestion_resolved,
        source_suggestion_match_count,
        quality_score_delta,
    ) = _build_clean_verification(suggestions)
    if source_suggestion_resolved:
        source_prompt_type, source_target_columns = _resolve_source_suggestion_prompt_metadata(
            db,
            current_user,
            detail,
        )
        _, suggestions = split_matching_suggestions(
            cleaned_prompt=detail.prompt,
            cleaned_prompt_type=source_prompt_type,
            cleaned_target_columns=source_target_columns,
            suggestions=suggestions,
        )
    quality_score = _finalize_quality_score(suggestions)
    quality_score_delta = quality_score - raw_quality_score if raw_quality_score is not None else None
    if source_suggestion_resolved is not None:
        verification_note = (
            "Selected cleaned suggestion is no longer detected in cleaned analysis."
            if source_suggestion_resolved
            else f"Selected cleaned suggestion still has {source_suggestion_match_count} matching issue(s) in cleaned analysis."
        )
        message = f"{message} {verification_note}"

    payload = {
        "job_id": job_id,
        "source_dataset_id": detail.source_dataset_id,
        "source_dataset_type": detail.source_dataset_type,
        "source_type": "clean",
        "quality_score": quality_score,
        "llm_used": llm_used,
        "suggestion_source": suggestion_source,
        "llm_provider": resolved_provider,
        "llm_model": resolved_model,
        "message": message,
        "source_suggestion_id": detail.source_suggestion_id,
        "source_suggestion_resolved": source_suggestion_resolved,
        "source_suggestion_match_count": source_suggestion_match_count,
        "quality_score_delta": quality_score_delta,
        "suggestions": [
            _to_suggestion_dict(suggestion, suggestion_id=str(uuid.uuid4()))
            for suggestion in suggestions
        ],
        "dataset_profile": dataset_profile,
    }

    detail.quality_score = quality_score
    detail.analysis = payload
    detail.suggestion_source = suggestion_source
    detail.llm_used = llm_used
    detail.llm_provider = resolved_provider
    detail.llm_model = resolved_model
    detail.message = message
    db.commit()

    return payload
