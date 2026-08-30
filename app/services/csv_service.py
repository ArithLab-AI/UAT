import csv
import io
import re
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from datetime import date, datetime, time
from pathlib import Path, PureWindowsPath
import openpyxl
import pandas as pd
import xlrd
from fastapi import UploadFile
from starlette.concurrency import run_in_threadpool
from sqlalchemy.orm import Session, object_session
from app.models.csv_dataset_models import (
    CsvMergedDataset,
    CsvUploadedDataset,
)
from app.services.analysis_profile_service import (
    COLUMN_TYPE_SAMPLE_ROWS,
    column_type_label,
    infer_column_data_types,
)
from app.utils.object_storage import get_object_storage_service
from app.utils.responses import error_response


@dataclass(frozen=True)
class ParsedUpload:
    file_name: str
    file_size: int
    columns: list[str]
    internal_columns: list[str]
    rows: list[dict]
    sheet_name: str | None = None


@dataclass(frozen=True)
class PendingSheetSelection:
    requires_sheet_selection: bool
    file_token: str
    file_name: str
    available_sheets: list[str]
    sheet_count: int
    preview_row_count: int | None = None


def _normalize_dataset_name(name: str) -> str:
    cleaned_name = " ".join(name.strip().split())
    if not cleaned_name:
        raise error_response(status_code=400, detail="Dataset name cannot be empty")
    return cleaned_name


def _build_table_slug(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return slug or "dataset"


def _pending_dataset_table_names(db: Session) -> set[str]:
    return {
        dataset.table_name
        for dataset in db.new
        if isinstance(dataset, (CsvUploadedDataset, CsvMergedDataset)) and dataset.table_name
    }


def _generate_table_name(db: Session, prefix: str, dataset_name: str) -> str:
    base_slug = _build_table_slug(dataset_name)
    candidate = f"{prefix}_{base_slug}"[:55].rstrip("_")
    table_name = candidate
    counter = 1

    pending_table_names = _pending_dataset_table_names(db)
    while (
        table_name in pending_table_names
        or db.query(CsvUploadedDataset.id).filter(CsvUploadedDataset.table_name == table_name).first()
        or db.query(CsvMergedDataset.id).filter(CsvMergedDataset.table_name == table_name).first()
    ):
        suffix = f"_{counter}"
        table_name = f"{candidate[: 63 - len(suffix)]}{suffix}"
        counter += 1

    return table_name


def _csv_dataset_storage_key(table_name: str) -> str:
    return f"csv_datasets/{table_name}.csv"


def _clean_upload_file_name(file_name: str) -> str:
    return Path(PureWindowsPath(file_name).name).name


@contextmanager
def _temporary_csv_file(prefix: str):
    temp_file = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".csv", delete=False)
    temp_file.close()
    temp_path = Path(temp_file.name)
    try:
        yield temp_path
    finally:
        temp_path.unlink(missing_ok=True)


def _write_rows_to_csv_file(file_path: Path, columns: list[str], rows: list[dict]) -> None:
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _build_row_from_values(
    internal_columns: list[str],
    values: list[Any],
    missing_value: Any = None,
) -> dict:
    return {
        column: _normalize_scalar_value(values[index] if index < len(values) else missing_value)
        for index, column in enumerate(internal_columns)
    }


def _upload_rows_to_object_storage(
    *,
    table_name: str,
    columns: list[str],
    rows: list[dict],
) -> tuple[str, str, int]:
    storage_service = get_object_storage_service()
    if not storage_service.enabled:
        raise error_response(
            status_code=500,
            detail="AWS S3 bucket is not configured for CSV dataset storage",
        )

    storage_key = _csv_dataset_storage_key(table_name)
    with _temporary_csv_file(prefix=f"{table_name}_") as temp_path:
        _write_rows_to_csv_file(temp_path, columns, rows)
        file_size = os.path.getsize(temp_path)
        file_url = storage_service.upload_file(str(temp_path), storage_key)
    return storage_key, file_url, file_size


def _fetch_dataset_rows(
    *,
    table_name: str,
    columns: list[str],
    storage_key: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    storage_service = get_object_storage_service()
    if not storage_service.enabled:
        raise error_response(
            status_code=500,
            detail="AWS S3 bucket is not configured for CSV dataset storage",
        )

    storage_key = storage_key or _csv_dataset_storage_key(table_name)
    with _temporary_csv_file(prefix=f"{table_name}_") as temp_path:
        restored = storage_service.download_file(storage_key, str(temp_path))
        if not restored:
            raise error_response(
                status_code=404,
                detail=f"Dataset data for {table_name} was not found in object storage",
            )

        with temp_path.open("r", newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            rows: list[dict] = []
            for row in reader:
                rows.append({column: row.get(column) for column in columns})
                if limit is not None and len(rows) >= limit:
                    break
            return rows


def build_column_types(
    columns: list[str],
    internal_columns: list[str],
    rows: list[dict],
) -> list[str]:
    """Infer one datatype per column from the dataset's own values, in ``columns`` order."""
    sample = rows[:COLUMN_TYPE_SAMPLE_ROWS]
    if not sample:
        return ["string"] * len(columns)
    frame = pd.DataFrame(sample, columns=internal_columns)
    return infer_column_data_types(frame, columns)


def resolve_dataset_column_types(dataset) -> list[str] | None:
    """Column types for a dataset, read back from its stored file when they were never saved.

    Datasets created before column types were stored carry none, so they are inferred from a
    sample of the stored rows. Returns ``None`` when that file can't be read, so the dataset
    can still be served without types instead of the request failing.
    """
    stored_types = dataset.column_types
    columns = list(dataset.columns or [])
    if stored_types and len(stored_types) == len(columns):
        return list(stored_types)

    try:
        rows = _fetch_dataset_rows(
            table_name=dataset.table_name,
            columns=dataset.internal_columns,
            storage_key=dataset.storage_key,
            limit=COLUMN_TYPE_SAMPLE_ROWS,
        )
    except Exception:
        return None

    return build_column_types(columns, dataset.internal_columns, rows)


# Distinct values shown per column in the detailed dataset view.
COLUMN_DETAIL_SAMPLE_VALUES = 3


def _percentage(count: int, total: int) -> float:
    if not total:
        return 0.0
    return round(count / total * 100, 2)


def _column_value_stats(series: pd.Series, total_rows: int) -> dict:
    """Missing/unique counts and a short value sample for one column.

    Blank strings count as missing alongside real nulls, because a CSV has no other way of
    spelling an empty cell.
    """
    values = series.astype("string").fillna("").str.strip()
    present_values = values[values != ""]
    unique_values = present_values.drop_duplicates()
    missing_count = total_rows - int(present_values.size)

    return {
        "missing_count": missing_count,
        "missing_percentage": _percentage(missing_count, total_rows),
        "unique_count": int(unique_values.size),
        "unique_percentage": _percentage(int(unique_values.size), total_rows),
        "sample": unique_values.head(COLUMN_DETAIL_SAMPLE_VALUES).tolist(),
    }


def build_dataset_column_details(dataset) -> list[dict] | None:
    """Per-column details for a dataset: datatype, missing values, unique values and a sample.

    Reads every stored row (unique counts need the whole column), so it is only worth doing
    when the caller explicitly asks for the detailed view. Returns ``None`` when the stored
    file can't be read, so the dataset can still be served without the details.
    """
    columns = list(dataset.columns or [])
    internal_columns = list(dataset.internal_columns or columns)

    try:
        rows = _fetch_dataset_rows(
            table_name=dataset.table_name,
            columns=internal_columns,
            storage_key=dataset.storage_key,
        )
    except Exception:
        return None

    frame = pd.DataFrame(rows, columns=internal_columns)
    total_rows = len(frame)

    column_types = list(dataset.column_types or [])
    if len(column_types) != len(columns):
        column_types = infer_column_data_types(frame, columns)

    details: list[dict] = []
    for index, column in enumerate(columns):
        column_type = column_types[index] if index < len(column_types) else None
        internal_column = internal_columns[index] if index < len(internal_columns) else column
        series = (
            frame[internal_column]
            if internal_column in frame.columns
            else pd.Series([None] * total_rows, dtype=object)
        )
        details.append(
            {
                "name": column,
                "type": column_type,
                "display_type": column_type_label(column_type),
                **_column_value_stats(series, total_rows),
            }
        )
    return details


def _delete_dataset_rows(*, table_name: str, storage_key: str | None = None) -> None:
    get_object_storage_service().delete_file(storage_key or _csv_dataset_storage_key(table_name))


def _normalize_scalar_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    return str(value)


def _normalize_header_value(value: Any) -> str:
    normalized_value = _normalize_scalar_value(value)
    if normalized_value is None:
        return ""
    return str(normalized_value).strip()


def _canonicalize_column_name(value: Any) -> str:
    normalized_value = _normalize_header_value(value)
    if re.fullmatch(r"[+-]?\d+\.0+", normalized_value):
        return str(int(float(normalized_value)))
    return normalized_value


def _normalize_columns(file_name: str, columns: list[Any]) -> tuple[list[str], list[str]]:
    if not columns:
        raise error_response(
            status_code=400,
            detail=f"{file_name} does not contain a header row",
        )

    original_columns: list[str] = []
    internal_columns: list[str] = []
    seen_columns: dict[str, int] = {}

    for index, column in enumerate(columns, start=1):
        original_name = _normalize_header_value(column) or f"column_{index}"
        original_columns.append(original_name)
        base_name = _canonicalize_column_name(column) or f"column_{index}"
        duplicate_count = seen_columns.get(base_name, 0)
        seen_columns[base_name] = duplicate_count + 1
        if duplicate_count:
            internal_columns.append(f"{base_name}_{duplicate_count + 1}")
        else:
            internal_columns.append(base_name)

    return original_columns, internal_columns


def _is_empty_upload_row(row: list[Any]) -> bool:
    return all(_normalize_header_value(value) == "" for value in row)


def _is_header_annotation_row(row: list[Any]) -> bool:
    non_empty_values = [
        _normalize_header_value(value)
        for value in row
        if _normalize_header_value(value) != ""
    ]
    if not non_empty_values:
        return False
    return all(value.startswith("(") and value.endswith(")") for value in non_empty_values)


def _detect_header_row_index(rows: list[list[Any]]) -> int:
    scan_rows = rows[:25]
    best_index = 0
    best_non_empty_count = -1

    for index, row in enumerate(scan_rows):
        non_empty_count = sum(1 for value in row if _normalize_header_value(value) != "")
        if non_empty_count > best_non_empty_count:
            best_index = index
            best_non_empty_count = non_empty_count

    return best_index


def _split_rows_by_detected_header(
    file_name: str,
    rows: list[list[Any]],
) -> tuple[list[str], list[str], list[list[Any]]]:
    if not rows:
        raise error_response(
            status_code=400,
            detail=f"{file_name} does not contain a header row",
        )

    header_index = _detect_header_row_index(rows)
    header_row = rows[header_index]
    original_columns, internal_columns = _normalize_columns(file_name, header_row)

    data_rows = rows[header_index + 1 :]
    while data_rows and (_is_empty_upload_row(data_rows[0]) or _is_header_annotation_row(data_rows[0])):
        data_rows = data_rows[1:]

    return original_columns, internal_columns, data_rows


def _get_csv_dialect(text_content: str):
    sample = text_content[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.excel


def _parse_csv_content(file_name: str, content: bytes) -> tuple[list[str], list[str], list[dict]]:
    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise error_response(
            status_code=400,
            detail=f"{file_name} must be UTF-8 encoded",
        ) from exc

    csv_reader = csv.reader(io.StringIO(text_content), dialect=_get_csv_dialect(text_content))
    original_columns, internal_columns, data_rows = _split_rows_by_detected_header(
        file_name,
        [list(row or []) for row in csv_reader],
    )

    rows = []
    for row in data_rows:
        rows.append(_build_row_from_values(internal_columns, row, missing_value=""))

    return original_columns, internal_columns, rows


def _parse_xlsx_content(file_name: str, content: bytes) -> tuple[list[str], list[str], list[dict]]:
    try:
        workbook = openpyxl.load_workbook(
            filename=io.BytesIO(content),
            read_only=True,
            data_only=True,
        )
    except Exception as exc:
        raise error_response(
            status_code=400,
            detail=f"{file_name} could not be read as an XLSX file",
        ) from exc

    sheet = workbook.worksheets[0] if workbook.worksheets else None
    if sheet is None:
        raise error_response(status_code=400, detail=f"{file_name} does not contain any sheets")

    sheet_rows = [list(row or []) for row in sheet.iter_rows(values_only=True)]
    original_columns, internal_columns, data_rows = _split_rows_by_detected_header(
        file_name,
        sheet_rows,
    )

    rows = []
    for row in data_rows:
        rows.append(_build_row_from_values(internal_columns, list(row or [])))

    workbook.close()
    return original_columns, internal_columns, rows


def _parse_xls_content(file_name: str, content: bytes) -> tuple[list[str], list[str], list[dict]]:
    try:
        workbook = xlrd.open_workbook(file_contents=content)
    except Exception as exc:
        raise error_response(
            status_code=400,
            detail=f"{file_name} could not be read as an XLS file",
        ) from exc

    if workbook.nsheets == 0:
        raise error_response(status_code=400, detail=f"{file_name} does not contain any sheets")

    sheet = workbook.sheet_by_index(0)
    sheet_rows = [sheet.row_values(row_index) for row_index in range(sheet.nrows)]
    original_columns, internal_columns, data_rows = _split_rows_by_detected_header(
        file_name,
        sheet_rows,
    )

    rows = []
    for row in data_rows:
        rows.append(_build_row_from_values(internal_columns, row))

    return original_columns, internal_columns, rows


@contextmanager
def _temporary_upload_file(file_name: str, content: bytes):
    suffix = Path(file_name).suffix.lower()
    temp_file = tempfile.NamedTemporaryFile(prefix="csv_upload_", suffix=suffix, delete=False)
    try:
        temp_file.write(content)
        temp_file.close()
        yield Path(temp_file.name)
    finally:
        Path(temp_file.name).unlink(missing_ok=True)


async def parse_csv_upload(file: UploadFile, *, user_id: int) -> ParsedUpload | PendingSheetSelection:
    if not file.filename:
        raise error_response(status_code=400, detail="Uploaded file must have a name")

    file_name = _clean_upload_file_name(file.filename)
    suffix = Path(file_name).suffix.lower()
    allowed_extensions = {".csv", ".xlsx", ".xls"}
    if suffix not in allowed_extensions:
        raise error_response(
            status_code=400,
            detail="Only CSV, XLSX, and XLS files are allowed",
        )

    try:
        content = await file.read()
        if not content:
            raise error_response(status_code=400, detail=f"{file_name} is empty")
        file_size = len(content)

        if suffix == ".csv":
            original_columns, internal_columns, rows = await run_in_threadpool(
                _parse_csv_content, file_name, content
            )
            return ParsedUpload(
                file_name=file_name,
                file_size=file_size,
                columns=original_columns,
                internal_columns=internal_columns,
                rows=rows,
            )
        from app.services.excel_sheet_service import extract_sheet_names, process_selected_sheet

        sheet_names = await run_in_threadpool(extract_sheet_names, content)
        if len(sheet_names) > 1:
            from app.services.temporary_upload_service import store_temporary_upload

            temporary_upload = store_temporary_upload(
                content=content,
                original_file_name=file_name,
                available_sheets=sheet_names,
                user_id=user_id,
            )
            return PendingSheetSelection(
                requires_sheet_selection=True,
                file_token=temporary_upload.token,
                file_name=file_name,
                available_sheets=sheet_names,
                sheet_count=len(sheet_names),
                preview_row_count=None,
            )

        with _temporary_upload_file(file_name, content) as temp_path:
            (
                parsed_file_name,
                parsed_file_size,
                original_columns,
                internal_columns,
                rows,
                sheet_name,
            ) = await run_in_threadpool(
                process_selected_sheet,
                file_path=str(temp_path),
                file_name=file_name,
                sheet_name=sheet_names[0],
                available_sheets=sheet_names,
            )
            return ParsedUpload(
                file_name=parsed_file_name,
                file_size=parsed_file_size,
                columns=original_columns,
                internal_columns=internal_columns,
                rows=rows,
                sheet_name=sheet_name,
            )
    finally:
        await file.close()


def build_dataset_name(explicit_name: str | None, file_name: str) -> str:
    if explicit_name:
        return _normalize_dataset_name(explicit_name)
    return _normalize_dataset_name(Path(file_name).stem.replace("_", " ").replace("-", " "))


def count_user_active_datasets(db: Session, user_id: int) -> int:
    uploaded_count = (
        db.query(CsvUploadedDataset)
        .filter(CsvUploadedDataset.created_by_user_id == user_id)
        .count()
    )
    merged_count = (
        db.query(CsvMergedDataset)
        .filter(CsvMergedDataset.created_by_user_id == user_id)
        .count()
    )
    return uploaded_count + merged_count


def _build_merge_column_mappings(
    source_datasets: list[CsvUploadedDataset],
) -> tuple[list[str], dict[int, dict[str, str]]]:
    if len(source_datasets) < 2:
        raise error_response(
            status_code=400,
            detail="At least two uploaded datasets are required to merge",
        )

    ordered_columns = list(source_datasets[0].internal_columns)
    if len(set(ordered_columns)) != len(ordered_columns):
        raise error_response(
            status_code=400,
            detail=(
                f"{source_datasets[0].name} cannot be merged because it contains duplicate "
                "column names after header normalization"
            ),
        )

    column_mappings: dict[int, dict[str, str]] = {
        source_datasets[0].id: dict(zip(ordered_columns, source_datasets[0].internal_columns))
    }
    mismatched_datasets = [
        dataset.name
        for dataset in source_datasets[1:]
        if list(dataset.internal_columns) != ordered_columns
    ]

    if mismatched_datasets:
        raise error_response(
            status_code=400,
            detail=(
                "Selected dataset files cannot be merged because their headers do not match "
                f"exactly. Mismatched datasets: {', '.join(mismatched_datasets)}"
            ),
        )

    for dataset in source_datasets[1:]:
        canonical_columns = list(dataset.internal_columns)
        if len(set(canonical_columns)) != len(canonical_columns):
            raise error_response(
                status_code=400,
                detail=(
                    f"{dataset.name} cannot be merged because it contains duplicate column "
                    "names after header normalization"
                ),
            )
        column_mappings[dataset.id] = dict(zip(ordered_columns, dataset.internal_columns))

    return ordered_columns, column_mappings


MERGE_TYPE_INFO = {
    "inner": "Includes only the rows that have matching values in both datasets.",
    "left": "Includes all rows from the first dataset and matching rows from the second dataset.",
    "right": "Includes all rows from the second dataset and matching rows from the first dataset.",
    "full": "Includes all rows from both datasets, whether matching records exist or not.",
}

PANDAS_MERGE_TYPE_MAP = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "outer",
}

PREFERRED_JOIN_COLUMN_NAMES = {
    "id",
    "email",
    "email_id",
    "user_id",
    "customer_id",
    "phone",
    "mobile",
}


def _column_lookup_key(column_name: Any) -> str:
    return str(column_name).strip().lower()


def _column_lookup(dataset: CsvUploadedDataset) -> dict[str, tuple[str, str]]:
    lookup = {}
    for original_column, internal_column in zip(dataset.columns, dataset.internal_columns):
        lookup[_column_lookup_key(original_column)] = (original_column, internal_column)
        lookup[_column_lookup_key(internal_column)] = (original_column, internal_column)
    return lookup


def _resolve_dataset_column(
    dataset: CsvUploadedDataset,
    column_name: str,
    lookup: dict[str, tuple[str, str]] | None = None,
) -> tuple[str, str]:
    resolved = (lookup or _column_lookup(dataset)).get(_column_lookup_key(column_name))
    if resolved is None:
        raise error_response(
            status_code=400,
            detail=f"{column_name} was not found in {dataset.name}",
        )
    return resolved


def _find_default_join_column_mapping(source_datasets: list[CsvUploadedDataset]) -> dict[str, str] | None:
    if len(source_datasets) != 2:
        raise error_response(
            status_code=400,
            detail="Smart joins support exactly two uploaded datasets",
        )

    left_dataset, right_dataset = source_datasets
    right_lookup = _column_lookup(right_dataset)
    fallback_mapping = None

    for left_original, left_internal in zip(left_dataset.columns, left_dataset.internal_columns):
        normalized_name = _column_lookup_key(left_original)
        right_match = right_lookup.get(normalized_name)
        if right_match is None:
            right_match = right_lookup.get(_column_lookup_key(left_internal))
        if right_match is None:
            continue

        right_original, _ = right_match
        mapping = {
            "left_column": left_original,
            "right_column": right_original,
        }
        if normalized_name in PREFERRED_JOIN_COLUMN_NAMES:
            return mapping
        if fallback_mapping is None:
            fallback_mapping = mapping

    return fallback_mapping


def _build_join_output_columns(
    left_dataset: CsvUploadedDataset,
    right_dataset: CsvUploadedDataset,
    right_join_internal_columns: set[str],
) -> tuple[list[str], list[str], dict[str, str], dict[str, str]]:
    output_columns = list(left_dataset.columns)
    output_internal_columns = list(left_dataset.internal_columns)
    left_output_mapping = dict(zip(left_dataset.internal_columns, left_dataset.internal_columns))
    right_output_mapping = {}
    used_internal_columns = set(output_internal_columns)
    used_display_columns = set(output_columns)

    for original_column, internal_column in zip(right_dataset.columns, right_dataset.internal_columns):
        if internal_column in right_join_internal_columns:
            continue

        display_column = original_column
        if display_column in used_display_columns:
            display_column = f"{original_column} ({right_dataset.name})"

        output_internal_column = internal_column
        if output_internal_column in used_internal_columns:
            base_column = f"{internal_column}_{right_dataset.id}"
            output_internal_column = base_column
            counter = 2
            while output_internal_column in used_internal_columns:
                output_internal_column = f"{base_column}_{counter}"
                counter += 1

        used_display_columns.add(display_column)
        used_internal_columns.add(output_internal_column)
        output_columns.append(display_column)
        output_internal_columns.append(output_internal_column)
        right_output_mapping[internal_column] = output_internal_column

    return output_columns, output_internal_columns, left_output_mapping, right_output_mapping


def _build_temporary_column_names(prefix: str, count: int, used_columns: set[str]) -> list[str]:
    columns = []
    next_index = 0
    while len(columns) < count:
        column = f"{prefix}_{next_index}"
        next_index += 1
        if column in used_columns:
            continue
        used_columns.add(column)
        columns.append(column)
    return columns


def _build_joined_rows_with_pandas(
    *,
    left_rows: list[dict],
    right_rows: list[dict],
    left_columns: list[str],
    left_join_columns: list[str],
    right_join_columns: list[str],
    left_output_mapping: dict[str, str],
    right_output_mapping: dict[str, str],
    merge_type: str,
) -> list[dict]:
    right_columns = list(right_join_columns) + list(right_output_mapping.keys())
    right_renames = {
        source_column: output_column
        for source_column, output_column in right_output_mapping.items()
        if source_column != output_column
    }
    right_output_columns = [
        right_renames.get(column, column)
        for column in right_output_mapping
    ]

    left_df = pd.DataFrame(left_rows, columns=left_columns).rename(columns=left_output_mapping)
    right_df = pd.DataFrame(right_rows, columns=right_columns).rename(columns=right_renames)
    used_columns = set(left_df.columns).union(right_df.columns)
    left_merge_columns = _build_temporary_column_names("__left_join", len(left_join_columns), used_columns)
    right_merge_columns = _build_temporary_column_names("__right_join", len(right_join_columns), used_columns)
    for source_column, merge_column in zip(left_join_columns, left_merge_columns):
        left_df[merge_column] = left_df[left_output_mapping[source_column]]
    for source_column, merge_column in zip(right_join_columns, right_merge_columns):
        right_df[merge_column] = right_df[right_renames.get(source_column, source_column)]
    right_df = right_df.drop(
        columns=[
            right_renames.get(column, column)
            for column in right_join_columns
        ],
        errors="ignore",
    )

    merged_df = pd.merge(
        left_df,
        right_df,
        how=PANDAS_MERGE_TYPE_MAP[merge_type],
        left_on=left_merge_columns,
        right_on=right_merge_columns,
    )

    for left_column, right_column in zip(
        [left_output_mapping[column] for column in left_join_columns],
        right_merge_columns,
    ):
        if left_column != right_column and right_column in merged_df.columns:
            merged_df[left_column] = merged_df[left_column].combine_first(merged_df[right_column])

    output_columns = [
        left_output_mapping[column]
        for column in left_columns
    ] + right_output_columns
    merged_df = merged_df.reindex(columns=output_columns)
    merged_df = merged_df.astype(object).where(pd.notna(merged_df), None)
    return merged_df.to_dict(orient="records")


def _build_merged_dataset_record(
    *,
    merged_name: str,
    table_name: str,
    storage_key: str,
    file_url: str,
    file_size: int,
    user_id: int,
    source_datasets: list[CsvUploadedDataset],
    columns: list[str],
    internal_columns: list[str],
    rows: list[dict],
    total_rows: int,
) -> CsvMergedDataset:
    return CsvMergedDataset(
        name=_normalize_dataset_name(merged_name),
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        file_size=file_size,
        created_by_user_id=user_id,
        source_datasets_metadata=[
            {
                "id": dataset.id,
                "name": dataset.name,
                "file_name": dataset.file_name,
                "sheet_name": dataset.sheet_name,
                "table_name": dataset.table_name,
                "columns": dataset.columns,
                "internal_columns": dataset.internal_columns,
                "total_rows": dataset.total_rows,
                "metadata": {
                    "file_size": dataset.file_size,
                    "storage_key": dataset.storage_key,
                    "file_url": dataset.file_url,
                    "created_at": dataset.created_at.isoformat() if dataset.created_at else None,
                },
            }
            for dataset in source_datasets
        ],
        columns=columns,
        internal_columns=internal_columns,
        column_types=build_column_types(columns, internal_columns, rows),
        total_rows=total_rows,
    )


def create_uploaded_dataset(
    db: Session,
    *,
    dataset_name: str,
    file_name: str,
    sheet_name: str | None = None,
    file_size: int,
    columns: list[str],
    internal_columns: list[str],
    rows: list[dict],
    user_id: int,
) -> CsvUploadedDataset:
    table_name = _generate_table_name(db, "upload", dataset_name)
    storage_key, file_url, _ = _upload_rows_to_object_storage(
        table_name=table_name,
        columns=internal_columns,
        rows=rows,
    )

    dataset = CsvUploadedDataset(
        name=dataset_name,
        file_name=file_name,
        sheet_name=sheet_name,
        file_size=file_size,
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        created_by_user_id=user_id,
        total_rows=len(rows),
        columns=columns,
        internal_columns=internal_columns,
        column_types=build_column_types(columns, internal_columns, rows),
        created_at=datetime.utcnow(),
    )
    db.add(dataset)

    return dataset


def merge_uploaded_datasets(
    db: Session,
    *,
    merged_name: str,
    source_datasets: list[CsvUploadedDataset],
    user_id: int,
    merge_type: str | None = None,
    join_columns: list[dict] | None = None,
) -> CsvMergedDataset:
    if merge_type is not None:
        return _join_uploaded_datasets(
            db,
            merged_name=merged_name,
            source_datasets=source_datasets,
            user_id=user_id,
            merge_type=merge_type,
            join_columns=join_columns,
        )

    ordered_columns, column_mappings = _build_merge_column_mappings(source_datasets)
    table_name = _generate_table_name(db, "merged", merged_name)

    merged_rows: list[dict] = []
    for dataset in source_datasets:
        dataset_rows = _fetch_dataset_rows(
            table_name=dataset.table_name,
            columns=dataset.internal_columns,
            storage_key=dataset.storage_key,
        )
        dataset_column_mapping = column_mappings[dataset.id]
        for row in dataset_rows:
            merged_rows.append(
                {
                    column: row.get(dataset_column_mapping[column])
                    for column in ordered_columns
                }
            )

    storage_key, file_url, file_size = _upload_rows_to_object_storage(
        table_name=table_name,
        columns=ordered_columns,
        rows=merged_rows,
    )

    merged_dataset = _build_merged_dataset_record(
        merged_name=merged_name,
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        file_size=file_size,
        user_id=user_id,
        source_datasets=source_datasets,
        columns=list(source_datasets[0].columns),
        internal_columns=ordered_columns,
        rows=merged_rows,
        total_rows=len(merged_rows),
    )
    db.add(merged_dataset)

    return merged_dataset


def _join_uploaded_datasets(
    db: Session,
    *,
    merged_name: str,
    source_datasets: list[CsvUploadedDataset],
    user_id: int,
    merge_type: str,
    join_columns: list[dict] | None,
) -> CsvMergedDataset:
    if merge_type not in MERGE_TYPE_INFO:
        raise error_response(
            status_code=400,
            detail="Merge type must be one of: inner, left, right, full",
        )
    if len(source_datasets) != 2:
        raise error_response(
            status_code=400,
            detail="Smart joins support exactly two uploaded datasets",
        )

    if not join_columns:
        default_mapping = _find_default_join_column_mapping(source_datasets)
        if default_mapping is None:
            raise error_response(
                status_code=400,
                detail="No common columns were found. Please select join columns manually.",
            )
        join_columns = [default_mapping]

    left_dataset, right_dataset = source_datasets
    left_lookup = _column_lookup(left_dataset)
    right_lookup = _column_lookup(right_dataset)
    left_join_columns = []
    right_join_columns = []
    for mapping in join_columns:
        left_column = mapping["left_column"] if isinstance(mapping, dict) else mapping.left_column
        right_column = mapping["right_column"] if isinstance(mapping, dict) else mapping.right_column
        _, left_internal_column = _resolve_dataset_column(left_dataset, left_column, left_lookup)
        _, right_internal_column = _resolve_dataset_column(right_dataset, right_column, right_lookup)
        left_join_columns.append(left_internal_column)
        right_join_columns.append(right_internal_column)

    left_rows = _fetch_dataset_rows(
        table_name=left_dataset.table_name,
        columns=left_dataset.internal_columns,
        storage_key=left_dataset.storage_key,
    )
    right_rows = _fetch_dataset_rows(
        table_name=right_dataset.table_name,
        columns=right_dataset.internal_columns,
        storage_key=right_dataset.storage_key,
    )

    (
        output_columns,
        output_internal_columns,
        left_output_mapping,
        right_output_mapping,
    ) = _build_join_output_columns(left_dataset, right_dataset, set(right_join_columns))
    merged_rows = _build_joined_rows_with_pandas(
        left_rows=left_rows,
        right_rows=right_rows,
        left_columns=left_dataset.internal_columns,
        left_join_columns=left_join_columns,
        right_join_columns=right_join_columns,
        left_output_mapping=left_output_mapping,
        right_output_mapping=right_output_mapping,
        merge_type=merge_type,
    )

    table_name = _generate_table_name(db, "merged", merged_name)
    storage_key, file_url, file_size = _upload_rows_to_object_storage(
        table_name=table_name,
        columns=output_internal_columns,
        rows=merged_rows,
    )

    merged_dataset = _build_merged_dataset_record(
        merged_name=merged_name,
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        file_size=file_size,
        user_id=user_id,
        source_datasets=source_datasets,
        columns=output_columns,
        internal_columns=output_internal_columns,
        rows=merged_rows,
        total_rows=len(merged_rows),
    )
    db.add(merged_dataset)

    return merged_dataset


def delete_uploaded_dataset(
    *,
    dataset: CsvUploadedDataset,
) -> None:
    _delete_dataset_rows(table_name=dataset.table_name, storage_key=dataset.storage_key)
    session = object_session(dataset)
    if session is None:
        raise error_response(status_code=500, detail="Dataset session is not available")
    session.delete(dataset)


def delete_merged_dataset(
    *,
    dataset: CsvMergedDataset,
) -> None:
    _delete_dataset_rows(table_name=dataset.table_name, storage_key=dataset.storage_key)
    session = object_session(dataset)
    if session is None:
        raise error_response(status_code=500, detail="Dataset session is not available")
    session.delete(dataset)
