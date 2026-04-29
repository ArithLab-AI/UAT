import csv
import io
import re
import os
import tempfile
from contextlib import contextmanager
from typing import Any
from datetime import date, datetime, time
from pathlib import Path, PureWindowsPath
import openpyxl
import xlrd
from fastapi import UploadFile
from sqlalchemy.orm import Session, object_session
from app.models.csv_dataset_models import (
    CsvMergedDataset,
    CsvUploadedDataset,
)
from app.utils.object_storage import get_object_storage_service
from app.utils.responses import error_response


def _normalize_dataset_name(name: str) -> str:
    cleaned_name = " ".join(name.strip().split())
    if not cleaned_name:
        raise error_response(status_code=400, detail="Dataset name cannot be empty")
    return cleaned_name


def _build_table_slug(name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    return slug or "dataset"


def _generate_table_name(db: Session, prefix: str, dataset_name: str) -> str:
    base_slug = _build_table_slug(dataset_name)
    candidate = f"{prefix}_{base_slug}"[:55].rstrip("_")
    table_name = candidate
    counter = 1

    while db.query(CsvUploadedDataset.id).filter(CsvUploadedDataset.table_name == table_name).first() or db.query(
        CsvMergedDataset.id
    ).filter(CsvMergedDataset.table_name == table_name).first():
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
            return [
                {column: row.get(column) for column in columns}
                for row in reader
            ]


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
    columns = next(csv_reader, None)
    original_columns, internal_columns = _normalize_columns(file_name, columns or [])

    rows = []
    for row in csv_reader:
        cleaned_row = {
            internal_columns[column_index]: _normalize_scalar_value(
                row[column_index] if column_index < len(row) else ""
            )
            for column_index in range(len(internal_columns))
        }
        rows.append(cleaned_row)

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

    iterator = sheet.iter_rows(values_only=True)
    header_row = next(iterator, None)
    original_columns, internal_columns = _normalize_columns(file_name, list(header_row) if header_row is not None else [])

    rows = []
    for row in iterator:
        row_values = list(row or [])
        cleaned_row = {
            internal_columns[index]: _normalize_scalar_value(
                row_values[index] if index < len(row_values) else None
            )
            for index in range(len(internal_columns))
        }
        rows.append(cleaned_row)

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
    header_row = sheet.row_values(0) if sheet.nrows else []
    original_columns, internal_columns = _normalize_columns(file_name, header_row)

    rows = []
    for row_index in range(1, sheet.nrows):
        row_values = sheet.row_values(row_index)
        cleaned_row = {
            internal_columns[index]: _normalize_scalar_value(
                row_values[index] if index < len(row_values) else None
            )
            for index in range(len(internal_columns))
        }
        rows.append(cleaned_row)

    return original_columns, internal_columns, rows


async def parse_csv_upload(file: UploadFile) -> tuple[str, int, list[str], list[str], list[dict]]:
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
            original_columns, internal_columns, rows = _parse_csv_content(file_name, content)
        elif suffix == ".xlsx":
            original_columns, internal_columns, rows = _parse_xlsx_content(file_name, content)
        else:
            original_columns, internal_columns, rows = _parse_xls_content(file_name, content)

        return file_name, file_size, original_columns, internal_columns, rows
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


def create_uploaded_dataset(
    db: Session,
    *,
    dataset_name: str,
    file_name: str,
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
        file_size=file_size,
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        created_by_user_id=user_id,
        total_rows=len(rows),
        columns=columns,
        internal_columns=internal_columns,
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
) -> CsvMergedDataset:
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

    merged_dataset = CsvMergedDataset(
        name=_normalize_dataset_name(merged_name),
        table_name=table_name,
        storage_key=storage_key,
        file_url=file_url,
        file_size=file_size,
        created_by_user_id=user_id,
        source_datasets_metadata=[
            {"id": dataset.id, "file_name": dataset.file_name}
            for dataset in source_datasets
        ],
        columns=list(source_datasets[0].columns),
        internal_columns=ordered_columns,
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
