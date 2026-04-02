import csv
import io
import re
from typing import Any
from datetime import date, datetime, time
from pathlib import Path
import openpyxl
import xlrd
from fastapi import UploadFile
from sqlalchemy import Column, Integer, MetaData, Table, Text, inspect, select
from sqlalchemy.orm import Session
from app.models.csv_dataset_models import (
    CsvMergedDataset,
    CsvUploadedDataset,
)
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
    inspector = inspect(db.bind)
    base_slug = _build_table_slug(dataset_name)
    candidate = f"{prefix}_{base_slug}"[:55].rstrip("_")
    table_name = candidate
    counter = 1

    while inspector.has_table(table_name):
        suffix = f"_{counter}"
        table_name = f"{candidate[: 63 - len(suffix)]}{suffix}"
        counter += 1

    return table_name


def _build_physical_table(table_name: str, columns: list[str]) -> Table:
    metadata = MetaData()
    return Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        *[Column(column, Text, nullable=True) for column in columns],
    )


def create_physical_csv_table(
    db: Session,
    *,
    table_name: str,
    columns: list[str],
    rows: list[dict],
) -> None:
    dynamic_table = _build_physical_table(table_name, columns)
    dynamic_table.create(bind=db.bind)

    if rows:
        db.execute(dynamic_table.insert(), rows)


def fetch_table_rows(
    db: Session,
    *,
    table_name: str,
    columns: list[str],
) -> list[dict]:
    reflected_table = Table(table_name, MetaData(), autoload_with=db.bind)
    result = db.execute(select(*[reflected_table.c[column] for column in columns]))
    return [dict(row._mapping) for row in result]


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


def _parse_csv_content(file_name: str, content: bytes) -> tuple[list[str], list[str], list[dict]]:
    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise error_response(
            status_code=400,
            detail=f"{file_name} must be UTF-8 encoded",
        ) from exc

    csv_reader = csv.reader(io.StringIO(text_content))
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


async def parse_csv_upload(file: UploadFile) -> tuple[str, list[str], list[str], list[dict]]:
    if not file.filename:
        raise error_response(status_code=400, detail="Uploaded file must have a name")

    suffix = Path(file.filename).suffix.lower()
    allowed_extensions = {".csv", ".xlsx", ".xls"}
    if suffix not in allowed_extensions:
        raise error_response(
            status_code=400,
            detail="Only CSV, XLSX, and XLS files are allowed",
        )

    try:
        content = await file.read()
        if not content:
            raise error_response(status_code=400, detail=f"{file.filename} is empty")

        if suffix == ".csv":
            original_columns, internal_columns, rows = _parse_csv_content(file.filename, content)
        elif suffix == ".xlsx":
            original_columns, internal_columns, rows = _parse_xlsx_content(file.filename, content)
        else:
            original_columns, internal_columns, rows = _parse_xls_content(file.filename, content)

        return file.filename, original_columns, internal_columns, rows
    finally:
        await file.close()


def build_dataset_name(explicit_name: str | None, file_name: str) -> str:
    if explicit_name:
        return _normalize_dataset_name(explicit_name)
    return _normalize_dataset_name(Path(file_name).stem.replace("_", " ").replace("-", " "))


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
    columns: list[str],
    internal_columns: list[str],
    rows: list[dict],
    user_id: int,
) -> CsvUploadedDataset:
    dataset = CsvUploadedDataset(
        name=dataset_name,
        file_name=file_name,
        table_name=_generate_table_name(db, "upload", dataset_name),
        created_by_user_id=user_id,
        total_rows=len(rows),
        columns=columns,
        internal_columns=internal_columns,
    )
    db.add(dataset)
    db.flush()
    create_physical_csv_table(
        db,
        table_name=dataset.table_name,
        columns=internal_columns,
        rows=rows,
    )

    return dataset


def merge_uploaded_datasets(
    db: Session,
    *,
    merged_name: str,
    source_datasets: list[CsvUploadedDataset],
    user_id: int,
) -> CsvMergedDataset:
    ordered_columns, column_mappings = _build_merge_column_mappings(source_datasets)

    merged_dataset = CsvMergedDataset(
        name=_normalize_dataset_name(merged_name),
        table_name=_generate_table_name(db, "merged", merged_name),
        created_by_user_id=user_id,
        source_dataset_ids=[dataset.id for dataset in source_datasets],
        columns=list(source_datasets[0].columns),
        internal_columns=ordered_columns,
        total_rows=0,
    )
    db.add(merged_dataset)
    db.flush()

    merged_rows: list[dict] = []
    for dataset in source_datasets:
        dataset_rows = fetch_table_rows(
            db,
            table_name=dataset.table_name,
            columns=dataset.internal_columns,
        )
        dataset_column_mapping = column_mappings[dataset.id]
        for row in dataset_rows:
            merged_rows.append(
                {
                    column: row.get(dataset_column_mapping[column])
                    for column in ordered_columns
                }
            )

    create_physical_csv_table(
        db,
        table_name=merged_dataset.table_name,
        columns=ordered_columns,
        rows=merged_rows,
    )
    merged_dataset.total_rows = len(merged_rows)
    return merged_dataset
