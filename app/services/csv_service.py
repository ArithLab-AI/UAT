import csv
import io
import re
from pathlib import Path

from fastapi import HTTPException, UploadFile
from sqlalchemy import Column, Integer, MetaData, Table, Text, inspect, select
from sqlalchemy.orm import Session

from app.models.csv_dataset_models import (
    CsvMergedDataset,
    CsvUploadedDataset,
)


def _normalize_dataset_name(name: str) -> str:
    cleaned_name = " ".join(name.strip().split())
    if not cleaned_name:
        raise HTTPException(status_code=400, detail="Dataset name cannot be empty")
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


async def parse_csv_upload(file: UploadFile) -> tuple[str, list[str], list[dict]]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail=f"{file.filename} is empty")

    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{file.filename} must be UTF-8 encoded",
        ) from exc

    csv_reader = csv.DictReader(io.StringIO(text_content))
    columns = csv_reader.fieldnames

    if not columns:
        raise HTTPException(
            status_code=400,
            detail=f"{file.filename} does not contain a header row",
        )

    normalized_columns = [column.strip() for column in columns if column and column.strip()]
    if len(normalized_columns) != len(columns):
        raise HTTPException(
            status_code=400,
            detail=f"{file.filename} contains blank column names",
        )
    if len(set(normalized_columns)) != len(normalized_columns):
        raise HTTPException(
            status_code=400,
            detail=f"{file.filename} contains duplicate column names",
        )

    rows = []
    for row in csv_reader:
        cleaned_row = {
            normalized_columns[column_index]: (
                row.get(original_column, "").strip()
                if isinstance(row.get(original_column), str)
                else row.get(original_column)
            )
            for column_index, original_column in enumerate(columns)
        }
        rows.append(cleaned_row)

    await file.close()
    return file.filename, normalized_columns, rows


def build_dataset_name(explicit_name: str | None, file_name: str) -> str:
    if explicit_name:
        return _normalize_dataset_name(explicit_name)
    return _normalize_dataset_name(Path(file_name).stem.replace("_", " ").replace("-", " "))


def create_uploaded_dataset(
    db: Session,
    *,
    dataset_name: str,
    file_name: str,
    columns: list[str],
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
    )
    db.add(dataset)
    db.flush()
    create_physical_csv_table(
        db,
        table_name=dataset.table_name,
        columns=columns,
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
    ordered_columns: list[str] = []
    for dataset in source_datasets:
        for column in dataset.columns:
            if column not in ordered_columns:
                ordered_columns.append(column)

    merged_dataset = CsvMergedDataset(
        name=_normalize_dataset_name(merged_name),
        table_name=_generate_table_name(db, "merged", merged_name),
        created_by_user_id=user_id,
        source_dataset_ids=[dataset.id for dataset in source_datasets],
        columns=ordered_columns,
        total_rows=0,
    )
    db.add(merged_dataset)
    db.flush()

    merged_rows: list[dict] = []
    for dataset in source_datasets:
        dataset_rows = fetch_table_rows(
            db,
            table_name=dataset.table_name,
            columns=dataset.columns,
        )
        for row in dataset_rows:
            merged_rows.append({column: row.get(column) for column in ordered_columns})

    create_physical_csv_table(
        db,
        table_name=merged_dataset.table_name,
        columns=ordered_columns,
        rows=merged_rows,
    )
    merged_dataset.total_rows = len(merged_rows)
    return merged_dataset
