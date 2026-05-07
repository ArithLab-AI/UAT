from typing import Any

from sqlalchemy.orm import Session

from app.models.csv_dataset_models import CsvMergedDataset, CsvUploadedDataset
from app.services.csv_service import (
    MERGE_TYPE_INFO,
    PANDAS_MERGE_TYPE_MAP,
    _build_join_output_columns,
    _build_joined_rows_with_pandas,
    _build_merged_dataset_record,
    _fetch_dataset_rows,
    _generate_table_name,
    _normalize_dataset_name,
    _upload_rows_to_object_storage,
)
from app.utils.responses import error_response

def validate_source_dataset_ids(source_dataset_ids: list[int]) -> None:
    if len(source_dataset_ids) != 2:
        raise error_response(
            status_code=400,
            detail="Exactly two source dataset IDs are required",
        )
    if len(set(source_dataset_ids)) != len(source_dataset_ids):
        raise error_response(
            status_code=400,
            detail="Source dataset IDs must be unique",
        )


def validate_merge_type(merge_type: str) -> None:
    if merge_type not in MERGE_TYPE_INFO:
        raise error_response(
            status_code=400,
            detail="Merge type must be one of: inner, left, right, full",
        )
    if merge_type not in PANDAS_MERGE_TYPE_MAP:
        raise error_response(status_code=400, detail="Merge type is not supported by Pandas")


def get_ordered_uploaded_datasets(
    db: Session,
    *,
    source_ids: list[int],
    user_id: int,
) -> list[CsvUploadedDataset]:
    validate_source_dataset_ids(source_ids)
    fetched_datasets = (
        db.query(CsvUploadedDataset)
        .filter(
            CsvUploadedDataset.created_by_user_id == user_id,
            CsvUploadedDataset.id.in_(source_ids),
        )
        .all()
    )

    if len(fetched_datasets) != len(source_ids):
        raise error_response(
            status_code=404,
            detail="One or more source dataset IDs were not found",
        )

    source_dataset_map = {dataset.id: dataset for dataset in fetched_datasets}
    return [source_dataset_map[source_id] for source_id in source_ids]


def dataset_info(dataset: CsvUploadedDataset) -> dict:
    return {
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
            "created_at": dataset.created_at,
            "is_retention": dataset.is_retention,
            "retention_until": dataset.retention_until,
            "retention_at": dataset.retention_at,
        },
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


def _mapping_value(mapping: Any, key: str) -> str:
    if isinstance(mapping, dict):
        return mapping[key]
    return getattr(mapping, key)


def resolve_join_columns(
    source_datasets: list[CsvUploadedDataset],
    join_columns: list[Any] | None,
) -> tuple[list[str], list[str]]:
    if len(source_datasets) != 2:
        raise error_response(
            status_code=400,
            detail="Merge preview and smart joins support exactly two uploaded datasets",
        )
    if not join_columns:
        raise error_response(status_code=400, detail="At least one join column is required")

    left_dataset, right_dataset = source_datasets
    left_lookup = _column_lookup(left_dataset)
    right_lookup = _column_lookup(right_dataset)
    left_join_columns = []
    right_join_columns = []

    for mapping in join_columns:
        left_column = _mapping_value(mapping, "left_column")
        right_column = _mapping_value(mapping, "right_column")
        if not left_column.strip() or not right_column.strip():
            raise error_response(status_code=400, detail="Join columns cannot be empty")
        _, left_internal_column = _resolve_dataset_column(left_dataset, left_column, left_lookup)
        _, right_internal_column = _resolve_dataset_column(right_dataset, right_column, right_lookup)
        left_join_columns.append(left_internal_column)
        right_join_columns.append(right_internal_column)

    return left_join_columns, right_join_columns


def build_merged_rows(
    *,
    source_datasets: list[CsvUploadedDataset],
    merge_type: str,
    join_columns: list[Any],
) -> tuple[list[str], list[str], list[dict]]:
    validate_merge_type(merge_type)
    left_join_columns, right_join_columns = resolve_join_columns(source_datasets, join_columns)
    left_dataset, right_dataset = source_datasets

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
    return output_columns, output_internal_columns, merged_rows


def rows_with_display_columns(
    rows: list[dict],
    *,
    columns: list[str],
    internal_columns: list[str],
) -> list[dict]:
    return [
        {
            display_column: row.get(internal_column)
            for display_column, internal_column in zip(columns, internal_columns)
        }
        for row in rows
    ]


def merge_uploaded_datasets(
    db: Session,
    *,
    merged_name: str,
    source_datasets: list[CsvUploadedDataset],
    user_id: int,
    merge_type: str,
    join_columns: list[Any],
) -> CsvMergedDataset:
    output_columns, output_internal_columns, merged_rows = build_merged_rows(
        source_datasets=source_datasets,
        merge_type=merge_type,
        join_columns=join_columns,
    )

    table_name = _generate_table_name(db, "merged", _normalize_dataset_name(merged_name))
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
        total_rows=len(merged_rows),
    )
    db.add(merged_dataset)
    return merged_dataset
