from typing import Any

from app.models.csv_dataset_models import CsvUploadedDataset
from app.services.merge_service import build_merged_rows, rows_with_display_columns

PREVIEW_ROW_LIMIT = 10


def preview_uploaded_dataset_merge(
    *,
    source_datasets: list[CsvUploadedDataset],
    merge_type: str | None,
    join_columns: list[Any] | None,
) -> dict:
    output_columns, output_internal_columns, merged_rows = build_merged_rows(
        source_datasets=source_datasets,
        merge_type=merge_type,
        join_columns=join_columns,
    )
    preview_rows = rows_with_display_columns(
        merged_rows[:PREVIEW_ROW_LIMIT],
        columns=output_columns,
        internal_columns=output_internal_columns,
    )
    return {
        "merged_columns": output_columns,
        "preview_rows": preview_rows,
        "preview_row_count": len(preview_rows),
    }
