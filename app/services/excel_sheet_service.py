import io
from pathlib import Path
from typing import Any

import pandas as pd

from app.services.csv_service import (
    _clean_upload_file_name,
    _normalize_columns,
    _normalize_scalar_value,
)
from app.services.temporary_upload_service import TemporaryUpload
from app.utils.responses import error_response

EXCEL_EXTENSIONS = {".xlsx", ".xls"}
MAX_EXCEL_SHEET_ROWS = 100_000


def _is_excel_file(file_name: str) -> bool:
    return Path(file_name).suffix.lower() in EXCEL_EXTENSIONS


def extract_sheet_names(file_content: bytes | str) -> list[str]:
    try:
        excel_file = pd.ExcelFile(file_content if isinstance(file_content, str) else io.BytesIO(file_content))
        sheet_names = excel_file.sheet_names
        excel_file.close()
    except Exception as exc:
        raise error_response(status_code=400, detail="Excel file could not be read") from exc

    if not sheet_names:
        raise error_response(status_code=400, detail="Excel file does not contain any sheets")
    return sheet_names


def validate_sheet_selection(*, sheet_name: str, available_sheets: list[str]) -> None:
    if sheet_name not in available_sheets:
        raise error_response(status_code=400, detail="Selected sheet was not found in workbook")


def _dataframe_to_rows(
    *,
    file_name: str,
    dataframe: pd.DataFrame,
) -> tuple[list[str], list[str], list[dict]]:
    if dataframe.empty and len(dataframe.columns) == 0:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet is empty")

    original_columns, internal_columns = _normalize_columns(file_name, list(dataframe.columns))
    if not internal_columns:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet is empty")
    if dataframe.empty:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet does not contain data rows")

    normalized_dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)
    rows: list[dict[str, Any]] = []
    for values in normalized_dataframe.itertuples(index=False, name=None):
        rows.append(
            {
                column: _normalize_scalar_value(values[index] if index < len(values) else None)
                for index, column in enumerate(internal_columns)
            }
        )

    return original_columns, internal_columns, rows


def process_selected_sheet(
    *,
    file_path: str,
    file_name: str,
    sheet_name: str,
    available_sheets: list[str] | None = None,
) -> tuple[str, int, list[str], list[str], list[dict], str]:
    clean_file_name = _clean_upload_file_name(file_name)
    if not _is_excel_file(clean_file_name):
        raise error_response(status_code=400, detail="Only XLSX and XLS files support sheet selection")
    if available_sheets is None:
        available_sheets = extract_sheet_names(file_path)
    validate_sheet_selection(sheet_name=sheet_name, available_sheets=available_sheets)

    try:
        dataframe = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            nrows=MAX_EXCEL_SHEET_ROWS + 1,
        )
    except Exception as exc:
        raise error_response(status_code=400, detail=f"{sheet_name} could not be read") from exc

    if len(dataframe.index) > MAX_EXCEL_SHEET_ROWS:
        raise error_response(
            status_code=400,
            detail=f"{sheet_name} exceeds the maximum supported row count of {MAX_EXCEL_SHEET_ROWS}",
        )

    columns, internal_columns, rows = _dataframe_to_rows(
        file_name=f"{clean_file_name} ({sheet_name})",
        dataframe=dataframe,
    )
    file_size = Path(file_path).stat().st_size
    return clean_file_name, file_size, columns, internal_columns, rows, sheet_name


def process_temporary_upload_selection(
    *,
    upload: TemporaryUpload,
    sheet_name: str,
) -> tuple[str, int, list[str], list[str], list[dict], str]:
    return process_selected_sheet(
        file_path=upload.temp_file_path,
        file_name=upload.original_file_name,
        sheet_name=sheet_name,
        available_sheets=upload.available_sheets,
    )
