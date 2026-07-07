import io
from pathlib import Path
from typing import Any

import openpyxl
import pandas as pd
from fastapi import HTTPException

from app.services.csv_service import (
    _build_row_from_values,
    _clean_upload_file_name,
    _normalize_scalar_value,
    _split_rows_by_detected_header,
)
from app.services.temporary_upload_service import TemporaryUpload
from app.utils.responses import error_response

EXCEL_EXTENSIONS = {".xlsx", ".xls"}
MAX_EXCEL_SHEET_ROWS = 600_000


def _is_excel_file(file_name: str) -> bool:
    return Path(file_name).suffix.lower() in EXCEL_EXTENSIONS


def extract_sheet_names(file_content: bytes | str) -> list[str]:
    workbook = None
    try:
        workbook = openpyxl.load_workbook(
            filename=file_content if isinstance(file_content, str) else io.BytesIO(file_content),
            read_only=True,
            data_only=True,
        )
        sheet_names = list(workbook.sheetnames)
    except Exception:
        sheet_names = []
    finally:
        if workbook is not None:
            workbook.close()

    if sheet_names:
        return sheet_names
    if workbook is not None:
        if not sheet_names:
            raise error_response(status_code=400, detail="Excel file does not contain any sheets")

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

    original_columns, internal_columns, data_rows = _split_rows_by_detected_header(
        file_name,
        dataframe.astype(object).where(pd.notna(dataframe), None).values.tolist(),
    )
    if not internal_columns:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet is empty")
    if not data_rows:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet does not contain data rows")

    rows: list[dict[str, Any]] = []
    for values in data_rows:
        rows.append(
            {
                column: _normalize_scalar_value(values[index] if index < len(values) else None)
                for index, column in enumerate(internal_columns)
            }
        )

    return original_columns, internal_columns, rows


def _process_xlsx_selected_sheet(
    *,
    file_path: str,
    file_name: str,
    sheet_name: str,
) -> tuple[list[str], list[str], list[dict]]:
    workbook = None
    try:
        workbook = openpyxl.load_workbook(
            filename=file_path,
            read_only=True,
            data_only=True,
        )
        sheet = workbook[sheet_name]
        sheet_rows = []
        for row_count, row in enumerate(sheet.iter_rows(values_only=True), start=1):
            if row_count > MAX_EXCEL_SHEET_ROWS + 25:
                raise error_response(
                    status_code=400,
                    detail=f"{sheet_name} exceeds the maximum supported row count of {MAX_EXCEL_SHEET_ROWS}",
                )
            sheet_rows.append(list(row or []))

        columns, internal_columns, data_rows = _split_rows_by_detected_header(
            file_name,
            sheet_rows,
        )

        rows = []
        for row_count, row in enumerate(data_rows, start=1):
            if row_count > MAX_EXCEL_SHEET_ROWS:
                raise error_response(
                    status_code=400,
                    detail=f"{sheet_name} exceeds the maximum supported row count of {MAX_EXCEL_SHEET_ROWS}",
                )
            rows.append(_build_row_from_values(internal_columns, list(row or [])))
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise error_response(status_code=400, detail=f"{sheet_name} could not be read") from exc
    finally:
        if workbook is not None:
            workbook.close()

    if not rows:
        raise error_response(status_code=400, detail=f"{file_name} selected sheet does not contain data rows")

    return columns, internal_columns, rows


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

    display_file_name = f"{clean_file_name} ({sheet_name})"
    if Path(clean_file_name).suffix.lower() == ".xlsx":
        columns, internal_columns, rows = _process_xlsx_selected_sheet(
            file_path=file_path,
            file_name=display_file_name,
            sheet_name=sheet_name,
        )
    else:
        try:
            dataframe = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=None,
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
            file_name=display_file_name,
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
