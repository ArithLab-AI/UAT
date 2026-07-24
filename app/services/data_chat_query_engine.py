"""DuckDB-backed query engine for natural-language data chat.

The cleaned/uploaded/merged datasets live as CSV files in object storage (see
``analysis_service``). We reuse that download path, load the dataset into a pandas
DataFrame once, register it as a DuckDB table, and execute the LLM-generated
read-only SQL against it. DuckDB is used (instead of Postgres text-to-SQL) because
the row data is not stored in live SQL tables.
"""

from __future__ import annotations

import re
import threading
from typing import Any

import pandas as pd

from app.config.config import settings
from app.services.analysis_service import (
    DatasetSource,
    _analysis_chunksize,
    _build_profile_for_dataset,
    _download_dataset_file,
    _parse_csv_iter,
)
from app.utils.responses import error_response

DATASET_TABLE_NAME = "dataset"
MAX_RESULT_ROWS = 1000
MAX_PREVIEW_ROWS = 100


class QueryTimeoutError(RuntimeError):
    """Raised when a query runs longer than UAT_DATA_CHAT_TIMEOUT_SECONDS and is interrupted."""

# Only single read-only statements are allowed.
_FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|copy|pragma|"
    r"install|load|export|import|call|set|vacuum|truncate|replace)\b",
    re.IGNORECASE,
)
_STATEMENT_SPLIT = re.compile(r";\s*\S")


class SqlValidationError(ValueError):
    """Raised when the generated SQL is not a safe, single read-only query."""


def load_dataset_dataframe(source: DatasetSource) -> pd.DataFrame:
    """Download the dataset CSV from object storage and return the full DataFrame."""
    chunksize = _analysis_chunksize()
    with _download_dataset_file(source.storage_key, source.table_name) as file_path:
        chunks = list(
            _parse_csv_iter(file_path, chunksize=chunksize, preserve_placeholders=source.is_clean)
        )
    if not chunks:
        return pd.DataFrame()
    frame = pd.concat(chunks, ignore_index=True)
    return frame


def build_schema_profile(source: DatasetSource) -> dict[str, Any]:
    """Reuse the analysis profile so the LLM understands columns without being told names."""
    return _build_profile_for_dataset(source)


def validate_sql(sql: str) -> str:
    cleaned = (sql or "").strip().rstrip(";").strip()
    if not cleaned:
        raise SqlValidationError("Empty SQL statement.")

    lowered = cleaned.lstrip("(").lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise SqlValidationError("Only SELECT/WITH read-only queries are allowed.")

    if _STATEMENT_SPLIT.search(cleaned):
        raise SqlValidationError("Multiple SQL statements are not allowed.")

    if _FORBIDDEN_SQL.search(cleaned):
        raise SqlValidationError("Only read-only queries are allowed.")

    return cleaned


def run_sql(df: pd.DataFrame, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Execute validated SQL against the DataFrame using DuckDB. Returns (columns, rows)."""
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise error_response(
            status_code=500,
            detail="duckdb is required for data chat. Add 'duckdb' to requirements.",
        ) from exc

    safe_sql = validate_sql(sql)

    # Filesystem/network lockdown: DuckDB ka external access band kar dete hain, taaki
    # file-reading functions (read_csv, read_text, glob, COPY, extension load, httpfs, ...)
    # kabhi na chalein — chahe aisa SQL validate_sql se bach bhi jaye. Dataset sirf memory me
    # registered DataFrame ke roop me hai, isliye normal queries pe koi asar nahi padta.
    con = duckdb.connect(database=":memory:", config={"enable_external_access": False})

    # Resource limits (#2): cap RAM + threads so a heavy query fails cleanly instead of
    # eating the whole server, and a watchdog interrupts anything that runs too long.
    timeout_seconds = settings.UAT_DATA_CHAT_TIMEOUT_SECONDS
    con.execute(f"SET memory_limit='{settings.UAT_DATA_CHAT_MEMORY_LIMIT}'")
    con.execute(f"SET threads={settings.UAT_DATA_CHAT_MAX_THREADS}")

    timed_out = threading.Event()

    def _interrupt() -> None:
        timed_out.set()
        con.interrupt()

    watchdog = threading.Timer(timeout_seconds, _interrupt)
    watchdog.start()
    try:
        con.register(DATASET_TABLE_NAME, df)
        result_df = con.execute(safe_sql).fetch_df()
    except Exception as exc:
        if timed_out.is_set():
            raise QueryTimeoutError(
                f"Query took longer than {timeout_seconds}s and was cancelled. "
                "Please narrow it down (e.g. add filters or a smaller range)."
            ) from exc
        raise
    finally:
        watchdog.cancel()
        con.close()

    if len(result_df) > MAX_RESULT_ROWS:
        result_df = result_df.head(MAX_RESULT_ROWS)

    # Make values JSON-serialisable.
    result_df = result_df.astype(object).where(pd.notnull(result_df), None)
    columns = [str(col) for col in result_df.columns]
    rows = result_df.to_dict(orient="records")
    return columns, rows
