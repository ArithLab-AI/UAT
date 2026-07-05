import re
from typing import Any

import pandas as pd

EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
PHONE_DIGITS_PATTERN = re.compile(r"^\d{10,15}$")
ID_LIKE_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9\-_]{7,}$", re.IGNORECASE)

SENSITIVE_COLUMN_HINTS = (
    "name",
    "full_name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "mobile",
    "contact",
    "address",
    "street",
    "zip",
    "postal",
    "dob",
    "birth",
    "aadhaar",
    "aadhar",
    "pan",
    "passport",
    "ssn",
    "account",
    "card",
    "cvv",
    "iban",
    "swift",
    "ifsc",
    "password",
    "secret",
    "token",
    "apikey",
    "api_key",
)


def is_sensitive_column_name(column_name: str) -> bool:
    normalized = str(column_name).strip().lower()
    return any(hint in normalized for hint in SENSITIVE_COLUMN_HINTS)


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "") or pd.isna(value)


def _looks_like_phone(value: str) -> bool:
    digits = re.sub(r"\D+", "", value)
    return bool(PHONE_DIGITS_PATTERN.fullmatch(digits))


def _looks_like_numeric_identifier(value: str) -> bool:
    digits = re.sub(r"\D+", "", value)
    return len(digits) >= 8 and len(digits) == len(value.strip())


def value_looks_sensitive(value: Any) -> bool:
    if _is_empty(value):
        return False

    if isinstance(value, (int, float)):
        return False

    text = str(value).strip()
    if not text:
        return False

    if EMAIL_PATTERN.fullmatch(text):
        return True
    if _looks_like_phone(text):
        return True
    if _looks_like_numeric_identifier(text):
        return True
    # An ID-like token must actually contain a digit. Without this, ordinary 8+ letter
    # words ("Refunded", "Bengaluru", "Marketing") match the pattern and get flagged as
    # sensitive, which privacy-blocks (and silently no-ops) legitimate cleaning of plain
    # categorical/text columns like Status, City, or Job Title.
    if ID_LIKE_PATTERN.fullmatch(text) and any(char.isdigit() for char in text):
        return True
    return False


def select_llm_safe_columns(df: pd.DataFrame, ai_columns: list[str]) -> list[str]:
    safe_columns: list[str] = []
    for column in ai_columns:
        if is_sensitive_column_name(column):
            continue

        if column not in df.columns:
            continue

        series = df[column]
        sample_values = [value for value in series.head(25).tolist() if not _is_empty(value)]
        if any(value_looks_sensitive(value) for value in sample_values):
            continue

        safe_columns.append(column)

    return safe_columns
