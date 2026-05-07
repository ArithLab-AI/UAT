import re

from app.models.csv_dataset_models import CsvUploadedDataset
from app.services.csv_service import MERGE_TYPE_INFO
from app.services.merge_service import dataset_info
from app.utils.responses import error_response

COMMON_JOIN_COLUMNS = {
    "id",
    "email",
    "customer_id",
    "phone",
}

JOIN_COLUMN_ALIASES = COMMON_JOIN_COLUMNS | {
    "email_id",
    "user_id",
    "client_id",
    "account_id",
    "mobile",
    "mobile_number",
    "phone_number",
}


def _case_key(column_name: str) -> str:
    return str(column_name).strip().lower()


def _compact_key(column_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _case_key(column_name))


COMPACT_JOIN_COLUMN_ALIASES = {_compact_key(alias) for alias in JOIN_COLUMN_ALIASES}


def _confidence(left_column: str, right_column: str) -> str:
    left_key = _case_key(left_column)
    right_key = _case_key(right_column)
    left_compact_key = _compact_key(left_column)
    right_compact_key = _compact_key(right_column)
    if left_key == right_key and left_key in JOIN_COLUMN_ALIASES:
        return "high"
    if left_key == right_key:
        return "high"
    if (
        left_compact_key == right_compact_key
        and left_compact_key in COMPACT_JOIN_COLUMN_ALIASES
    ):
        return "high"
    if left_compact_key == right_compact_key:
        return "medium"
    return "low"


def suggest_join_columns(
    *,
    source_datasets: list[CsvUploadedDataset],
) -> dict:
    if len(source_datasets) != 2:
        raise error_response(
            status_code=400,
            detail="Merge suggestions support exactly two uploaded datasets",
        )

    left_dataset, right_dataset = source_datasets
    suggestions = []
    seen_pairs = set()

    for left_column in left_dataset.columns:
        for right_column in right_dataset.columns:
            columns_match = (
                _case_key(left_column) == _case_key(right_column)
                or _compact_key(left_column) == _compact_key(right_column)
            )
            if columns_match:
                pair_key = (_case_key(left_column), _case_key(right_column))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                suggestions.append(
                    {
                        "left_column": left_column,
                        "right_column": right_column,
                        "confidence": _confidence(left_column, right_column),
                    }
                )

    suggestions.sort(
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}[item["confidence"]],
            _case_key(item["left_column"]) not in COMMON_JOIN_COLUMNS,
            item["left_column"].lower(),
        )
    )

    return {
        "left_dataset": dataset_info(left_dataset),
        "right_dataset": dataset_info(right_dataset),
        "suggested_join_columns": suggestions,
        "supported_merge_types": list(MERGE_TYPE_INFO.keys()),
        "merge_type_info": MERGE_TYPE_INFO,
    }
