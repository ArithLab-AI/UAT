import re


_TITLE_BY_PROMPT_TYPE = {
    "age_normalization": "Invalid Age Values",
    "boolean_validation": "Invalid Boolean Values",
    "date_normalization": "Date Format Issues",
    "duplicate_removal": "Duplicate Rows",
    "float_validation": "Invalid Float Values",
    "header_type_normalization": "Column Type Mismatch",
    "integer_validation": "Invalid Integer Values",
    "missing_value_normalization": "Missing Values",
    "numeric_normalization": "Numeric Formatting Issues",
    "phone_normalization": "Phone Format Issues",
    "text_normalization": "Text Formatting Issues",
}

_TITLE_BY_KEYWORD = (
    ("duplicate", "Duplicate Rows"),
    ("missing", "Missing Values"),
    ("blank", "Missing Values"),
    ("placeholder", "Missing Values"),
    ("date", "Date Format Issues"),
    ("phone", "Phone Format Issues"),
    ("email", "Invalid Email Values"),
    ("boolean", "Invalid Boolean Values"),
    ("float", "Invalid Float Values"),
    ("integer", "Invalid Integer Values"),
    ("numeric", "Numeric Formatting Issues"),
    ("age", "Invalid Age Values"),
    ("whitespace", "Text Formatting Issues"),
    ("casing", "Text Formatting Issues"),
    ("text/string type", "Column Type Mismatch"),
)

_LEADING_CONTEXT_PATTERN = re.compile(
    r"^(?:the sampled dataset|(?:moderate-cardinality )?(?:boolean-like|phone-like|date-like|integer-like|"
    r"float-like|age-like|text-like|numeric-like)?\s*column\(s\)|column\(s\))\s+.+?\s+"
    r"(?:contain|contains|have|has|show|shows)\s+",
    re.IGNORECASE,
)


def build_suggestion_title(*, cleaning_prompt_type: str | None, issue_description: str) -> str:
    prompt_type = str(cleaning_prompt_type or "").strip().lower()
    if prompt_type in _TITLE_BY_PROMPT_TYPE:
        return _TITLE_BY_PROMPT_TYPE[prompt_type]

    normalized_description = re.sub(r"\s+", " ", str(issue_description or "")).strip()
    lowered_description = normalized_description.lower()

    for keyword, title in _TITLE_BY_KEYWORD:
        if keyword in lowered_description:
            return title

    normalized_description = _LEADING_CONTEXT_PATTERN.sub("", normalized_description)
    normalized_description = re.split(r"[.;:]", normalized_description, maxsplit=1)[0].strip()
    if not normalized_description:
        return "Data Quality Issue"

    words = normalized_description.split()
    short_title = " ".join(words[:6])
    return short_title[:1].upper() + short_title[1:]
