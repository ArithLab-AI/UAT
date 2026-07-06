"""
ArithLab Data Cleaning Engine
All 26 universal cleaning steps. Each registered via decorator.
"""
import re
import numpy as np
import pandas as pd
from typing import Any, Optional
from scipy import stats


# ═══════════════════════════════════════════════════════════════════════════════
#  STEP REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

STEP_REGISTRY: dict[str, dict] = {}


def register_step(name: str, category: str, description: str, severity: str = "high"):
    def decorator(func):
        STEP_REGISTRY[name] = {
            "name": name, "category": category,
            "description": description, "severity": severity,
            "function": func,
        }
        return func
    return decorator


# ═══════════════════════════════════════════════════════════════════════════════
#  1. Remove Duplicate Rows
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("remove_duplicates", "structure", "Remove duplicate rows", "critical")
def remove_duplicates(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    before = len(df)
    df = df.drop_duplicates(subset=columns or None).reset_index(drop=True)
    return df, {"rows_removed": before - len(df)}


# ═══════════════════════════════════════════════════════════════════════════════
#  2. Handle Missing Values
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("handle_missing_values", "structure", "Handle missing values (null/NaN)", "critical")
def handle_missing_values(df: pd.DataFrame, columns: Optional[list[str]] = None,
                          strategy: str = "fill_median", params: Optional[dict] = None, **kw):
    params = params or {}
    targets = columns or df.columns.tolist()
    details = {}

    for col in targets:
        if col not in df.columns:
            continue
        nulls = int(df[col].isna().sum())
        if nulls == 0:
            continue

        if strategy == "drop":
            df = df.dropna(subset=[col])
        elif strategy == "fill_mean" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "fill_median":
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].median())
            else:
                mode = df[col].mode()
                df[col] = df[col].fillna(mode.iloc[0] if len(mode) > 0 else "Unknown")
        elif strategy == "fill_mode":
            mode = df[col].mode()
            df[col] = df[col].fillna(mode.iloc[0] if len(mode) > 0 else "Unknown")
        elif strategy == "fill_constant":
            val = params.get("fill_value", 0 if pd.api.types.is_numeric_dtype(df[col]) else "Unknown")
            df[col] = df[col].fillna(val)
        elif strategy == "fill_forward":
            df[col] = df[col].ffill()
        elif strategy == "fill_backward":
            df[col] = df[col].bfill()
        elif strategy == "fill_knn":
            df = _fill_knn(df, col, params.get("n_neighbors", 5))
        elif strategy == "fill_regression":
            df = _fill_regression(df, col)

        details[col] = {"nulls_before": nulls, "nulls_after": int(df[col].isna().sum())}

    return df.reset_index(drop=True), {"columns_processed": details}


def _fill_knn(df, col, n=5):
    from sklearn.impute import KNNImputer
    num = df.select_dtypes(include=[np.number]).columns.tolist()
    if col not in num:
        return df
    imp = KNNImputer(n_neighbors=n)
    df[num] = imp.fit_transform(df[num])
    return df


def _fill_regression(df, col):
    from sklearn.linear_model import LinearRegression
    num = df.select_dtypes(include=[np.number]).columns.tolist()
    if col not in num:
        return df
    feat = [c for c in num if c != col]
    if not feat:
        df[col] = df[col].fillna(df[col].median())
        return df
    train = df[df[col].notna()]
    mask = df[col].isna()
    if mask.sum() == 0 or len(train) < 2:
        return df
    m = LinearRegression().fit(train[feat].fillna(0), train[col])
    df.loc[mask, col] = m.predict(df.loc[mask, feat].fillna(0))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  3. Fix Data Types
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("fix_data_types", "structure", "Fix wrong data types", "critical")
def fix_data_types(df: pd.DataFrame, columns: Optional[list[str]] = None, params: Optional[dict] = None, **kw):
    targets = columns or df.columns.tolist()
    conversions = {}
    for col in targets:
        if col not in df.columns:
            continue
        orig = str(df[col].dtype)
        if df[col].dtype == object or str(df[col].dtype) in ("string", "str"):
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue

            # Skip columns with long text (likely freeform notes/comments)
            avg_len = non_null.astype(str).str.len().mean()
            if avg_len > 30:
                continue

            # Try numeric — require 80%+ parseable (not 50%)
            num = pd.to_numeric(df[col], errors="coerce")
            if num.notna().sum() > non_null.shape[0] * 0.8:
                df[col] = num
                conversions[col] = {"from": orig, "to": str(df[col].dtype)}
                continue

            # Try datetime
            try:
                dt = pd.to_datetime(df[col], errors="coerce")
                if dt.notna().sum() > non_null.shape[0] * 0.6:
                    df[col] = dt
                    conversions[col] = {"from": orig, "to": "datetime64"}
                    continue
            except Exception:
                pass

            # Try boolean
            bmap = {"true": True, "false": False, "yes": True, "no": False,
                    "1": True, "0": False, "y": True, "n": False}
            vals = non_null.astype(str).str.lower().unique()
            if len(vals) > 0 and all(v in bmap for v in vals):
                df[col] = df[col].astype(str).str.lower().map(bmap)
                conversions[col] = {"from": orig, "to": "bool"}
                continue

        # Excel serial date detection
        if pd.api.types.is_numeric_dtype(df[col]):
            sample = df[col].dropna()
            if len(sample) > 0 and 25000 < sample.median() < 65000:
                try:
                    df[col] = pd.Timestamp("1899-12-30") + pd.to_timedelta(df[col], unit="D")
                    conversions[col] = {"from": orig, "to": "datetime64 (Excel serial)"}
                except Exception:
                    pass
    return df, {"conversions": conversions}


# ═══════════════════════════════════════════════════════════════════════════════
#  4. Standardize Column Headers
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("standardize_headers", "structure", "Standardize column headers to snake_case", "high")
def standardize_headers(df: pd.DataFrame, **kw):
    original = df.columns.tolist()
    new = (df.columns.str.strip().str.lower()
           .str.replace(r"[^\w]+", "_", regex=True)
           .str.strip("_")
           .str.replace(r"^[\d_]+", "", regex=True))
    new = [c if c else f"col_{i}" for i, c in enumerate(new)]
    seen, final = {}, []
    for c in new:
        if c in seen:
            seen[c] += 1
            final.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            final.append(c)
    df.columns = final
    renamed = {o: n for o, n in zip(original, final) if o != n}
    return df, {"columns_renamed": renamed}


# ═══════════════════════════════════════════════════════════════════════════════
#  5. Strip & Collapse Whitespace
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("strip_whitespace", "text", "Strip and collapse whitespace", "high")
def strip_whitespace(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    cols = columns or df.select_dtypes(include=["object", "string"]).columns.tolist()
    cleaned = {}
    for col in cols:
        if col not in df.columns:
            continue
        before = df[col].copy()
        df[col] = df[col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        df.loc[before.isna(), col] = np.nan
        n = (before.fillna("") != df[col].fillna("")).sum()
        if n > 0:
            cleaned[col] = int(n)
    return df, {"columns_cleaned": cleaned}


# ═══════════════════════════════════════════════════════════════════════════════
#  6. Normalize Text Case
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("normalize_text_case", "text", "Normalize text case (lower/upper/title)", "high")
def normalize_text_case(df: pd.DataFrame, columns: Optional[list[str]] = None,
                        strategy: str = "lower", **kw):
    cols = columns or df.select_dtypes(include=["object", "string"]).columns.tolist()
    fn = {"lower": str.lower, "upper": str.upper, "title": str.title}.get(strategy, str.lower)
    for col in cols:
        if col not in df.columns:
            continue
        mask = df[col].notna()
        df.loc[mask, col] = df.loc[mask, col].astype(str).apply(fn)
    return df, {"case": strategy, "columns": cols}


# ═══════════════════════════════════════════════════════════════════════════════
#  7. Fix Encoding Errors
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("fix_encoding", "text", "Fix UTF-8/Latin-1 encoding errors", "high")
def fix_encoding(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    cols = columns or df.select_dtypes(include=["object", "string"]).columns.tolist()
    fixed = {}
    for col in cols:
        if col not in df.columns:
            continue
        count = 0
        def _fix(val):
            nonlocal count
            if not isinstance(val, str):
                return val
            try:
                r = val.encode("latin1").decode("utf-8")
                if r != val:
                    count += 1
                    return r
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
            return val
        df[col] = df[col].apply(_fix)
        if count:
            fixed[col] = count
    return df, {"columns_fixed": fixed}


# ═══════════════════════════════════════════════════════════════════════════════
#  7b. Clean Null-like String Values → NaN
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("clean_null_strings", "text", "Convert N/A, NULL, --, blank strings to actual NaN", "high")
def clean_null_strings(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    NULL_STRINGS = {"null", "none", "na", "n/a", "#n/a", "--", "-", "nan", "nil", "undefined", ""}
    targets = columns or df.select_dtypes(include=["object", "string"]).columns.tolist()
    cleaned = {}
    for col in targets:
        if col not in df.columns:
            continue
        mask = df[col].astype(str).str.strip().str.lower().isin(NULL_STRINGS)
        count = int(mask.sum())
        if count > 0:
            df.loc[mask, col] = np.nan
            cleaned[col] = count
    return df, {"columns_cleaned": cleaned}


# ═══════════════════════════════════════════════════════════════════════════════
#  7c. String Length Validation
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("validate_string_length", "validation", "Flag/trim strings that are too short or too long", "medium")
def validate_string_length(df: pd.DataFrame, columns: Optional[list[str]] = None,
                           params: Optional[dict] = None, **kw):
    params = params or {}
    min_len = params.get("min_len", 1)
    max_len = params.get("max_len", 500)
    targets = columns or df.select_dtypes(include=["object", "string"]).columns.tolist()
    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        lengths = df[col].dropna().astype(str).str.len()
        too_short = int((lengths < min_len).sum())
        too_long = int((lengths > max_len).sum())
        if too_short > 0:
            df.loc[df[col].astype(str).str.len() < min_len, col] = np.nan
        if too_long > 0:
            df[col] = df[col].astype(str).str[:max_len]
        if too_short + too_long > 0:
            applied[col] = {"too_short": too_short, "too_long": too_long}
    return df, {"columns_validated": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  8. Standardize Categorical Values
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("standardize_categories", "text", "Standardize categorical values via mapping or auto-grouping", "high")
def standardize_categories(df: pd.DataFrame, columns: Optional[list[str]] = None,
                           params: Optional[dict] = None, **kw):
    params = params or {}
    mapping = params.get("mapping", {})
    applied = {}

    if mapping:
        for col in (columns or []):
            if col not in df.columns or col not in mapping:
                continue
            before = df[col].nunique()
            df[col] = df[col].replace(mapping[col])
            applied[col] = {"unique_before": before, "unique_after": df[col].nunique()}
        return df, {"columns_applied": applied}

    # Auto-mode: find categorical columns with 3-50 unique values
    targets = columns or []
    if not targets:
        for col in df.columns:
            if df[col].dtype == object or str(df[col].dtype) in ("string", "str"):
                nuniq = df[col].nunique()
                if 3 <= nuniq <= 50:
                    targets.append(col)

    for col in targets:
        if col not in df.columns:
            continue
        unique_vals = df[col].dropna().unique().tolist()
        if len(unique_vals) < 2:
            continue

        # ── Step A: Clean dirty null-like values first ──
        NULL_LIKE = {"null", "none", "na", "n/a", "#n/a", "--", "-", "nan", ""}
        for val in unique_vals:
            if str(val).strip().lower() in NULL_LIKE:
                df.loc[df[col] == val, col] = np.nan

        unique_vals = df[col].dropna().unique().tolist()

        # ── Step B: Context-aware abbreviation expansion ──
        # Select relevant abbreviations based on column name
        col_lower = col.lower()

        # Base abbreviations (always apply)
        abbreviations = {}

        # Department-related columns
        if any(k in col_lower for k in ["dept", "department", "division", "team", "function"]):
            abbreviations.update({
                "engg": "engineering", "eng": "engineering",
                "mktg": "marketing", "mkt": "marketing",
                "fin": "finance",
                "hr": "human resources",
                "mgmt": "management", "mgt": "management",
                "admin": "administration",
                "dev": "development",
                "ops": "operations",
                "acct": "accounting",
                "sale": "sales",
                "mfg": "manufacturing",
                "qa": "quality assurance",
                "cs": "customer service",
                "it": "information technology", "info tech": "information technology",
                "rd": "research and development",
                "prod": "production",
                "svc": "services",
                "legal": "legal",
            })
        # Gender-related columns
        elif any(k in col_lower for k in ["gender", "sex"]):
            abbreviations.update({
                "m": "male", "f": "female",
                "0": "female", "1": "male",
                "other": "other", "unknown": "other",
            })
        # Payment-related columns
        elif any(k in col_lower for k in ["payment", "pay_mode", "pay_method", "transaction"]):
            abbreviations.update({
                "cc": "credit card", "dc": "debit card",
                "cod": "cash on delivery", "cash": "cash",
                "netbanking": "net banking",
                "upi": "upi", "wallet": "wallet",
            })
        # Product-related columns
        elif any(k in col_lower for k in ["product", "item", "sku"]):
            abbreviations.update({
                "tablt": "tablet", "monitr": "monitor",
                "printers": "printer", "laptops": "laptop",
                "mobiles": "mobile", "keyboards": "keyboard",
                "mouses": "mouse", "monitors": "monitor",
                "tablets": "tablet", "headphone": "headphones",
            })
        # City-related columns
        elif any(k in col_lower for k in ["city", "town", "location", "place"]):
            abbreviations.update({
                "bombay": "mumbai", "bengaluru": "bangalore",
                "bangalroe": "bangalore", "bangalure": "bangalore",
                "chenai": "chennai", "madras": "chennai",
                "hydrabad": "hyderabad", "secunderabad": "hyderabad",
                "calcutta": "kolkata",
                "new delhi": "delhi",
                "ahemdabad": "ahmedabad", "amdavad": "ahmedabad",
                "benaras": "varanasi", "benares": "varanasi",
                "trivandrum": "thiruvananthapuram",
                "pondicherry": "puducherry",
                "gurgaon": "gurugram",
            })
        # State-related columns
        elif any(k in col_lower for k in ["state", "province", "region"]):
            abbreviations.update({
                "mh": "maharashtra", "dl": "delhi", "ka": "karnataka",
                "tn": "tamil nadu", "ts": "telangana", "ap": "andhra pradesh",
                "gj": "gujarat", "rj": "rajasthan", "up": "uttar pradesh",
                "wb": "west bengal", "mp": "madhya pradesh", "pb": "punjab",
                "hr": "haryana", "jk": "jammu and kashmir", "uk": "uttarakhand",
                "ga": "goa", "br": "bihar", "jh": "jharkhand",
                "or": "odisha", "ct": "chhattisgarh", "hp": "himachal pradesh",
                "as": "assam", "kl": "kerala", "mn": "manipur",
            })
        # Status-related columns
        elif any(k in col_lower for k in ["status", "state"]):
            abbreviations.update({
                "na": "unknown", "null": "unknown",
            })
        else:
            # Generic fallback — safe expansions
            abbreviations.update({
                "engg": "engineering", "eng": "engineering",
                "mktg": "marketing", "fin": "finance",
                "ops": "operations", "it": "information technology",
                "info tech": "information technology",
                "sale": "sales", "sale": "sales",
                "tablt": "tablet", "monitr": "monitor",
                "printers": "printer",
                "cc": "credit card", "dc": "debit card",
                "cod": "cash on delivery",
                "netbanking": "net banking",
                "m": "male", "f": "female",
                "0": "female", "1": "male",
                "unknown": "other",
            })

        groups = {}
        for val in unique_vals:
            key = str(val).lower().strip()
            key = re.sub(r"[.\-_/\\()&,;:!?'\"]+", " ", key).strip()
            key = re.sub(r"\s+", " ", key)
            key = abbreviations.get(key, key)
            if key not in groups:
                groups[key] = []
            groups[key].append(val)

        col_map = {}
        for key, variants in groups.items():
            if len(variants) <= 1:
                continue
            # Use the expanded key as canonical (e.g. 'other' not 'unknown')
            # If the key itself is among variants, use it. Otherwise use longest.
            if key in variants:
                canonical = key
            else:
                canonical = key  # use the abbreviation-expanded form
            for v in variants:
                if v != canonical:
                    col_map[v] = canonical

        # Also map single-variant abbreviation expansions
        for val in unique_vals:
            key = str(val).lower().strip()
            key_clean = re.sub(r"[.\-_/\\()&,;:!?'\"]+", " ", key).strip()
            key_clean = re.sub(r"\s+", " ", key_clean)
            expanded = abbreviations.get(key_clean)
            if expanded and expanded != key_clean and val not in col_map:
                col_map[val] = expanded

        if col_map:
            before = df[col].nunique()
            df[col] = df[col].replace(col_map)
            applied[col] = {
                "unique_before": before,
                "unique_after": df[col].nunique(),
                "mappings_applied": len(col_map),
            }

    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  9. Standardize Boolean Values
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("standardize_booleans", "text", "Standardize booleans (Yes/1/TRUE → True)", "medium")
def standardize_booleans(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    TRUE_VALS = {"yes", "1", "true", "y", "si", "on", "1.0"}
    FALSE_VALS = {"no", "0", "false", "n", "off", "0.0"}
    targets = columns or []
    if not targets:
        for col in df.select_dtypes(include=["object", "string"]).columns:
            uniq = set(df[col].dropna().astype(str).str.lower().str.strip().unique())
            if uniq and uniq.issubset(TRUE_VALS | FALSE_VALS):
                targets.append(col)
    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        lower = df[col].astype(str).str.lower().str.strip()
        df[col] = lower.apply(lambda x: True if x in TRUE_VALS else (False if x in FALSE_VALS else np.nan))
        applied[col] = "bool"
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  10. Standardize Date & Time Formats
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("standardize_dates", "datetime", "Standardize dates to ISO 8601", "critical")
def standardize_dates(df: pd.DataFrame, columns: Optional[list[str]] = None,
                      params: Optional[dict] = None, **kw):
    from dateutil import parser as dateparser

    params = params or {}
    fmt = params.get("output_format")
    targets = columns or []

    def _try_parse(val):
        if pd.isna(val) or str(val).strip() == "":
            return pd.NaT
        try:
            return dateparser.parse(str(val), dayfirst=True)
        except (ValueError, TypeError, OverflowError):
            return pd.NaT

    if not targets:
        for col in df.columns:
            if not (df[col].dtype == object or str(df[col].dtype) in ("string", "str")):
                continue
            sample = df[col].dropna().head(50)
            if len(sample) == 0:
                continue
            # Skip long text columns
            if sample.astype(str).str.len().mean() > 25:
                continue
            # CRITICAL: Skip columns where most values are pure numbers (age, rating, etc.)
            numeric_check = pd.to_numeric(sample, errors="coerce")
            if numeric_check.notna().sum() > len(sample) * 0.5:
                continue  # This is a numeric column, not a date column
            # Check column name for date hints
            name_hint = any(k in col.lower() for k in ["date", "time", "created", "updated", "joined", "dob", "birth"])
            # Skip column names that clearly aren't dates
            not_date_hint = any(k in col.lower() for k in ["age", "rating", "score", "amount", "price", "salary",
                                                             "discount", "pct", "percent", "qty", "quantity", "count",
                                                             "id", "phone", "zip", "pin", "code"])
            if not_date_hint and not name_hint:
                continue
            # Try parsing sample
            parsed = sample.apply(_try_parse)
            ratio = parsed.notna().sum() / len(sample)
            if ratio > 0.5 or (name_hint and ratio > 0.3):
                targets.append(col)

    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        orig = str(df[col].dtype)
        # Fast path: vectorized pd.to_datetime first
        parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
        # Slow fallback only for rows that failed
        failed_mask = parsed.isna() & df[col].notna()
        failed_count = int(failed_mask.sum())
        if failed_count > 0 and failed_count < len(df) * 0.5:
            failed_vals = df.loc[failed_mask, col].apply(_try_parse)
            parsed.loc[failed_mask] = pd.to_datetime(failed_vals, errors="coerce")
        df[col] = parsed
        if fmt:
            df[col] = df[col].dt.strftime(fmt)
        applied[col] = {"from": orig, "to": str(df[col].dtype)}
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  11. Remove Currency Symbols & Format Noise
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("clean_currency", "numeric", "Remove currency symbols and format noise", "critical")
def clean_currency(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    targets = columns or []
    if not targets:
        for col in df.select_dtypes(include=["object", "string"]).columns:
            sample = df[col].dropna().astype(str).head(50)
            if len(sample) == 0:
                continue
            if sample.str.len().mean() > 25:
                continue
            # Detect currency: $, €, ₹, Rs., INR, /- suffix
            currency_hits = sample.str.contains(
                r"[$€£¥₹₽]|^Rs\.?\s*\d|^INR\s|^USD\s|^EUR\s|^MXN\s|^KRW\s|^GBP\s|\d/-\s*$",
                regex=True, case=False
            ).sum()
            if currency_hits > len(sample) * 0.15:
                targets.append(col)
    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        s = df[col].astype(str)
        # Strip currency words/symbols and /- suffix (vectorized)
        s = s.str.replace(r"[$€£¥₹₽]", "", regex=True)
        s = s.str.replace(r"(?i)^Rs\.?\s*", "", regex=True)
        s = s.str.replace(r"(?i)^(INR|USD|EUR|GBP|MXN|KRW)\s*", "", regex=True)
        s = s.str.replace(r"/-\s*$", "", regex=True)
        s = s.str.strip()
        # European format: dots as thousands, comma as decimal
        euro_mask = s.str.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", na=False)
        s_euro = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        s_std = s.str.replace(",", "", regex=False)
        combined = s_std.copy()
        combined[euro_mask] = s_euro[euro_mask]
        df[col] = pd.to_numeric(combined, errors="coerce")
        applied[col] = "cleaned"
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  12. Detect & Treat Outliers
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("handle_outliers", "numeric", "Detect and treat outliers (IQR/Z-score)", "critical")
def handle_outliers(df: pd.DataFrame, columns: Optional[list[str]] = None,
                    strategy: str = "clip", params: Optional[dict] = None, **kw):
    params = params or {}
    method = params.get("method", "iqr")
    threshold = params.get("threshold", 1.5 if method == "iqr" else 3.0)
    targets = columns or df.select_dtypes(include=[np.number]).columns.tolist()
    applied = {}
    for col in targets:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        s = df[col].dropna()
        if len(s) < 10:
            continue
        if method == "iqr":
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr = q3 - q1
            lo, hi = q1 - threshold * iqr, q3 + threshold * iqr
        else:
            lo, hi = s.mean() - threshold * s.std(), s.mean() + threshold * s.std()
        mask = (df[col] < lo) | (df[col] > hi)
        n = int(mask.sum())
        if n == 0:
            continue
        if strategy == "clip":
            df[col] = df[col].clip(lower=lo, upper=hi)
        elif strategy == "remove":
            df = df[~mask]
        elif strategy in ("impute_median", "impute_mean"):
            fill = s.median() if "median" in strategy else s.mean()
            df.loc[mask, col] = fill
        applied[col] = {"outliers": n, "bounds": [round(lo, 4), round(hi, 4)]}
    return df.reset_index(drop=True), {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  13. Validate Numeric Range
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("validate_numeric_range", "numeric", "Validate numeric range bounds", "high")
def validate_numeric_range(df: pd.DataFrame, params: Optional[dict] = None, **kw):
    params = params or {}
    ranges = params.get("ranges", {})

    # Auto-detect common ranges by column name when no config provided
    if not ranges:
        AUTO_RANGES = {
            "age": {"min": 0, "max": 120},
            "rating": {"min": 1, "max": 5},
            "score": {"min": 0, "max": 100},
            "percentage": {"min": 0, "max": 100},
            "pct": {"min": 0, "max": 100},
            "percent": {"min": 0, "max": 100},
        }
        # Also enforce no-negatives for monetary/quantity columns
        NO_NEGATIVE_KEYWORDS = ["amount", "salary", "price", "cost", "revenue", "income",
                                 "discount", "qty", "quantity", "fee", "total", "payment"]
        for col in df.select_dtypes(include=[np.number]).columns:
            col_lower = col.lower()
            # Check explicit range rules first
            matched = False
            for keyword, bounds in AUTO_RANGES.items():
                if keyword in col_lower:
                    ranges[col] = bounds
                    matched = True
                    break
            # Then check no-negative rules
            if not matched:
                for keyword in NO_NEGATIVE_KEYWORDS:
                    if keyword in col_lower:
                        ranges[col] = {"min": 0}
                        break

    applied = {}
    for col, bounds in ranges.items():
        if col not in df.columns:
            continue
        flagged = 0
        if "min" in bounds:
            m = df[col] < bounds["min"]
            flagged += int(m.sum())
            df.loc[m, col] = np.nan
        if "max" in bounds:
            m = df[col] > bounds["max"]
            flagged += int(m.sum())
            df.loc[m, col] = np.nan
        applied[col] = {"flagged": flagged, "bounds": bounds}
    return df, {"columns_validated": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  14. Validate Emails
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("validate_emails", "quality", "Validate and clean email addresses", "critical")
def validate_emails(df: pd.DataFrame, columns: Optional[list[str]] = None,
                    strategy: str = "flag", **kw):
    PAT = r"^[\w.+\-]+@[\w\-]+\.[\w.]+$"
    targets = columns or []
    if not targets:
        for col in df.select_dtypes(include=["object", "string"]).columns:
            if df[col].dropna().astype(str).head(50).str.contains("@").sum() > 5:
                targets.append(col)
    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.strip().str.lower()
        valid = df[col].str.match(PAT, na=False)
        inv = int((~valid & df[col].notna()).sum())
        if strategy == "remove":
            df.loc[~valid, col] = np.nan
        else:
            df[f"{col}_valid"] = valid
        applied[col] = {"invalid": inv}
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  15. Normalize Phone Numbers
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("normalize_phones", "quality", "Normalize phone numbers to digits", "high")
def normalize_phones(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    targets = columns or []
    # Auto-detect phone columns by name
    if not targets:
        for col in df.columns:
            if any(k in col.lower() for k in ["phone", "mobile", "cell", "tel", "contact_number"]):
                targets.append(col)
    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.replace(r"[^\d+]", "", regex=True)
        # Clean up 'nan' strings
        df.loc[df[col].isin(["nan", ""]), col] = np.nan
        applied[col] = "normalized"
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  16. Cross-Column Consistency
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("cross_column_check", "quality", "Cross-column consistency checks", "high")
def cross_column_check(df: pd.DataFrame, params: Optional[dict] = None, **kw):
    params = params or {}
    rules = params.get("rules", [])
    flagged = 0
    for rule in rules:
        a, b = rule.get("col_a"), rule.get("col_b")
        if a not in df.columns or b not in df.columns:
            continue
        try:
            da = pd.to_datetime(df[a], errors="coerce")
            db = pd.to_datetime(df[b], errors="coerce")
            flag = da > db
            df[f"{a}_{b}_inconsistent"] = flag
            flagged += int(flag.sum())
        except Exception:
            pass
    return df, {"rows_flagged": flagged}


# ═══════════════════════════════════════════════════════════════════════════════
#  17. Whitelist Validation
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("whitelist_validation", "quality", "Validate values against whitelist", "high")
def whitelist_validation(df: pd.DataFrame, params: Optional[dict] = None, **kw):
    params = params or {}
    whitelists = params.get("whitelists", {})
    applied = {}
    for col, allowed in whitelists.items():
        if col not in df.columns:
            continue
        allowed_set = {str(v).strip() for v in allowed}
        inv = ~df[col].astype(str).str.strip().isin(allowed_set) & df[col].notna()
        df[f"{col}_invalid"] = inv
        applied[col] = {"invalid": int(inv.sum())}
    return df, {"columns_validated": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  18. Extract from Freeform Text
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("extract_from_text", "advanced", "Extract structured data via regex", "high")
def extract_from_text(df: pd.DataFrame, params: Optional[dict] = None, **kw):
    params = params or {}
    extractions = params.get("extractions", [])
    applied = {}
    for ext in extractions:
        src, pat = ext.get("source_col"), ext.get("pattern")
        new_col = ext.get("new_col", f"{src}_extracted")
        if src not in df.columns:
            continue
        try:
            df[new_col] = df[src].astype(str).str.extract(pat, expand=False)
            applied[new_col] = {"extracted": int(df[new_col].notna().sum())}
        except Exception as e:
            applied[new_col] = {"error": str(e)}
    return df, {"columns_created": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  19. Mask PII
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("mask_pii", "advanced", "Mask and anonymize PII", "high")
def mask_pii(df: pd.DataFrame, columns: Optional[list[str]] = None,
             params: Optional[dict] = None, **kw):
    params = params or {}
    pii_type = params.get("pii_type", "auto")
    patterns = {
        "ssn": (r"\b\d{3}-\d{2}-\d{4}\b", "***-**-****"),
        "aadhaar": (r"\b\d{4}\s\d{4}\s\d{4}\b", "**** **** ****"),
    }

    # Auto-detect: scan all text columns for PII patterns
    targets = columns or []
    if not targets:
        for col in df.select_dtypes(include=["object", "string"]).columns:
            sample = df[col].dropna().astype(str).head(50)
            for pname, (pat, _) in patterns.items():
                if sample.str.contains(pat, regex=True, na=False).sum() > 2:
                    targets.append(col)
                    break

    applied = {}
    for col in targets:
        if col not in df.columns:
            continue
        count = 0
        for pname, (pat, repl) in patterns.items():
            n = int(df[col].astype(str).str.contains(pat, regex=True, na=False).sum())
            if n > 0:
                df[col] = df[col].astype(str).str.replace(pat, repl, regex=True)
                count += n
        applied[col] = {"redactions": count}
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  20. Standardize Measurement Units
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("standardize_units", "advanced", "Standardize measurement units", "high")
def standardize_units(df: pd.DataFrame, params: Optional[dict] = None, **kw):
    params = params or {}
    unit_config = params.get("units", {})
    applied = {}
    for key, cfg in unit_config.items():
        col = cfg.get("column")
        base_unit = cfg.get("base_unit", "g")
        factors = cfg.get("factors", {})
        if col not in df.columns:
            continue
        def convert(val):
            if pd.isna(val):
                return np.nan
            s = str(val).strip().lower()
            m = re.search(r"([\d.]+)", s)
            if not m:
                return np.nan
            num = float(m.group(1))
            for u, f in factors.items():
                if u.lower() in s:
                    return num * f
            return num
        df[col] = df[col].apply(convert)
        df[f"{col}_unit"] = base_unit
        applied[col] = {"base_unit": base_unit}
    return df, {"columns_applied": applied}


# ═══════════════════════════════════════════════════════════════════════════════
#  EXTENDED STEPS (from PDF spec)
# ═══════════════════════════════════════════════════════════════════════════════

@register_step("remove_constant_columns", "structure", "Remove constant/uninformative columns", "medium")
def remove_constant_columns(df: pd.DataFrame, **kw):
    const = [c for c in df.columns if df[c].nunique(dropna=False) <= 1]
    df = df.drop(columns=const)
    return df, {"removed": const}


@register_step("remove_empty_rows", "structure", "Remove rows where all values are empty/null", "critical")
def remove_empty_rows(df: pd.DataFrame, **kw):
    before = len(df)
    df = df.dropna(how="all").reset_index(drop=True)
    return df, {"rows_removed": before - len(df)}


@register_step("remove_duplicate_columns", "structure", "Remove duplicate column names keeping first occurrence", "medium")
def remove_duplicate_columns(df: pd.DataFrame, **kw):
    before_cols = df.columns.tolist()
    df = df.loc[:, ~df.columns.duplicated()]
    removed = [c for c in before_cols if c not in df.columns.tolist()]
    return df, {"removed": removed}


@register_step("feature_engineering_dates", "advanced", "Extract date parts (year/month/day/weekday)", "medium")
def feature_engineering_dates(df: pd.DataFrame, columns: Optional[list[str]] = None, **kw):
    targets = columns or df.select_dtypes(include=["datetime64", "datetime64[ns]"]).columns.tolist()
    created = {}
    for col in targets:
        if col not in df.columns:
            continue
        dt = pd.to_datetime(df[col], errors="coerce")
        if dt.notna().sum() == 0:
            continue
        df[f"{col}_year"] = dt.dt.year
        df[f"{col}_month"] = dt.dt.month
        df[f"{col}_day"] = dt.dt.day
        df[f"{col}_weekday"] = dt.dt.day_name()
        created[col] = [f"{col}_year", f"{col}_month", f"{col}_day", f"{col}_weekday"]
    return df, {"columns_created": created}


@register_step("scaling_normalization", "numeric", "Scale/normalize (minmax/zscore/robust) — only runs if columns specified", "medium")
def scaling_normalization(df: pd.DataFrame, columns: Optional[list[str]] = None,
                          strategy: str = "minmax", **kw):
    # Only scale if columns are explicitly specified — auto-scaling all numbers destroys real values
    if not columns:
        return df, {"skipped": True, "message": "No columns specified for scaling"}
    applied = {}
    for col in columns:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        s = df[col].dropna()
        if len(s) < 2:
            continue
        if strategy == "minmax":
            mn, mx = s.min(), s.max()
            if mx - mn != 0:
                df[col] = (df[col] - mn) / (mx - mn)
                applied[col] = "minmax"
        elif strategy == "zscore":
            mean, std = s.mean(), s.std()
            if std != 0:
                df[col] = (df[col] - mean) / std
                applied[col] = "zscore"
        elif strategy == "robust":
            med = s.median()
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr = q3 - q1
            if iqr != 0:
                df[col] = (df[col] - med) / iqr
                applied[col] = "robust"
    return df, {"columns_scaled": applied}


@register_step("binning", "numeric", "Bin continuous data into categories", "medium")
def binning(df: pd.DataFrame, columns: Optional[list[str]] = None,
            strategy: str = "equal_width", params: Optional[dict] = None, **kw):
    params = params or {}
    n_bins = params.get("n_bins", 5)
    applied = {}
    for col in (columns or []):
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        new = f"{col}_binned"
        if strategy == "equal_width":
            df[new] = pd.cut(df[col], bins=n_bins, labels=False)
        else:
            df[new] = pd.qcut(df[col], q=n_bins, labels=False, duplicates="drop")
        applied[col] = new
    return df, {"columns_binned": applied}


@register_step("use_first_row_as_header", "structure", "Use first data row as column headers if current headers look auto-generated", "medium")
def use_first_row_as_header(df: pd.DataFrame, **kw):
    if len(df) < 2:
        return df, {"skipped": True, "message": "Not enough rows"}

    # Only trigger if headers look auto-generated (Unnamed:, col_0, 0, 1, 2...)
    current = df.columns.tolist()
    auto_patterns = sum(
        1 for c in current
        if str(c).startswith("Unnamed") or
           str(c).startswith("col_") or
           str(c).isdigit()
    )
    if auto_patterns < len(current) * 0.5:
        # Headers look like real column names — don't replace them
        return df, {"skipped": True, "message": "Headers look valid, not replacing"}

    hdr = df.iloc[0].astype(str).tolist()
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = hdr
    return df, {"new_headers": hdr}


@register_step("replace_value", "text", "Replace specific values in columns", "medium")
def replace_value(df: pd.DataFrame, columns: Optional[list[str]] = None,
                  params: Optional[dict] = None, **kw):
    params = params or {}
    find = params.get("find")
    if find is None:
        return df, {"skipped": True, "message": "No 'find' value specified"}
    repl = params.get("replace", "")
    use_regex = params.get("regex", False)
    targets = columns or df.columns.tolist()
    total = 0
    for col in targets:
        if col not in df.columns:
            continue
        if use_regex:
            n = int(df[col].astype(str).str.contains(str(find), regex=True, na=False).sum())
            df[col] = df[col].astype(str).str.replace(str(find), str(repl), regex=True)
        else:
            n = int((df[col] == find).sum())
            df[col] = df[col].replace(find, repl)
        total += n
    return df, {"total_replaced": total}


# ═══════════════════════════════════════════════════════════════════════════════
#  ENGINE: Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def get_available_steps() -> list[dict]:
    return [
        {"name": v["name"], "category": v["category"],
         "description": v["description"], "severity": v["severity"]}
        for v in STEP_REGISTRY.values()
    ]


def get_auto_clean_steps(df: pd.DataFrame) -> list[dict]:
    """Analyse DataFrame and return auto-detected cleaning step configs."""
    recs = []

    # Duplicates
    n = df.duplicated().sum()
    if n > 0:
        recs.append({"step": "remove_duplicates"})

    # Missing values
    null_cols = [c for c in df.columns if df[c].isna().sum() > 0]
    if null_cols:
        recs.append({"step": "handle_missing_values", "strategy": "fill_median"})

    # Messy headers
    messy = [c for c in df.columns if c != c.strip().lower().replace(" ", "_") or re.search(r"[^a-z0-9_]", str(c).lower())]
    if messy:
        recs.append({"step": "standardize_headers"})

    # Whitespace in text
    ws_found = False
    for col in df.select_dtypes(include=["object", "string"]).columns:
        sample = df[col].dropna().head(100).astype(str)
        if (sample != sample.str.strip()).any() or sample.str.contains(r"\s{2,}", regex=True).any():
            ws_found = True
            break
    if ws_found:
        recs.append({"step": "strip_whitespace"})

    # Data type issues
    for col in df.select_dtypes(include=["object", "string"]).columns:
        num = pd.to_numeric(df[col], errors="coerce")
        if num.notna().sum() > df[col].notna().sum() * 0.5:
            recs.append({"step": "fix_data_types"})
            break

    # Constant columns
    const = [c for c in df.columns if df[c].nunique(dropna=False) <= 1]
    if const:
        recs.append({"step": "remove_constant_columns"})

    # Outliers
    for col in df.select_dtypes(include=[np.number]).columns:
        s = df[col].dropna()
        if len(s) < 10:
            continue
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3 - q1
        if ((s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)).sum() > 0:
            recs.append({"step": "handle_outliers", "strategy": "clip"})
            break

    return recs


def execute_step(df: pd.DataFrame, step_config: dict) -> tuple[pd.DataFrame, dict]:
    name = step_config.get("step")
    if name not in STEP_REGISTRY:
        return df, {"step": name, "status": "skipped", "message": f"Unknown step: {name}"}
    try:
        result_df, details = STEP_REGISTRY[name]["function"](
            df,
            columns=step_config.get("columns"),
            strategy=step_config.get("strategy"),
            params=step_config.get("params"),
        )
        return result_df, {"step": name, "status": "applied", "details": details}
    except Exception as e:
        return df, {"step": name, "status": "error", "message": str(e)}


def run_cleaning_pipeline(df: pd.DataFrame, steps: list[dict], on_progress=None) -> tuple[pd.DataFrame, list[dict]]:
    import time
    results = []
    for i, cfg in enumerate(steps, 1):
        step_name = cfg.get("step", "unknown")
        t0 = time.time()
        df, r = execute_step(df, cfg)
        elapsed = time.time() - t0
        status = r.get("status", "?")
        print(f"  [{i}/{len(steps)}] {step_name:35} {status:7} {elapsed:.2f}s  ({len(df)} rows)", flush=True)
        results.append(r)
        if on_progress:
            on_progress(i, len(steps), step_name, elapsed)
    return df, results
