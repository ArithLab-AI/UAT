import pandas as pd
import json
import numpy as np
from pathlib import Path
from typing import Optional

SUPPORTED_EXTENSIONS = {"csv", "tsv", "xlsx", "xls", "json", "parquet", "pdf"}


def read_file_to_dataframe(file_path: str, file_type: Optional[str] = None) -> pd.DataFrame:
    path = Path(file_path)
    if file_type is None:
        file_type = path.suffix.lower().lstrip(".")
    if file_type not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported: '.{file_type}'. Supported: {', '.join(SUPPORTED_EXTENSIONS)}")

    if file_type == "csv":
        try: return pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError: return pd.read_csv(file_path, encoding="latin-1")
    elif file_type == "tsv":
        try: return pd.read_csv(file_path, sep="\t", encoding="utf-8")
        except UnicodeDecodeError: return pd.read_csv(file_path, sep="\t", encoding="latin-1")
    elif file_type in ("xlsx", "xls"):
        try: return pd.read_excel(file_path, engine="calamine")
        except Exception: return pd.read_excel(file_path, engine="openpyxl")
    elif file_type == "json":
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list): return pd.DataFrame(data)
        if isinstance(data, dict):
            if all(isinstance(v, list) for v in data.values()): return pd.DataFrame(data)
            if all(isinstance(v, dict) for v in data.values()): return pd.DataFrame.from_dict(data, orient="index")
            return pd.json_normalize(data)
        raise ValueError("Unsupported JSON structure")
    elif file_type == "parquet": return pd.read_parquet(file_path)
    elif file_type == "pdf":
        import pdfplumber
        tables = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                for t in page.extract_tables():
                    if t and len(t) > 1: tables.append(pd.DataFrame(t[1:], columns=t[0]))
        if tables: return pd.concat(tables, ignore_index=True)
        lines = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    for line in text.strip().split("\n"): lines.append({"text": line.strip()})
        if lines: return pd.DataFrame(lines)
        raise ValueError("Could not extract data from PDF.")


def write_dataframe_to_file(df: pd.DataFrame, output_path: str, file_type: str) -> str:
    writers = {
        "csv": lambda d, p: d.to_csv(p, index=False),
        "tsv": lambda d, p: d.to_csv(p, index=False, sep="\t"),
        "xlsx": lambda d, p: d.to_excel(p, index=False, engine="openpyxl"),
        "json": lambda d, p: d.to_json(p, orient="records", indent=2, force_ascii=False),
        "parquet": lambda d, p: d.to_parquet(p, index=False),
    }
    writer = writers.get(file_type)
    if not writer:
        file_type = "csv"
        output_path = str(Path(output_path).with_suffix(".csv"))
        writer = writers["csv"]
    writer(df, output_path)
    return output_path
