from __future__ import annotations

import csv
import os
from io import BytesIO
from typing import Any, Dict, List, Tuple

import pandas as pd


class FileFormatError(ValueError):
    pass


def _clean_value(v: Any) -> Any:
    if pd.isna(v):
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return repr(v)
    return v


def dataframe_preview(df: pd.DataFrame, n: int = 20) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if df is None or df.empty:
        return rows
    head = df.head(n)
    for _, row in head.iterrows():
        d: Dict[str, Any] = {}
        for col in head.columns:
            d[str(col)] = _clean_value(row.get(col))
        rows.append(d)
    return rows


def read_table_file(path: str) -> Tuple[pd.DataFrame, List[str]]:
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    if ext in ("xlsx",):
        df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    elif ext in ("csv",):
        df = _read_csv_robust(path)
    else:
        raise FileFormatError("仅支持 xlsx/csv")

    df.columns = [str(c).strip() for c in df.columns]
    columns = list(df.columns)
    return df, columns


def _read_csv_robust(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        sample = f.read(8192)
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))
        delimiter = dialect.delimiter or ","
    except Exception:
        delimiter = ","

    for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
        try:
            return pd.read_csv(path, encoding=enc, sep=delimiter)
        except Exception:
            continue
    return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")


def export_dataframe_bytes(df: pd.DataFrame, out_format: str) -> Tuple[bytes, str]:
    out_format = (out_format or "").lower()
    if out_format not in ("xlsx", "csv"):
        raise FileFormatError("输出格式仅支持 xlsx/csv")

    if out_format == "xlsx":
        bio = BytesIO()
        df.to_excel(bio, index=False, engine="openpyxl")
        return bio.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    content = df.to_csv(index=False, encoding="utf-8-sig")
    return content.encode("utf-8-sig"), "text/csv; charset=utf-8"

