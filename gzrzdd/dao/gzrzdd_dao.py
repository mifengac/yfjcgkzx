from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from gonggong.config.database import DB_CONFIG, get_database_connection


def ensure_select_only(sql: str) -> None:
    s = (sql or "").strip().lstrip("\ufeff")
    s_lower = s.lower()
    if not (s_lower.startswith("select") or s_lower.startswith("with")):
        raise ValueError("仅允许 SELECT/WITH 查询")
    if ";" in s_lower:
        raise ValueError("SQL 中不允许包含分号 ';'")
    forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "create "]
    if any(k in s_lower for k in forbidden):
        raise ValueError("SQL 中包含禁止关键字")


def query_to_dataframe(sql: str) -> pd.DataFrame:
    ensure_select_only(sql)
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            schema = (DB_CONFIG.get("schema") or "").strip()
            if schema:
                cur.execute(f"SET search_path TO {schema};")
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def find_col(df: pd.DataFrame, want: str) -> Optional[str]:
    w = (want or "").strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == w:
            return str(c)
    return None

