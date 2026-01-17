from __future__ import annotations

from typing import Optional

import pandas as pd

from gonggong.config.database import DB_CONFIG, get_database_connection


DEFAULT_GZRZ_SQL = """
SELECT
  a.xm as 姓名,
  a.zjhm as 证件号码,
  CASE
    WHEN substring(a.lgdw,1,6)='445302' THEN '云城'
    WHEN substring(a.lgdw,1,6)='445303' THEN '云安'
    WHEN substring(a.lgdw,1,6)='445321' THEN '新兴'
    WHEN substring(a.lgdw,1,6)='445322' THEN '郁南'
    WHEN substring(a.lgdw,1,6)='445381' THEN '罗定'
    ELSE a.lgdw
  END AS 分局名称,
  b.sspcs AS 所属派出所,
  a.lgsj AS 列管时间,
  c.kzgzsj AS 工作日志开展工作时间,
  d.detail AS 工作日志工作类型,
  c.gzqksm AS 工作日志工作情况说明,
  c.djsj AS 工作日志系统登记时间
FROM (SELECT * FROM stdata.b_per_mdjffxrygl WHERE "deleteflag"='0' AND gkzt='01') a
LEFT JOIN stdata.b_dic_zzjgdm b ON a.lgdw = b.sspcsdm
LEFT JOIN (SELECT * FROM stdata.b_zdry_ryxx_gzrz WHERE deleteflag='0') c ON a.systemid = c.zdryid
LEFT JOIN (SELECT * FROM stdata.s_sg_dict WHERE kind_code='ZAZDRY_GZRZ_GZLX') d ON c.gzlx = d.code
WHERE c.kzgzsj >= '2025-1-1'
""".strip()


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

