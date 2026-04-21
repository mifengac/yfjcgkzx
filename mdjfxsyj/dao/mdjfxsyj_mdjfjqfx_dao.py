from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from gonggong.config.database import get_database_connection


def _fetch_all(sql: str, params: Sequence[Any] | None = None) -> List[Dict[str, Any]]:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params or ()))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def query_branch_options() -> List[Dict[str, str]]:
    sql = """
    SELECT DISTINCT ON ("ssfjdm")
        "ssfjdm" AS value,
        COALESCE(NULLIF(BTRIM("ssfj"), ''), "ssfjdm") AS label
    FROM "stdata"."b_dic_zzjgdm"
    WHERE NULLIF(BTRIM(COALESCE("ssfjdm", '')), '') IS NOT NULL
    ORDER BY "ssfjdm", "rksj" DESC NULLS LAST, "ssfj" NULLS LAST
    """
    return [
        {"value": str(row.get("value") or ""), "label": str(row.get("label") or "")}
        for row in _fetch_all(sql)
        if row.get("value")
    ]


def query_org_mappings() -> Tuple[Dict[str, str], Dict[str, str]]:
    sql = """
    SELECT DISTINCT ON ("sspcsdm")
        "ssfjdm",
        COALESCE(NULLIF(BTRIM("ssfj"), ''), "ssfjdm") AS "ssfj",
        "sspcsdm",
        COALESCE(NULLIF(BTRIM("sspcs"), ''), "sspcsdm") AS "sspcs"
    FROM "stdata"."b_dic_zzjgdm"
    WHERE NULLIF(BTRIM(COALESCE("sspcsdm", '')), '') IS NOT NULL
    ORDER BY "sspcsdm", "rksj" DESC NULLS LAST
    """
    branch_map: Dict[str, str] = {}
    station_map: Dict[str, str] = {}
    for row in _fetch_all(sql):
        ssfjdm = str(row.get("ssfjdm") or "").strip()
        sspcsdm = str(row.get("sspcsdm") or "").strip()
        if ssfjdm and ssfjdm not in branch_map:
            branch_map[ssfjdm] = str(row.get("ssfj") or ssfjdm)
        if sspcsdm:
            station_map[sspcsdm] = str(row.get("sspcs") or sspcsdm)
    return branch_map, station_map


def query_case_conversion_map(case_numbers: Iterable[str]) -> Dict[str, List[str]]:
    clean_numbers = sorted({str(value or "").strip() for value in case_numbers if str(value or "").strip()})
    if not clean_numbers:
        return {}

    result: Dict[str, List[str]] = defaultdict(list)
    chunk_size = 1000
    sql = """
    SELECT "ajxx_jqbh" AS case_no, "ajxx_ajbh" AS case_id
    FROM "ywdata"."zq_zfba_ajxx"
    WHERE "ajxx_jqbh" = ANY(%s)
      AND NULLIF(BTRIM(COALESCE("ajxx_jqbh", '')), '') IS NOT NULL
    ORDER BY "ajxx_jqbh", "ajxx_ajbh"
    """
    for index in range(0, len(clean_numbers), chunk_size):
        chunk = clean_numbers[index : index + chunk_size]
        for row in _fetch_all(sql, [chunk]):
            case_no = str(row.get("case_no") or "").strip()
            case_id = str(row.get("case_id") or "").strip()
            if case_no and case_id and case_id not in result[case_no]:
                result[case_no].append(case_id)
    return dict(result)
