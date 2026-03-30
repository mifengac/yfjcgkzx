from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection

SourceType = Literal["原始", "确认"]


@dataclass(frozen=True)
class JiemianSanleiDbQuery:
    start_time: str
    end_time: str
    leixing_list: Sequence[str]
    source_list: Sequence[SourceType]
    minor_only: bool = False
    limit: Optional[int] = None
    offset: int = 0


def list_case_types() -> List[str]:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT leixing FROM "ywdata"."case_type_config" ORDER BY leixing')
            return [str(row[0]).strip() for row in cur.fetchall() if row and str(row[0]).strip()]
    finally:
        conn.close()


def get_case_type_code_map(leixing_list: Optional[Sequence[str]] = None) -> Dict[str, List[str]]:
    sql = 'SELECT leixing, newcharasubclass_list FROM "ywdata"."case_type_config"'
    params: List[object] = []

    normalized_leixing = [str(item).strip() for item in (leixing_list or []) if str(item).strip()]
    if normalized_leixing:
        sql += " WHERE leixing = ANY(%s)"
        params.append(normalized_leixing)
    sql += " ORDER BY leixing"

    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    result: Dict[str, List[str]] = {}
    for leixing, raw_codes in rows:
        key = str(leixing or "").strip()
        if not key:
            continue
        result[key] = _normalize_code_list(raw_codes)
    return result


def _normalize_code_list(value: object) -> List[str]:
    if value is None:
        return []

    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            text = text[1:-1]
        raw_items: Iterable[object] = text.split(",") if text else []
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]

    codes: List[str] = []
    seen = set()
    for item in raw_items:
        code = str(item or "").strip().strip('"').strip("'")
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def count_db_jingqings(query: JiemianSanleiDbQuery) -> int:
    sql, params = _build_union_sql(query, count_only=True)
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def fetch_db_jingqings(query: JiemianSanleiDbQuery) -> List[Dict[str, Any]]:
    sql, params = _build_union_sql(query, count_only=False)
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def fetch_db_jingqings_for_export(
    *,
    start_time: str,
    end_time: str,
    leixing: str,
    source: SourceType,
    minor_only: bool = False,
) -> List[Dict[str, Any]]:
    return fetch_db_jingqings(
        JiemianSanleiDbQuery(
            start_time=start_time,
            end_time=end_time,
            leixing_list=[leixing],
            source_list=[source],
            minor_only=minor_only,
            limit=None,
            offset=0,
        )
    )


def _build_union_sql(query: JiemianSanleiDbQuery, *, count_only: bool) -> Tuple[str, List[Any]]:
    selects: List[str] = []
    params: List[Any] = []

    leixing_list = list(query.leixing_list or [])
    source_list = list(query.source_list or [])

    if not leixing_list or not source_list:
        if count_only:
            return "SELECT 0", []
        return (
            "SELECT ''::text AS caseno, ''::text AS leixing, ''::text AS yuanshiqueren, ''::text AS 分局, "
            "''::text AS 派出所编号, ''::text AS 派出所名称, "
            "NULL::timestamp AS 报警时间, ''::text AS 警情地址, ''::text AS 报警内容, "
            "''::text AS 处警情况, NULL::double precision AS 经度, NULL::double precision AS 纬度, ''::text AS jq_type "
            "WHERE 1=0",
            [],
        )

    for source in source_list:
        if source == "原始":
            subclass_col = "neworicharasubclass"
            type_name_col = "neworicharasubcategoryname"
        else:
            subclass_col = "newcharasubclass"
            type_name_col = "newcharasubcategoryname"

        branch_where = [
            "ctc.leixing = ANY(%s)",
            "jq.calltime BETWEEN %s AND %s",
        ]
        branch_params: List[Any] = [source, leixing_list, query.start_time, query.end_time]

        if query.minor_only:
            branch_where.append(
                "(COALESCE(jq.casemark, '') ~ %s OR COALESCE(jq.casemarkok, '') ~ %s)"
            )
            branch_params.extend(["未成年", "未成年"])

        selects.append(
            f"""
            SELECT
                jq.caseno,
                ctc.leixing AS leixing,
                %s::text AS yuanshiqueren,
                jq.cmdname AS 分局,
                jq.dutydeptno AS 派出所编号,
                jq.dutydeptname AS 派出所名称,
                jq.calltime AS 报警时间,
                jq.occuraddress AS 警情地址,
                jq.casecontents AS 报警内容,
                jq.replies AS 处警情况,
                jq.lngofcriterion AS 经度,
                jq.latofcriterion AS 纬度,
                jq.{type_name_col} AS jq_type
            FROM "ywdata"."case_type_config" ctc
            JOIN LATERAL UNNEST(ctc.newcharasubclass_list) AS subclass(code) ON TRUE
            JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
              ON jq.{subclass_col} = subclass.code
            WHERE {' AND '.join(branch_where)}
            """
        )
        params.extend(branch_params)

    union_sql = "\nUNION ALL\n".join(selects)

    if count_only:
        return f"SELECT COUNT(1) FROM ({union_sql}) t", params

    final_sql = f"""
    SELECT caseno, leixing, yuanshiqueren, 分局, 派出所编号, 派出所名称, 报警时间, 警情地址, 报警内容, 处警情况, 经度, 纬度, jq_type
    FROM ({union_sql}) t
    ORDER BY 报警时间 DESC
    """
    if query.limit is not None:
        final_sql += " LIMIT %s OFFSET %s"
        params.extend([query.limit, query.offset])
    return final_sql, params
