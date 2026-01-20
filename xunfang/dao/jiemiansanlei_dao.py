"""
街面三类警情（地址分类）数据访问层。

从人大金仓（PostgreSQL 协议）读取警情数据与警情性质配置。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection

SourceType = Literal["原始", "确认"]


@dataclass(frozen=True)
class JiemianSanleiQuery:
    start_time: str
    end_time: str
    leixing_list: Sequence[str]
    source_list: Sequence[SourceType]
    limit: Optional[int] = None
    offset: int = 0


def list_case_types() -> List[str]:
    """
    获取警情性质下拉列表（case_type_config.leixing）。
    """
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT leixing FROM "ywdata"."case_type_config" ORDER BY leixing')
            return [row[0] for row in cur.fetchall() if row and row[0] is not None]
    finally:
        conn.close()


def count_jingqings(query: JiemianSanleiQuery) -> int:
    sql, params = _build_union_sql(query, count_only=True)
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def fetch_jingqings(query: JiemianSanleiQuery) -> List[Dict[str, Any]]:
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


def fetch_jingqings_for_export(
    *,
    start_time: str,
    end_time: str,
    leixing: str,
    source: SourceType,
) -> List[Dict[str, Any]]:
    query = JiemianSanleiQuery(
        start_time=start_time,
        end_time=end_time,
        leixing_list=[leixing],
        source_list=[source],
        limit=None,
        offset=0,
    )
    return fetch_jingqings(query)


def _build_union_sql(query: JiemianSanleiQuery, *, count_only: bool) -> Tuple[str, List[Any]]:
    """
    构造 union 查询：
    - 支持 source 多选（原始/确认）
    - 支持 leixing 多选
    - 支持 limit/offset（分页）
    """
    selects: List[str] = []
    params: List[Any] = []

    leixing_list = list(query.leixing_list or [])
    source_list = list(query.source_list or [])

    if not leixing_list or not source_list:
        if count_only:
            return "SELECT 0", []
        return (
            "SELECT ''::text AS leixing, ''::text AS yuanshiqueren, ''::text AS 分局, "
            "''::text AS 派出所编号, ''::text AS 派出所名称, "
            "NULL::timestamp AS 报警时间, ''::text AS 警情地址, ''::text AS jq_type "
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

        selects.append(
            f"""
            SELECT
                ctc.leixing AS leixing,
                %s::text AS yuanshiqueren,
                jq.cmdname AS 分局,
                jq.dutydeptno AS 派出所编号,
                jq.dutydeptname AS 派出所名称,
                jq.calltime AS 报警时间,
                jq.occuraddress AS 警情地址,
                jq.{type_name_col} AS jq_type
            FROM "ywdata"."case_type_config" ctc
            JOIN LATERAL UNNEST(ctc.newcharasubclass_list) AS subclass(code) ON TRUE
            JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
              ON jq.{subclass_col} = subclass.code
            WHERE ctc.leixing = ANY(%s)
              AND jq.calltime BETWEEN %s AND %s
            """
        )
        params.extend([source, leixing_list, query.start_time, query.end_time])

    union_sql = "\nUNION ALL\n".join(selects)

    if count_only:
        return f"SELECT COUNT(1) FROM ({union_sql}) t", params

    final_sql = f"""
    SELECT leixing, yuanshiqueren, 分局, 派出所编号, 派出所名称, 报警时间, 警情地址, jq_type
    FROM ({union_sql}) t
    ORDER BY 报警时间 DESC
    """
    if query.limit is not None:
        final_sql += " LIMIT %s OFFSET %s"
        params.extend([query.limit, query.offset])

    return final_sql, params
