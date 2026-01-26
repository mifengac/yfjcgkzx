"""
矛盾纠纷线索移交：DAO（人大金仓/Kingbase）。

说明：
- 连接复用项目统一的 `gonggong.config.database.get_database_connection`（psycopg2）。
- 支持按发生时间（a.fssj）、纠纷名称（a.jfmc 模糊）、分局名称（CASE 映射）筛选。
- 使用 DISTINCT ON (a.systemid) 并按 a.djsj 倒序取最新一条。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from gonggong.config.database import get_database_connection

FENJU_CASE = """
CASE
    WHEN substring(a.ssfj, 1, 6)= '445302' THEN '云城分局'
    WHEN substring(a.ssfj, 1, 6)= '445303' THEN '云安分局'
    WHEN substring(a.ssfj, 1, 6)= '445321' THEN '新兴县公安局'
    WHEN substring(a.ssfj, 1, 6)= '445381' THEN '罗定市公安局'
    WHEN substring(a.ssfj, 1, 6)= '445322' THEN '郁南县公安局'
    ELSE a.ssfj
END
""".strip()

DISTINCT_SQL = f"""
SELECT
    DISTINCT on(a.systemid)
    a.systemid AS 系统编号,
    a.ywlsh AS 业务流水号,
    a.jfmc AS 纠纷名称,
    c.detail AS 纠纷类型,
    a.jyqk AS 简要情况,
    a.fssj AS 发生时间,
    CASE
        WHEN a.sssj = '445300000000' THEN '云浮市公安局'
        ELSE a.sssj
    END AS 所属市局,
    {FENJU_CASE} AS 分局名称,
    e.sspcs AS 所属派出所,
    d.detail AS 流转状态,
    a.djsj AS 纠纷登记时间,
    a.djdw_mc AS 纠纷登记单位名称,
    a.xgsj AS 纠纷修改时间,
    b.yjqqsj AS 移交请求时间,
    g.detail AS 粤平安反馈状态,
    CASE
        WHEN b.tczt = '1' THEN '已化解'
        WHEN b.tczt = '0' THEN '未化解'
        ELSE b.tczt
    END AS 调处状态,
    b.rksj AS 入库时间,
    CASE
        WHEN b.orderstate = '2' THEN '已登记:已分发待确认'
        WHEN b.orderstate = '5' THEN '处理中:其他'
        WHEN b.orderstate = '6' THEN '已结案'
        WHEN b.orderstate = '4' THEN '处理中:业务系统已受理'
        ELSE b.orderstate
    END AS 粤平安流程节点状态,
    b.processtime AS 粤平安流程节点时间,
    round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2) AS 粤平安移交时间差,
    case
        when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=12  and b.yjqqsj is null then '12小时内未移交'
        when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=24  and b.yjqqsj is null then '24小时内未移交'
        when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=48  and b.yjqqsj is null then '48小时内未移交'
        when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=72  and b.yjqqsj is null then '72小时内未移交'
        when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)>72  and b.yjqqsj is null then '超出72小时仍未移交'
        when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=48 and b.yjqqsj is not null then '48小时内移交'
        when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=72 and b.yjqqsj is not null  then '72小时内移交'
        else '超出72小时移交'
    end as "12-24-48-72小时内移交情况"
FROM
    (
        SELECT
            *
        FROM
            stdata.b_per_mdjfjfsjgl
        WHERE
            deleteflag = '0'
            AND sfgazzfw = '0'
            AND djsj >= '2026-01-01'
    ) a
LEFT JOIN (
        SELECT
            *
        FROM
            stdata.b_per_mdjfypafhsj
        WHERE
            deleteflag = '0'
    ) b ON
    a.systemid = b.systemid
LEFT JOIN (
        SELECT
            code,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_JFLX'
    ) c ON
    a.jflx = c.code
LEFT JOIN (
        SELECT
            code,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_LCZT'
    ) d ON
    a.lczt = d.code
LEFT JOIN (
        SELECT
            code,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_YJFKZT'
    ) g ON
    b.yjfkzt = g.code
LEFT JOIN stdata.b_dic_zzjgdm e ON
    a.sspcs = e.sspcsdm
WHERE
    a.lczt<>'6'
ORDER BY
    a.systemid,
    a.djsj DESC
""".strip()


def _build_filtered_sql(
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
) -> Tuple[str, List[Any]]:
    sql = f"SELECT * FROM ({DISTINCT_SQL}) t WHERE 1=1"
    params: List[Any] = []

    if start_time is not None:
        sql += " AND t.\"发生时间\" >= %s"
        params.append(start_time)
    if end_time is not None:
        sql += " AND t.\"发生时间\" <= %s"
        params.append(end_time)
    if jfmc:
        sql += " AND t.\"纠纷名称\" ILIKE %s"
        params.append(f"%{jfmc}%")
    if fenju_list:
        sql += " AND t.\"分局名称\" IN %s"
        params.append(tuple(fenju_list))

    return sql, params


def query_mdj_xsyj_data(
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
    page: int,
    page_size: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    分页查询（返回 rows 与 total）。
    """
    base_sql, base_params = _build_filtered_sql(
        start_time=start_time, end_time=end_time, jfmc=jfmc, fenju_list=fenju_list
    )
    count_sql = f"SELECT count(*) FROM ({base_sql}) c"
    list_sql = base_sql + " ORDER BY t.\"纠纷登记时间\" DESC NULLS LAST LIMIT %s OFFSET %s"

    offset = max(page - 1, 0) * page_size
    list_params = [*base_params, page_size, offset]

    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(count_sql, base_params)
            total = int(cur.fetchone()[0] or 0)

            cur.execute(list_sql, list_params)
            rows_raw = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, r)) for r in rows_raw]

        return rows, total
    finally:
        conn.close()


def get_all_mdj_xsyj_data(
    *,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    jfmc: Optional[str],
    fenju_list: Optional[List[str]],
) -> List[Dict[str, Any]]:
    """
    导出用：不分页，按登记时间倒序输出全部数据。
    """
    base_sql, base_params = _build_filtered_sql(
        start_time=start_time, end_time=end_time, jfmc=jfmc, fenju_list=fenju_list
    )
    sql = base_sql + " ORDER BY t.\"纠纷登记时间\" DESC NULLS LAST"

    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, base_params)
            rows_raw = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, r)) for r in rows_raw]
    finally:
        conn.close()
