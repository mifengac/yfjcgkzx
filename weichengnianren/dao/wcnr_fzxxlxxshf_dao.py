from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


def _as_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


BASE_CTE_SQL = """
WITH base_data AS (
    SELECT
        c."姓名",
        c."证件号码",
        EXTRACT(
            YEAR FROM AGE(current_date, TO_DATE(SUBSTRING(c."证件号码" FROM 7 FOR 8), 'YYYYMMDD'))
        )::int AS "周岁",
        TO_CHAR(c.lxsj_raw, 'YYYY-MM-DD HH24:MI:SS') AS "离校时间",
        c."是否属于满一年无异常无需走访",
        TO_CHAR(c.sjdjsj_raw, 'YYYY-MM-DD HH24:MI:SS') AS "数据登记时间",
        c."分局名称",
        c."派出所名称",
        d."是否需回访",
        d."不需回访原因",
        TO_CHAR(d.hfrq_raw, 'YYYY-MM-DD HH24:MI:SS') AS "回访日期",
        f.detail AS "回访方式",
        d."其他回访方式",
        TO_CHAR(d.hfdjsj_raw, 'YYYY-MM-DD HH24:MI:SS') AS "回访系统登记时间",
        TO_CHAR(e.lastdjsj_raw, 'YYYY-MM-DD HH24:MI:SS') AS "最后一次回访时间",
        DATE_PART('day', current_date - e.lastdjsj_raw::date) AS "距离最后一次走访天数差",
        d.hfdjsj_raw,
        c.sjdjsj_raw
    FROM (
        SELECT
            id,
            xm AS "姓名",
            gmsfzhm AS "证件号码",
            "wczmjzjyjylxsj" AS lxsj_raw,
            "sfsymynwycwxjxzf" AS "是否属于满一年无异常无需走访",
            djsj AS sjdjsj_raw,
            CASE
                WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE a.sjgsdwdm
            END AS "分局名称",
            b.sspcs AS "派出所名称"
        FROM stdata.b_per_fzxxlxxshf a
        LEFT JOIN ywdata.b_zzjgdm b ON a.sjgsdwdm = b.sspcsdm
        WHERE a.deleteflag = 0
    ) c
    LEFT JOIN (
        SELECT
            flid,
            sfxhf AS "是否需回访",
            "bxhfyy" AS "不需回访原因",
            hfrq AS hfrq_raw,
            hfxs AS "回访方式代码",
            CASE WHEN hfxs = '06' THEN qthffs ELSE '' END AS "其他回访方式",
            djsj AS hfdjsj_raw
        FROM stdata.b_per_fzxxlxxshf_hfdjb
        WHERE deleteflag = 0
    ) d ON c.id = d.flid
    LEFT JOIN (
        SELECT
            flid,
            max(djsj) AS lastdjsj_raw
        FROM stdata.b_per_fzxxlxxshf_hfdjb
        WHERE deleteflag = 0
        GROUP BY flid
    ) e ON c.id = e.flid
    LEFT JOIN (
        SELECT
            kind_code,
            code,
            detail
        FROM stdata.s_sg_dict
        WHERE kind_code = 'wcnrjzjyzfxs'
    ) f ON d."回访方式代码" = f.code
)
"""


VISIBLE_COLUMNS_SQL = """
SELECT
    b."姓名",
    b."证件号码",
    b."周岁",
    b."离校时间",
    b."是否属于满一年无异常无需走访",
    b."数据登记时间",
    b."分局名称",
    b."派出所名称",
    b."是否需回访",
    b."不需回访原因",
    b."回访日期",
    b."回访方式",
    b."其他回访方式",
    b."回访系统登记时间",
    b."最后一次回访时间",
    b."距离最后一次走访天数差"
FROM base_data b
"""


def _build_filters(
    *,
    start_time: str,
    end_time: str,
    branches: Optional[Sequence[str]] = None,
) -> Tuple[str, List[Any]]:
    clauses = [
        "b.hfdjsj_raw >= %s",
        "b.hfdjsj_raw <= %s",
    ]
    params: List[Any] = [start_time, end_time]

    branch_list = [x.strip() for x in (branches or []) if x and x.strip()]
    if branch_list:
        clauses.append('b."分局名称" = ANY(%s)')
        params.append(branch_list)

    return " WHERE " + " AND ".join(clauses), params


def query_fzxxlxxshf_page(
    conn,
    *,
    start_time: str,
    end_time: str,
    branches: Optional[Sequence[str]] = None,
    page: int,
    page_size: int,
) -> Tuple[List[Dict[str, Any]], int]:
    where_sql, params = _build_filters(start_time=start_time, end_time=end_time, branches=branches)
    offset = max(page - 1, 0) * page_size

    count_sql = BASE_CTE_SQL + "SELECT COUNT(*) FROM base_data b" + where_sql
    query_sql = (
        BASE_CTE_SQL
        + VISIBLE_COLUMNS_SQL
        + where_sql
        + " ORDER BY b.hfdjsj_raw DESC NULLS LAST, b.sjdjsj_raw DESC NULLS LAST LIMIT %s OFFSET %s"
    )

    with conn.cursor() as cur:
        cur.execute(count_sql, tuple(params))
        total = int(cur.fetchone()[0] or 0)

        query_params = list(params)
        query_params.extend([page_size, offset])
        cur.execute(query_sql, tuple(query_params))
        rows = _as_dict_rows(cur)

    return rows, total


def query_fzxxlxxshf_all(
    conn,
    *,
    start_time: str,
    end_time: str,
    branches: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    where_sql, params = _build_filters(start_time=start_time, end_time=end_time, branches=branches)
    query_sql = (
        BASE_CTE_SQL
        + VISIBLE_COLUMNS_SQL
        + where_sql
        + " ORDER BY b.hfdjsj_raw DESC NULLS LAST, b.sjdjsj_raw DESC NULLS LAST"
    )
    with conn.cursor() as cur:
        cur.execute(query_sql, tuple(params))
        return _as_dict_rows(cur)
