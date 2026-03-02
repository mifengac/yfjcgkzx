"""
矛盾纠纷重复报警统计：DAO 层

数据源：ywdata.v_b_jq_xjzd2025
- 子查询与外层查询均使用前端传入的 start_time / end_time 进行过滤
- 分局过滤通过正则 ~ ANY(patterns) 实现
- 报警次数阈值（min_cs）过滤报警电话次数
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# --------------------------------------------------------------------------
# 基础 SQL（CTE）
# --------------------------------------------------------------------------

_BASE_CTE = """
WITH base AS (
    SELECT
        a.jjdbh                         AS 警情编号,
        a.jqxzmc                        AS 警情性质,
        a.ysjqxzmc                      AS 原始警情性质,
        a.bjsj                          AS 报警时间,
        a.bjrlxdh                       AS 报警人联系电话,
        b.cs                            AS 报警电话次数,
        a.afdd                          AS 报警地址,
        a.bjnr                          AS 报警内容,
        a.cjqk                          AS 处警情况,
        '云浮市公安局'                   AS 所属市局,
        CASE
            WHEN substring(a.gxdwmc, 1, 4) = '云城分局' THEN '1云城分局'
            WHEN substring(a.gxdwmc, 1, 4) = '云安分局' THEN '2云安分局'
            WHEN substring(a.gxdwmc, 1, 4) = '罗定市局' THEN '3罗定市局'
            WHEN substring(a.gxdwmc, 1, 4) = '新兴县局' THEN '4新兴县局'
            WHEN substring(a.gxdwmc, 1, 4) = '郁南县局' THEN '5郁南县局'
            WHEN substring(a.gxdwmc, 1, 4) = '云浮市局' THEN '6云浮市局'
            ELSE a.gxdwmc
        END                             AS 所属分局,
        substring(a.gxdwmc, 5, 5)      AS 所属派出所
    FROM ywdata.v_b_jq_xjzd2025 a
    LEFT JOIN (
        SELECT bjrlxdh, count(*) AS cs
        FROM ywdata.v_b_jq_xjzd2025
        WHERE bjsj >= %s AND bjsj < %s
        GROUP BY bjrlxdh
    ) b ON a.bjrlxdh = b.bjrlxdh
    WHERE a.bjsj >= %s AND a.bjsj < %s
)
"""

# Inner SELECT body for one time period (reused in multi-period summary SQL)
# 4 %s placeholders: period_start, period_end, period_start, period_end
_PERIOD_BLOCK = """
    SELECT
        a.jjdbh                         AS 警情编号,
        a.jqxzmc                        AS 警情性质,
        a.ysjqxzmc                      AS 原始警情性质,
        a.bjsj                          AS 报警时间,
        a.bjrlxdh                       AS 报警人联系电话,
        b.cs                            AS 报警电话次数,
        a.afdd                          AS 报警地址,
        a.bjnr                          AS 报警内容,
        a.cjqk                          AS 处警情况,
        '云浮市公安局'                   AS 所属市局,
        CASE
            WHEN substring(a.gxdwmc, 1, 4) = '云城分局' THEN '1云城分局'
            WHEN substring(a.gxdwmc, 1, 4) = '云安分局' THEN '2云安分局'
            WHEN substring(a.gxdwmc, 1, 4) = '罗定市局' THEN '3罗定市局'
            WHEN substring(a.gxdwmc, 1, 4) = '新兴县局' THEN '4新兴县局'
            WHEN substring(a.gxdwmc, 1, 4) = '郁南县局' THEN '5郁南县局'
            WHEN substring(a.gxdwmc, 1, 4) = '云浮市局' THEN '6云浮市局'
            ELSE a.gxdwmc
        END                             AS 所属分局,
        substring(a.gxdwmc, 5, 5)      AS 所属派出所
    FROM ywdata.v_b_jq_xjzd2025 a
    LEFT JOIN (
        SELECT bjrlxdh, count(*) AS cs
        FROM ywdata.v_b_jq_xjzd2025
        WHERE bjsj >= %s AND bjsj < %s
        GROUP BY bjrlxdh
    ) b ON a.bjrlxdh = b.bjrlxdh
    WHERE a.bjsj >= %s AND a.bjsj < %s
"""


def _build_where(
    *,
    fenju_patterns: Optional[List[str]],
    min_cs: Optional[int],
) -> Tuple[str, List[Any]]:
    """返回 WHERE 子句片段和对应参数列表（不含 base CTE 占位符）"""
    clauses: List[str] = []
    params: List[Any] = []

    if fenju_patterns:
        clauses.append("所属分局 ~ ANY(%s)")
        params.append(fenju_patterns)

    if min_cs is not None and min_cs >= 1:
        clauses.append("报警电话次数 >= %s")
        params.append(min_cs)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# --------------------------------------------------------------------------
# 分组统计查询
# --------------------------------------------------------------------------

def fetch_cfbj_summary(
    conn,
    *,
    start_time: str,
    end_time: str,
    tongbi_start: str,
    tongbi_end: str,
    huanbi_start: str,
    huanbi_end: str,
    fenju_patterns: Optional[List[str]] = None,
    min_cs: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """按 所属分局 分组，返回 总数/同比/环比/同比比例/环比比例/重复数/发生率 统计列表（末行为总计）。"""
    where, extra_params = _build_where(fenju_patterns=fenju_patterns, min_cs=min_cs)

    sql = f"""
WITH
base           AS ({_PERIOD_BLOCK}),
tongbi_base    AS ({_PERIOD_BLOCK}),
huanbi_base    AS ({_PERIOD_BLOCK}),
filtered AS (
    SELECT * FROM base {where}
),
tongbi_filtered AS (
    SELECT * FROM tongbi_base {where}
),
huanbi_filtered AS (
    SELECT * FROM huanbi_base {where}
),
tongbi_agg AS (
    SELECT 所属分局, count(*) AS cnt FROM tongbi_filtered GROUP BY 所属分局
),
huanbi_agg AS (
    SELECT 所属分局, count(*) AS cnt FROM huanbi_filtered GROUP BY 所属分局
)
SELECT
    f.所属分局,
    count(*)::bigint                                                                    AS 总数,
    COALESCE(ta.cnt, 0)::bigint                                                         AS 同比,
    COALESCE(ha.cnt, 0)::bigint                                                         AS 环比,
    CASE WHEN COALESCE(ta.cnt, 0) = 0 THEN NULL
         ELSE round((count(*)::numeric - ta.cnt) / ta.cnt, 4)
    END                                                                                 AS 同比比例,
    CASE WHEN COALESCE(ha.cnt, 0) = 0 THEN NULL
         ELSE round((count(*)::numeric - ha.cnt) / ha.cnt, 4)
    END                                                                                 AS 环比比例,
    count(CASE WHEN f.报警电话次数 > 2 THEN 1 END)::bigint                              AS 重复数,
    CASE WHEN count(*) = 0 THEN 0
         ELSE round(count(CASE WHEN f.报警电话次数 > 2 THEN 1 END)::numeric / count(*), 4)
    END                                                                                 AS 发生率,
    1 AS _sort
FROM filtered f
LEFT JOIN tongbi_agg ta ON f.所属分局 = ta.所属分局
LEFT JOIN huanbi_agg ha ON f.所属分局 = ha.所属分局
GROUP BY f.所属分局, ta.cnt, ha.cnt
UNION ALL
SELECT
    '总计',
    (SELECT count(*) FROM filtered)::bigint,
    (SELECT count(*) FROM tongbi_filtered)::bigint,
    (SELECT count(*) FROM huanbi_filtered)::bigint,
    CASE WHEN (SELECT count(*) FROM tongbi_filtered) = 0 THEN NULL
         ELSE round(
             ((SELECT count(*) FROM filtered)::numeric - (SELECT count(*) FROM tongbi_filtered))
             / (SELECT count(*) FROM tongbi_filtered)::numeric, 4
         )
    END,
    CASE WHEN (SELECT count(*) FROM huanbi_filtered) = 0 THEN NULL
         ELSE round(
             ((SELECT count(*) FROM filtered)::numeric - (SELECT count(*) FROM huanbi_filtered))
             / (SELECT count(*) FROM huanbi_filtered)::numeric, 4
         )
    END,
    (SELECT count(CASE WHEN 报警电话次数 > 2 THEN 1 END) FROM filtered)::bigint,
    CASE WHEN (SELECT count(*) FROM filtered) = 0 THEN 0
         ELSE round(
             (SELECT count(CASE WHEN 报警电话次数 > 2 THEN 1 END) FROM filtered)::numeric
             / (SELECT count(*) FROM filtered)::numeric, 4
         )
    END,
    2
ORDER BY _sort, 所属分局
"""
    # Parameter order:
    #   base (4) + tongbi_base (4) + huanbi_base (4)
    #   + filtered WHERE (extra_params)
    #   + tongbi_filtered WHERE (extra_params)
    #   + huanbi_filtered WHERE (extra_params)
    params: List[Any] = (
        [start_time, end_time, start_time, end_time]
        + [tongbi_start, tongbi_end, tongbi_start, tongbi_end]
        + [huanbi_start, huanbi_end, huanbi_start, huanbi_end]
        + extra_params
        + extra_params
        + extra_params
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    result = []
    for row in rows:
        d = dict(zip(cols, row))
        d.pop('_sort', None)
        result.append(d)
    return result


# --------------------------------------------------------------------------
# 明细查询
# --------------------------------------------------------------------------

def fetch_cfbj_detail(
    conn,
    *,
    start_time: str,
    end_time: str,
    fenju_patterns: Optional[List[str]] = None,
    min_cs: Optional[int] = None,
    fenju_exact: Optional[str] = None,
    detail_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    返回明细数据行。

    fenju_exact: 精确分局名（含前缀数字，如 '1云城分局'），供弹出框按分局筛选
    detail_type: '总数' | '重复数'，供弹出框区分是否只看报警电话次数 > 2 的行
    """
    where, extra_params = _build_where(fenju_patterns=fenju_patterns, min_cs=min_cs)

    extra_clauses: List[str] = []

    if fenju_exact:
        extra_clauses.append("所属分局 = %s")
        extra_params.append(fenju_exact)

    if detail_type == "重复数":
        extra_clauses.append("报警电话次数 > 2")

    if extra_clauses:
        if where:
            where = where + " AND " + " AND ".join(extra_clauses)
        else:
            where = "WHERE " + " AND ".join(extra_clauses)

    sql = (
        _BASE_CTE
        + f"""
SELECT
    警情编号, 警情性质, 原始警情性质, 报警时间, 报警人联系电话,
    报警电话次数, 报警地址, 报警内容, 处警情况,
    所属市局, 所属分局, 所属派出所
FROM base
{where}
ORDER BY 报警时间 DESC
"""
    )
    params: List[Any] = [start_time, end_time, start_time, end_time] + extra_params

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    return [dict(zip(cols, row)) for row in rows]
