from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from psycopg2 import sql

from gonggong.config.database import DB_CONFIG


SCHEMA = DB_CONFIG.get("schema") or "ywdata"


def fetch_leixing_list(conn) -> List[str]:
    """获取类型下拉框列表"""
    with conn.cursor() as cur:
        cur.execute(
            '''SELECT DISTINCT "leixing" FROM "ywdata"."case_type_config" ORDER BY "leixing"'''
        )
        rows = cur.fetchall()
    return [str(r[0]) for r in rows if r[0]]


def fetch_ay_patterns(conn, leixing_list: Sequence[str]) -> List[str]:
    """根据类型列表获取案由匹配模式"""
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    if not leixing_list:
        return []
    with conn.cursor() as cur:
        cur.execute(
            '''SELECT "ay_pattern" FROM "ywdata"."case_type_config" WHERE "leixing" = ANY(%s)''',
            (list(leixing_list),),
        )
        rows = cur.fetchall()
    return [str(r[0]) for r in rows if r[0]]


def fetch_jzqk_data(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> List[Dict[str, Any]]:
    """
    执行完整 SQL 查询，返回详细数据列表
    使用提供的原 SQL（包含 base_data, jzws_info, fhss_detail 等 CTE）
    """
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]

    # 构建类型匹配条件
    if leixing_list:
        # 获取案由模式
        patterns = fetch_ay_patterns(conn, leixing_list)
        if not patterns:
            return []
        type_condition = sql.SQL(
            ''' AND wcn."xyrxx_ay_mc" SIMILAR TO (SELECT ctc."ay_pattern" FROM "ywdata"."case_type_config" ctc WHERE ctc."leixing" = ANY(%s))'''
        )
        type_params = [leixing_list]
    else:
        type_condition = sql.SQL("")
        type_params = []

    query = sql.SQL(
        """
        WITH base_data AS (
            -- 基础数据（按身份证号和案件编号去重）
            SELECT DISTINCT ON (wcn."xyrxx_sfzh", wcn."ajxx_join_ajxx_ajbh")
                wcn."ajxx_join_ajxx_ajbh",
                wcn."ajxx_join_ajxx_ajlx",
                wcn."ajxx_join_ajxx_ajmc",
                wcn."ajxx_join_ajxx_cbqy_jc",
                wcn."ajxx_join_ajxx_cbdw_bh",
                wcn."ajxx_join_ajxx_lasj",
                wcn."xyrxx_sfzh",
                wcn."xyrxx_xm",
                wcn."xyrxx_nl",
                wcn."xyrxx_hjdxz",
                wcn."xyrxx_jzdxzqh",
                wcn."xyrxx_rybh"
            FROM "ywdata"."zq_zfba_wcnr_xyr" wcn
            WHERE wcn."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
            {type_condition}
            ORDER BY wcn."xyrxx_sfzh", wcn."ajxx_join_ajxx_ajbh", wcn."ajxx_join_ajxx_lasj" DESC
        ),

        /* 收敛身份证集合，后续统计只算这批人，避免慢 */
        target_sfzh AS (
            SELECT DISTINCT bd."xyrxx_sfzh" AS xyrxx_sfzh
            FROM base_data bd
        ),

        /* 矫治文书判断 */
        jzws_info AS (
            SELECT
                bd."ajxx_join_ajxx_ajbh",
                bd."xyrxx_sfzh",
                CASE
                    WHEN xjs.ajbh IS NOT NULL AND zltzs.zltzs_ajbh IS NOT NULL THEN '训诫书/责令通知书'
                    WHEN xjs.ajbh IS NOT NULL THEN '训诫书'
                    WHEN zltzs.zltzs_ajbh IS NOT NULL THEN '责令通知书'
                    ELSE NULL
                END AS jzws_name,
                CASE
                    WHEN xjs.ajbh IS NOT NULL OR zltzs.zltzs_ajbh IS NOT NULL THEN '是'
                    ELSE '否'
                END AS has_jzws
            FROM base_data bd
            LEFT JOIN "ywdata"."zq_zfba_xjs2" xjs
                ON bd."ajxx_join_ajxx_ajbh" = xjs.ajbh
            AND bd."xyrxx_xm" = xjs.xgry_xm
            LEFT JOIN "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" zltzs
                ON bd."ajxx_join_ajxx_ajbh" = zltzs.zltzs_ajbh
            AND bd."xyrxx_sfzh" = zltzs.zltzs_sfzh
        ),

        /* 条件1：行政处罚决定书治拘天数 >= 5日 */
        fhss_xzcfjds AS (
            SELECT DISTINCT
                bd."ajxx_join_ajxx_ajbh",
                bd."xyrxx_sfzh"
            FROM base_data bd
            INNER JOIN "ywdata"."zq_zfba_xzcfjds" xzcf
                ON bd."ajxx_join_ajxx_ajbh" = xzcf.ajxx_ajbh
            AND bd."xyrxx_rybh" = xzcf.xzcfjds_rybh
            WHERE xzcf.xzcfjds_tj_jlts::INTEGER > 4
        ),

        /* 前科统计：只统计 target_sfzh 这些人的历史（按身份证） */
        ay_stats AS (
            SELECT
                w.xyrxx_sfzh,
                COUNT(*) AS total_cnt,
                COUNT(DISTINCT w.xyrxx_ay_mc) AS distinct_ay_cnt
            FROM "ywdata"."zq_zfba_wcnr_xyr" w
            INNER JOIN target_sfzh t
                ON t.xyrxx_sfzh = w.xyrxx_sfzh
            GROUP BY w.xyrxx_sfzh
        ),

        /* 记录证（用于判断刑事是否刑拘：有记录证 => 非未刑拘） */
        fhss_jlz AS (
            SELECT DISTINCT
                bd."ajxx_join_ajxx_ajbh",
                bd."xyrxx_sfzh"
            FROM base_data bd
            INNER JOIN "ywdata"."zq_zfba_jlz" jlz
                ON bd."ajxx_join_ajxx_ajbh" = jlz.ajxx_ajbh
            AND bd."xyrxx_rybh" = jlz.jlz_rybh
        ),

        /* 汇总：新增4列解释 + 总的是否符合送生 */
        fhss_detail AS (
            SELECT
                bd."ajxx_join_ajxx_ajbh",
                bd."xyrxx_sfzh",

                /* 1 行政：治拘>=5日 */
                CASE
                    WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                    AND xzcf."ajxx_join_ajxx_ajbh" IS NOT NULL
                    THEN '是' ELSE '否'
                END AS "是否治拘5日及以上",

                /* 2 行政：2次前科且案由相同 */
                CASE
                    WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                    AND ay.total_cnt = 2
                    AND ay.distinct_ay_cnt = 1
                    THEN '是' ELSE '否'
                END AS "是否2次前科且案由相同",

                /* 3 行政：3次前科及以上 */
                CASE
                    WHEN bd."ajxx_join_ajxx_ajlx" = '行政'
                    AND ay.total_cnt >= 3
                    THEN '是' ELSE '否'
                END AS "是否3次前科及以上",

                /* 4 刑事：是否未刑拘（按记录证反推：无记录证=未刑拘） */
                CASE
                    WHEN bd."ajxx_join_ajxx_ajlx" = '刑事'
                    AND jlz."ajxx_join_ajxx_ajbh" IS NULL
                    THEN '是' ELSE '否'
                END AS "是否未刑拘",

                /* 总结：四个条件任意满足 => 是 */
                CASE
                    WHEN (
                        (bd."ajxx_join_ajxx_ajlx" = '行政' AND xzcf."ajxx_join_ajxx_ajbh" IS NOT NULL)
                    OR (bd."ajxx_join_ajxx_ajlx" = '行政' AND ay.total_cnt = 2 AND ay.distinct_ay_cnt = 1)
                    OR (bd."ajxx_join_ajxx_ajlx" = '行政' AND ay.total_cnt >= 3)
                    OR (bd."ajxx_join_ajxx_ajlx" = '刑事' AND jlz."ajxx_join_ajxx_ajbh" IS NULL)
                    )
                    THEN '是' ELSE '否'
                END AS is_fhss

            FROM base_data bd
            LEFT JOIN fhss_xzcfjds xzcf
                ON bd."ajxx_join_ajxx_ajbh" = xzcf."ajxx_join_ajxx_ajbh"
            AND bd."xyrxx_sfzh" = xzcf."xyrxx_sfzh"
            LEFT JOIN ay_stats ay
                ON bd."xyrxx_sfzh" = ay.xyrxx_sfzh
            LEFT JOIN fhss_jlz jlz
                ON bd."ajxx_join_ajxx_ajbh" = jlz."ajxx_join_ajxx_ajbh"
            AND bd."xyrxx_sfzh" = jlz."xyrxx_sfzh"
        )

        SELECT DISTINCT
            bd."ajxx_join_ajxx_ajbh" AS 案件编号,
            bd."ajxx_join_ajxx_ajlx" AS 案件类型,
            bd."ajxx_join_ajxx_ajmc" AS 案件名称,
            bd."ajxx_join_ajxx_cbqy_jc" AS 分局,
            bd."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
            TO_CHAR(bd."ajxx_join_ajxx_lasj", 'YYYY-MM-DD HH24:MI:SS') AS 立案时间,
            bd."xyrxx_sfzh" AS 身份证号,
            bd."xyrxx_xm" AS 姓名,
            bd."xyrxx_nl" AS 年龄,
            bd."xyrxx_hjdxz" AS 户籍地,
            bd."xyrxx_jzdxzqh" AS 居住地,
            bd."xyrxx_rybh" AS 人员编号,

            COALESCE(jzws.has_jzws, '否') AS 是否开具矫治文书,
            jzws.jzws_name AS 矫治文书名称,

            /* 是否符合送生 + 4列解释 */
            COALESCE(fh.is_fhss, '否') AS 是否符合送生,
            fh."是否治拘5日及以上",
            fh."是否2次前科且案由相同",
            fh."是否3次前科及以上",
            fh."是否未刑拘",

            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_tqzmjy" tq
                    WHERE bd."ajxx_join_ajxx_ajbh" = tq.ajbh
                    AND bd."xyrxx_xm" = tq.xgry_xm
                ) THEN '是'
                ELSE '否'
            END AS 是否提请专门教育,

            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_wcnr_sfzxx" sfz
                    WHERE bd."xyrxx_sfzh" = sfz.sfzhm
                    AND bd."ajxx_join_ajxx_lasj" < sfz.rx_time
                ) THEN '是'
                ELSE '否'
            END AS 是否送校

        FROM base_data bd
        LEFT JOIN jzws_info jzws
            ON bd."ajxx_join_ajxx_ajbh" = jzws."ajxx_join_ajxx_ajbh"
        AND bd."xyrxx_sfzh" = jzws."xyrxx_sfzh"
        LEFT JOIN fhss_detail fh
            ON bd."ajxx_join_ajxx_ajbh" = fh."ajxx_join_ajxx_ajbh"
        AND bd."xyrxx_sfzh" = fh."xyrxx_sfzh"
        ORDER BY bd."ajxx_join_ajxx_lasj" DESC, bd."xyrxx_xm"
        """
    ).format(type_condition=type_condition)

    params = [start_time, end_time] + type_params

    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def calculate_summary_by_fenju(data_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    根据查询结果按分局分组统计
    返回按分局分组的统计列表 + 最后一行全市合计
    """
    if not data_rows:
        return []

    # 按分局分组统计
    fenju_stats: Dict[str, Dict[str, int]] = {}

    for row in data_rows:
        fenju = str(row.get("分局") or "未知分局")
        ajlx = str(row.get("案件类型") or "")
        has_jzws = str(row.get("是否开具矫治文书") or "否")
        has_tqzmjy = str(row.get("是否提请专门教育") or "否")
        is_wei_xingju = str(row.get("是否未刑拘") or "否")

        if fenju not in fenju_stats:
            fenju_stats[fenju] = {
                "违法人数": 0,
                "矫治教育文书开具数(行政)": 0,
                "提请专门教育申请书数(行政)": 0,
                "犯罪人数": 0,
                "矫治教育文书开具数(刑事)": 0,
                "提请专门教育申请书数(刑事)": 0,
                "刑拘数": 0,
            }

        stats = fenju_stats[fenju]

        if ajlx == "行政":
            stats["违法人数"] += 1
            if has_jzws == "是":
                stats["矫治教育文书开具数(行政)"] += 1
            if has_tqzmjy == "是":
                stats["提请专门教育申请书数(行政)"] += 1
        elif ajlx == "刑事":
            stats["犯罪人数"] += 1
            if has_jzws == "是":
                stats["矫治教育文书开具数(刑事)"] += 1
            if has_tqzmjy == "是":
                stats["提请专门教育申请书数(刑事)"] += 1
            # 刑拘数：案件类型='刑事' 且 是否未刑拘='否'
            if is_wei_xingju == "否":
                stats["刑拘数"] += 1

    # 转换为列表格式
    result = []
    for fenju, stats in sorted(fenju_stats.items()):
        result.append({"分局": fenju, **stats})

    # 添加全市合计行
    total = {
        "分局": "全市",
        "违法人数": sum(s["违法人数"] for s in fenju_stats.values()),
        "矫治教育文书开具数(行政)": sum(s["矫治教育文书开具数(行政)"] for s in fenju_stats.values()),
        "提请专门教育申请书数(行政)": sum(s["提请专门教育申请书数(行政)"] for s in fenju_stats.values()),
        "犯罪人数": sum(s["犯罪人数"] for s in fenju_stats.values()),
        "矫治教育文书开具数(刑事)": sum(s["矫治教育文书开具数(刑事)"] for s in fenju_stats.values()),
        "提请专门教育申请书数(刑事)": sum(s["提请专门教育申请书数(刑事)"] for s in fenju_stats.values()),
        "刑拘数": sum(s["刑拘数"] for s in fenju_stats.values()),
    }
    result.append(total)

    return result
