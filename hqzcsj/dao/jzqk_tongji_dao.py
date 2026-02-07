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
    获取矫治教育统计明细数据

    - 时间条件：使用 zq_zfba_wcnr_xyr.xyrxx_lrsj（与页面开始/结束时间联动）
    - 类型条件：支持多选；通过 case_type_config.ay_pattern 匹配 ajxx_join_ajxx_ay；
      若未选择任何“类型”，则不添加类型过滤条件（即查全量）。
    """
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]

    # 构建类型匹配条件
    if leixing_list:
        # 直接使用配置表做 EXISTS，避免“子查询返回多行”问题，且支持一个类型多条 pattern
        type_condition = sql.SQL(
            """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND zzwx."ajxx_join_ajxx_ay" SIMILAR TO ctc."ay_pattern"
            )
            """
        )
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    query = sql.SQL(
        """
        WITH base_data AS (
            -- 基础案件-人员数据（按身份证号和案件编号去重）
            SELECT DISTINCT ON (zzwx."xyrxx_sfzh", zzwx."ajxx_join_ajxx_ajbh")
                zzwx."ajxx_join_ajxx_ajbh" AS 案件编号,
                zzwx."xyrxx_rybh" AS 人员编号,
                zzwx."ajxx_join_ajxx_ajlx" AS 案件类型,
                zzwx."ajxx_join_ajxx_ay" AS 案由,
                zzwx."ajxx_join_ajxx_ay_dm" AS 案由代码,
                LEFT(zzwx."ajxx_join_ajxx_cbqy_bh_dm", 6) AS 地区,
                zzwx."ajxx_join_ajxx_cbdw_bh" AS 办案单位,
                zzwx."ajxx_join_ajxx_lasj" AS 立案时间,
                zzwx."xyrxx_xm" AS 姓名,
                zzwx."xyrxx_sfzh" AS 身份证号,
                zzwx."xyrxx_hjdxz" AS 户籍地,
                zzwx."xyrxx_nl" AS 年龄,
                zzwx."xyrxx_jzdxzqh" AS 居住地
            FROM "ywdata"."zq_zfba_wcnr_xyr" zzwx
            WHERE zzwx."ajxx_join_ajxx_isdel_dm" = '0'
              AND zzwx."xyrxx_isdel_dm" = '0'
              AND zzwx."xyrxx_sfda_dm" = '1'
              AND zzwx."xyrxx_lrsj" BETWEEN %s AND %s
            {type_condition}
            ORDER BY zzwx."xyrxx_sfzh", zzwx."ajxx_join_ajxx_ajbh", zzwx."xyrxx_lrsj" DESC
        ),

        filtered_data AS (
            -- 根据案件类型匹配相应的文书表（仅保留命中文书的记录）
            SELECT bd.*
            FROM base_data bd
            WHERE
                -- 行政案件：匹配行政处罚决定书或不予行政处罚决定书
                (bd.案件类型 = '行政' AND (
                    EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                        WHERE x.ajxx_ajbh = bd.案件编号 AND x.xzcfjds_rybh = bd.人员编号
                    )
                    OR EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_byxzcfjds" b
                        WHERE b.ajxx_ajbh = bd.案件编号 AND b.byxzcfjds_rybh = bd.人员编号
                    )
                ))
                -- 刑事案件：匹配拘留证
                OR bd.案件类型 = '刑事'
        ),

        -- 收敛身份证集合，后续统计只算这批人，避免慢
        target_sfzh AS (
            SELECT DISTINCT fd.身份证号
            FROM filtered_data fd
        ),

        -- 计算每个人的违法次数和案由（仅统计 target_sfzh）
        violation_counts AS (
            SELECT
                w.xyrxx_sfzh AS 身份证号,
                COUNT(*) AS 违法次数,
                COUNT(DISTINCT w.ajxx_join_ajxx_ay_dm) AS 不同案由数
            FROM "ywdata"."zq_zfba_wcnr_xyr" w
            INNER JOIN target_sfzh t ON t.身份证号 = w.xyrxx_sfzh
            WHERE w."xyrxx_isdel_dm" = '0' AND w."ajxx_join_ajxx_isdel_dm" = '0'
            GROUP BY w.xyrxx_sfzh
        ),

        -- 该人员曾开具训诫书的案件集合（仅统计 target_sfzh）
        xjs_cases AS (
            SELECT DISTINCT
                w.xyrxx_sfzh AS 身份证号,
                w."ajxx_join_ajxx_ajbh" AS 案件编号
            FROM "ywdata"."zq_zfba_wcnr_xyr" w
            INNER JOIN target_sfzh t ON t.身份证号 = w.xyrxx_sfzh
            INNER JOIN "ywdata"."zq_zfba_xjs2" x
                ON w."ajxx_join_ajxx_ajbh" = x.ajbh
               AND w.xyrxx_xm = x.xgry_xm
            WHERE w."xyrxx_isdel_dm" = '0' AND w."ajxx_join_ajxx_isdel_dm" = '0'
        ),

        -- “第一次违法是否开具了训诫书”：按人员 + 当前案件判断（存在其他案件有训诫书 => 当前案件视为满足）
        first_case_xjs AS (
            SELECT
                fd.身份证号,
                fd.案件编号 AS 当前案件编号,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM xjs_cases xc
                        WHERE xc.身份证号 = fd.身份证号 AND xc.案件编号 <> fd.案件编号
                    ) THEN 1 ELSE 0
                END AS 有训诫书
            FROM filtered_data fd
        )

        SELECT DISTINCT
            fd.案件编号,
            fd.人员编号,
            fd.案件类型,
            fd.案由,
            fd.地区,
            fd.办案单位,
            TO_CHAR(fd.立案时间, 'YYYY-MM-DD HH24:MI:SS') AS 立案时间,
            fd.姓名,
            fd.身份证号,
            fd.户籍地,
            fd.年龄,
            fd.居住地,

            -- 治拘大于4天(仅行政案件)
            CASE
                WHEN fd.案件类型 = '行政' AND EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                    WHERE x.ajxx_ajbh = fd.案件编号
                      AND x.xzcfjds_rybh = fd.人员编号
                      AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
                ) THEN '是'
                ELSE '否'
            END AS 治拘大于4天,

            -- 2次违法且案由相同且第一次违法开具了训诫书(仅行政案件)
            CASE
                WHEN fd.案件类型 = '行政'
                     AND COALESCE(vc.违法次数, 0) = 2
                     AND COALESCE(vc.不同案由数, 0) = 1
                     AND COALESCE(fcx.有训诫书, 0) = 1
                THEN '是'
                ELSE '否'
            END AS "2次违法且案由相同且第一次违法开具了训诫书",

            -- 3次及以上违法(仅行政案件)
            CASE
                WHEN fd.案件类型 = '行政' AND COALESCE(vc.违法次数, 0) > 2 THEN '是'
                ELSE '否'
            END AS "3次及以上违法",

            -- 是否刑拘(仅刑事案件)
            CASE
                WHEN fd.案件类型 = '刑事' AND EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                    WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
                ) THEN '是'
                ELSE '否'
            END AS 是否刑拘,

            -- 是否开具矫治文书
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" z
                    WHERE z.zltzs_ajbh = fd.案件编号 AND z.zltzs_rybh = fd.人员编号
                ) OR EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_xjs2" x
                    WHERE x.ajbh = fd.案件编号 AND x.xgry_xm = fd.姓名
                ) THEN '是'
                ELSE '否'
            END AS 是否开具矫治文书,

            -- 是否开具加强监督教育/责令接受家庭教育指导通知书
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_jtjyzdtzs" j
                    WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = fd.案件编号
                      AND j.jqjhjyzljsjtjyzdtzs_rybh = fd.人员编号
                ) THEN '是'
                ELSE '否'
            END AS "是否开具加强监督教育/责令接受家庭教育指导通知书",

            -- 是否开具提请专门教育申请书
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_tqzmjy" t
                    WHERE t.ajbh = fd.案件编号 AND t.xgry_xm = fd.姓名
                ) THEN '是'
                ELSE '否'
            END AS 是否开具提请专门教育申请书,

            -- 是否符合送生
            CASE
                WHEN (CASE WHEN fd.年龄::text ~ '^\\d+$' THEN CAST(fd.年龄 AS INTEGER) END) > 11
                     AND (
                        -- 治拘大于4天
                        (fd.案件类型 = '行政' AND EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                            WHERE x.ajxx_ajbh = fd.案件编号
                              AND x.xzcfjds_rybh = fd.人员编号
                              AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
                        ))
                        -- 2次违法且案由相同且第一次违法开具了训诫书
                        OR (fd.案件类型 = '行政'
                            AND COALESCE(vc.违法次数, 0) = 2
                            AND COALESCE(vc.不同案由数, 0) = 1
                            AND COALESCE(fcx.有训诫书, 0) = 1
                        )
                        -- 3次及以上违法
                        OR (fd.案件类型 = '行政' AND COALESCE(vc.违法次数, 0) > 2)
                        -- 是否刑拘
                        OR (fd.案件类型 = '刑事' AND EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                            WHERE j.ajxx_ajbh = fd.案件编号 AND j.jlz_rybh = fd.人员编号
                        ))
                     )
                THEN '是'
                ELSE '否'
            END AS 是否符合送生,

            -- 是否送校
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_wcnr_sfzxx" s
                    WHERE s.sfzhm = fd.身份证号 AND s.rx_time > fd.立案时间
                ) THEN '是'
                ELSE '否'
            END AS 是否送校

        FROM filtered_data fd
        LEFT JOIN violation_counts vc ON fd.身份证号 = vc.身份证号
        LEFT JOIN first_case_xjs fcx ON fd.身份证号 = fcx.身份证号 AND fd.案件编号 = fcx.当前案件编号

        ORDER BY fd.案件编号, fd.人员编号
        """
    ).format(type_condition=type_condition)

    params = [start_time, end_time] + type_params

    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def calculate_summary_by_diqu(data_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    根据查询结果按地区分组统计
    返回按地区分组的统计列表 + 最后一行全市合计

    地区映射/顺序：
    - 445302: 云城
    - 445303: 云安
    - 445381: 罗定
    - 445321: 新兴
    - 445322: 郁南
    """
    if not data_rows:
        return []

    diqu_name_map = {
        "445302": "云城",
        "445303": "云安",
        "445381": "罗定",
        "445321": "新兴",
        "445322": "郁南",
    }
    diqu_order = ["445302", "445303", "445381", "445321", "445322"]

    # 按地区分组统计
    diqu_stats: Dict[str, Dict[str, int]] = {}

    for row in data_rows:
        diqu_code = str(row.get("地区") or "").strip() or "未知"
        ajlx = str(row.get("案件类型") or "")
        has_jzws = str(row.get("是否开具矫治文书") or "否")
        has_tqzmjy = str(row.get("是否开具提请专门教育申请书") or "否")
        is_fhss = str(row.get("是否符合送生") or "否")
        is_songxiao = str(row.get("是否送校") or "否")
        is_xingju = str(row.get("是否刑拘") or "否")

        if diqu_code not in diqu_stats:
            diqu_stats[diqu_code] = {
                "违法人数": 0,
                "符合送生（行政）": 0,
                "矫治教育文书开具数(行政)": 0,
                "提请专门教育申请书数(行政)": 0,
                "送生数（行政）": 0,
                "犯罪人数": 0,
                "符合送生（刑事）": 0,
                "矫治教育文书开具数(刑事)": 0,
                "提请专门教育申请书数(刑事)": 0,
                "送生数（刑事）": 0,
                "刑拘数": 0,
            }

        stats = diqu_stats[diqu_code]

        if ajlx == "行政":
            stats["违法人数"] += 1
            if is_fhss == "是":
                stats["符合送生（行政）"] += 1
            if has_jzws == "是":
                stats["矫治教育文书开具数(行政)"] += 1
            if has_tqzmjy == "是":
                stats["提请专门教育申请书数(行政)"] += 1
            if is_songxiao == "是":
                stats["送生数（行政）"] += 1
        elif ajlx == "刑事":
            stats["犯罪人数"] += 1
            if is_fhss == "是":
                stats["符合送生（刑事）"] += 1
            if has_jzws == "是":
                stats["矫治教育文书开具数(刑事)"] += 1
            if has_tqzmjy == "是":
                stats["提请专门教育申请书数(刑事)"] += 1
            if is_songxiao == "是":
                stats["送生数（刑事）"] += 1
            # 刑拘数：案件类型='刑事' 且 是否刑拘='是'
            if is_xingju == "是":
                stats["刑拘数"] += 1

    # 转换为列表格式
    def _diqu_name(code: str) -> str:
        code = (code or "").strip()
        if not code or code == "未知":
            return "未知"
        return diqu_name_map.get(code, code)

    result: List[Dict[str, Any]] = []

    # 先按固定顺序输出
    for code in diqu_order:
        if code in diqu_stats:
            result.append({"地区": _diqu_name(code), **diqu_stats[code]})

    # 再输出其他地区（不在固定列表里的）
    rest_codes = [c for c in diqu_stats.keys() if c not in set(diqu_order)]
    for code in sorted(rest_codes):
        result.append({"地区": _diqu_name(code), **diqu_stats[code]})

    # 添加全市合计行
    total = {
        "地区": "全市",
        "违法人数": sum(s["违法人数"] for s in diqu_stats.values()),
        "符合送生（行政）": sum(s["符合送生（行政）"] for s in diqu_stats.values()),
        "矫治教育文书开具数(行政)": sum(s["矫治教育文书开具数(行政)"] for s in diqu_stats.values()),
        "提请专门教育申请书数(行政)": sum(s["提请专门教育申请书数(行政)"] for s in diqu_stats.values()),
        "送生数（行政）": sum(s["送生数（行政）"] for s in diqu_stats.values()),
        "犯罪人数": sum(s["犯罪人数"] for s in diqu_stats.values()),
        "符合送生（刑事）": sum(s["符合送生（刑事）"] for s in diqu_stats.values()),
        "矫治教育文书开具数(刑事)": sum(s["矫治教育文书开具数(刑事)"] for s in diqu_stats.values()),
        "提请专门教育申请书数(刑事)": sum(s["提请专门教育申请书数(刑事)"] for s in diqu_stats.values()),
        "送生数（刑事）": sum(s["送生数（刑事）"] for s in diqu_stats.values()),
        "刑拘数": sum(s["刑拘数"] for s in diqu_stats.values()),
    }
    result.append(total)

    return result
