from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from psycopg2 import sql

from gonggong.config.database import DB_CONFIG


SCHEMA = DB_CONFIG.get("schema") or "ywdata"

SUMMARY_COLUMNS: List[str] = [
    "地区",
    "违法犯罪人数",
    "矫治教育文书开具数",
    "监护文书开具数",
    "提请专门教育申请书数",
    "符合送校数",
    "送校数",
    "刑拘数",
]

DIQU_NAME_MAP: Dict[str, str] = {
    "445300": "市局",
    "445302": "云城",
    "445303": "云安",
    "445321": "新兴",
    "445322": "郁南",
    "445381": "罗定",
}

DIQU_ORDER: List[str] = ["445300", "445302", "445303", "445321", "445322", "445381"]

SUMMARY_FLAG_FIELD_MAP: Dict[str, str] = {
    "矫治教育文书开具数": "是否开具矫治文书",
    "监护文书开具数": "是否开具家庭教育指导书",
    "提请专门教育申请书数": "是否开具专门教育申请书",
    "符合送校数": "是否符合送生",
    "送校数": "是否送校",
    "刑拘数": "是否刑拘",
}


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

    - 数据源：v_wcnr_wfry_base 视图
    - 时间条件：使用 vw.录入时间（与页面开始/结束时间联动）
    - 类型条件：支持多选；通过 case_type_config.ay_pattern 匹配 vw.案由；
      若未选择任何“类型”，则不添加类型过滤条件（即查全量）。
    """
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]

    # 构建类型匹配条件
    if leixing_list:
        type_condition = sql.SQL(
            """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND vw.案由 SIMILAR TO ctc."ay_pattern"
            )
            """
        )
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    query = sql.SQL(
        """
        WITH violation_counts AS (
            -- 计算每个人的违法次数和案由
            SELECT
                w.xyrxx_sfzh AS 身份证号,
                COUNT(*) AS 违法次数,
                COUNT(DISTINCT w.ajxx_join_ajxx_ay_dm) AS 不同案由数
            FROM "ywdata"."zq_zfba_wcnr_xyr" w
            WHERE COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
              AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
            GROUP BY w.xyrxx_sfzh
        ),
        first_case_xjs AS (
            -- 查找第一次违法是否开具了训诫书
            SELECT DISTINCT
                vw.身份证号,
                vw.案件编号 AS 当前案件编号,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM "ywdata"."zq_zfba_wcnr_xyr" w
                        JOIN "ywdata"."zq_zfba_xjs2" x
                          ON w."ajxx_join_ajxx_ajbh" = x.ajbh
                         AND w.xyrxx_xm = x.xgry_xm
                        WHERE w.xyrxx_sfzh = vw.身份证号
                          AND w."ajxx_join_ajxx_ajbh" <> vw.案件编号
                          AND COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
                          AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
                    ) THEN 1
                    ELSE 0
                END AS 有训诫书
            FROM "ywdata"."v_wcnr_wfry_base" vw
        ),
        base_data AS (
            SELECT DISTINCT
                vw.案件编号,
                vw.人员编号,
                vw.案件类型,
                vw.案由,
                vw.地区,
                vw.办案单位,
                vw.立案时间,
                vw.姓名,
                vw.身份证号,
                vw.户籍地,
                vw.年龄,
                vw.居住地,
                CASE WHEN vw.年龄::text ~ '^\\d+$' THEN CAST(vw.年龄 AS INTEGER) END AS 年龄数值,
                COALESCE(vc.违法次数, 0) AS 违法次数,
                COALESCE(vc.不同案由数, 0) AS 不同案由数,
                COALESCE(fcx.有训诫书, 0) AS 有训诫书
            FROM "ywdata"."v_wcnr_wfry_base" vw
            LEFT JOIN violation_counts vc ON vw.身份证号 = vc.身份证号
            LEFT JOIN first_case_xjs fcx ON vw.身份证号 = fcx.身份证号 AND vw.案件编号 = fcx.当前案件编号
            WHERE vw.录入时间 BETWEEN %s AND %s
            {type_condition}
        ),
        flag_data AS (
            SELECT
                bd.*,
                CASE
                    WHEN bd.案件类型 = '行政' AND EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                        WHERE x.ajxx_ajbh = bd.案件编号
                          AND x.xzcfjds_rybh = bd.人员编号
                          AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
                    ) THEN 1
                    ELSE 0
                END AS is_zhiju_gt4,
                CASE
                    WHEN bd.案件类型 = '行政'
                         AND bd.违法次数 = 2
                         AND bd.不同案由数 = 1
                         AND bd.有训诫书 = 1
                    THEN 1
                    ELSE 0
                END AS is_second_same_ay_with_xjs,
                CASE
                    WHEN bd.案件类型 = '行政' AND bd.违法次数 > 2 THEN 1
                    ELSE 0
                END AS is_third_plus,
                CASE
                    WHEN bd.案件类型 = '刑事' AND EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                        WHERE j.ajxx_ajbh = bd.案件编号 AND j.jlz_rybh = bd.人员编号
                    ) THEN 1
                    ELSE 0
                END AS is_xingju,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" z
                        WHERE z.zltzs_ajbh = bd.案件编号 AND z.zltzs_rybh = bd.人员编号
                    ) OR EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_xjs2" x
                        WHERE x.ajbh = bd.案件编号 AND x.xgry_xm = bd.姓名
                    ) THEN 1
                    ELSE 0
                END AS is_jiaozhi_wenshu,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_xjs2" x
                        WHERE x.ajbh = bd.案件编号 AND x.xgry_xm = bd.姓名
                    ) THEN 1
                    ELSE 0
                END AS is_xunjieshu,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" z
                        WHERE z.zltzs_ajbh = bd.案件编号 AND z.zltzs_rybh = bd.人员编号
                    ) THEN 1
                    ELSE 0
                END AS is_zeling_tongzhishu,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_jtjyzdtzs" j
                        WHERE j.jqjhjyzljsjtjyzdtzs_ajbh = bd.案件编号
                          AND j.jqjhjyzljsjtjyzdtzs_rybh = bd.人员编号
                    ) THEN 1
                    ELSE 0
                END AS is_jiating_jiaoyu_wenshu,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_zfba_tqzmjy" t
                        WHERE t.ajbh = bd.案件编号 AND t.xgry_xm = bd.姓名
                    ) THEN 1
                    ELSE 0
                END AS is_zhuanmen_shenqingshu,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM "ywdata"."zq_wcnr_sfzxx" s
                        WHERE s.sfzhm = bd.身份证号 AND s.rx_time > bd.立案时间
                    ) THEN 1
                    ELSE 0
                END AS is_songxiao
            FROM base_data bd
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
            CASE WHEN fd.is_zhiju_gt4 = 1 THEN '是' ELSE '否' END AS 治拘大于4天,
            CASE WHEN fd.is_second_same_ay_with_xjs = 1 THEN '是' ELSE '否' END AS "2次违法且案由相同且第一次违法开具了训诫书",
            CASE WHEN fd.is_third_plus = 1 THEN '是' ELSE '否' END AS "3次及以上违法",
            CASE WHEN fd.is_xingju = 1 THEN '是' ELSE '否' END AS 是否刑拘,
            CASE WHEN fd.is_jiaozhi_wenshu = 1 THEN '是' ELSE '否' END AS 是否开具矫治文书,
            CASE
                WHEN fd.is_xunjieshu = 1 AND fd.is_zeling_tongzhishu = 1 THEN '训诫书/责令未成年人遵守特定行为规范通知书'
                WHEN fd.is_xunjieshu = 1 THEN '训诫书'
                WHEN fd.is_zeling_tongzhishu = 1 THEN '责令未成年人遵守特定行为规范通知书'
                ELSE ''
            END AS 开具矫治教育文书名称,
            CASE WHEN fd.is_jiating_jiaoyu_wenshu = 1 THEN '是' ELSE '否' END AS 是否开具家庭教育指导书,
            CASE WHEN fd.is_zhuanmen_shenqingshu = 1 THEN '是' ELSE '否' END AS 是否开具专门教育申请书,
            CASE
                WHEN fd.年龄数值 > 11
                     AND fd.is_xingju = 0
                     AND (
                        fd.is_zhiju_gt4 = 1
                        OR fd.is_second_same_ay_with_xjs = 1
                        OR fd.is_third_plus = 1
                     )
                THEN '是'
                ELSE '否'
            END AS 是否符合送生,
            CASE WHEN fd.is_songxiao = 1 THEN '是' ELSE '否' END AS 是否送校
        FROM flag_data fd
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

    输出列：
    - 地区
    - 违法犯罪人数
    - 矫治教育文书开具数（是否开具矫治文书=是）
    - 监护文书开具数（是否开具家庭教育指导书=是）
    - 提请专门教育申请书数（是否开具专门教育申请书=是）
    - 符合送校数（是否符合送生=是）
    - 送校数（是否送校=是）
    - 刑拘数（是否刑拘=是）
    """
    if not data_rows:
        return []

    # 按地区分组统计
    diqu_stats: Dict[str, Dict[str, int]] = {}

    metric_cols = [c for c in SUMMARY_COLUMNS if c != "地区"]

    for row in data_rows:
        diqu_code = str(row.get("地区") or "").strip() or "未知"

        if diqu_code not in diqu_stats:
            diqu_stats[diqu_code] = {k: 0 for k in metric_cols}

        stats = diqu_stats[diqu_code]
        stats["违法犯罪人数"] += 1

        for metric_col, detail_field in SUMMARY_FLAG_FIELD_MAP.items():
            if str(row.get(detail_field) or "否").strip() == "是":
                stats[metric_col] += 1

    # 转换为列表格式
    def _diqu_name(code: str) -> str:
        code = (code or "").strip()
        if not code or code == "未知":
            return "未知"
        return DIQU_NAME_MAP.get(code, code)

    result: List[Dict[str, Any]] = []

    # 先按固定顺序输出
    for code in DIQU_ORDER:
        if code in diqu_stats:
            item: Dict[str, Any] = {"地区": _diqu_name(code)}
            for col in metric_cols:
                item[col] = diqu_stats[code][col]
            result.append(item)

    # 再输出其他地区（不在固定列表里的）
    rest_codes = [c for c in diqu_stats.keys() if c not in set(DIQU_ORDER)]
    for code in sorted(rest_codes):
        item = {"地区": _diqu_name(code)}
        for col in metric_cols:
            item[col] = diqu_stats[code][col]
        result.append(item)

    # 添加全市合计行
    total: Dict[str, Any] = {"地区": "全市"}
    for col in metric_cols:
        total[col] = sum(s[col] for s in diqu_stats.values())
    result.append(total)

    return result

