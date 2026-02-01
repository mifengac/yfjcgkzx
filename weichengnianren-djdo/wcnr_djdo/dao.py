from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from gonggong.config.database import get_database_connection


def _fetch_all(sql: str, params: Tuple[Any, ...]) -> List[Dict[str, Any]]:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def query_jq_za_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
AND jq."newcharasubclass" IN (
    SELECT UNNEST(ctc."newcharasubclass_list")
    FROM ywdata."case_type_config" ctc
    WHERE ctc."leixing" = ANY(%s)
)"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
SELECT
    jq."caseno" AS "警情编号",
    jq."calltime" AS "报警时间",
    jq."cmdname" AS "分局",
    jq."dutydeptname" AS "管辖单位",
    jq."casecontents" AS "报警内容",
    jq."replies" AS "处警情况",
    mza."案件编号" AS "案件编号",
    mza."案件名称" AS "案件名称",
    mza."地区" AS "案件地区",
    mza."办案单位名称" AS "办案单位名称",
    CASE
        WHEN jq."cmdname" LIKE '%%云城%%' THEN '云城'
        WHEN jq."cmdname" LIKE '%%云安%%' THEN '云安'
        WHEN jq."cmdname" LIKE '%%罗定%%' THEN '罗定'
        WHEN jq."cmdname" LIKE '%%新兴%%' THEN '新兴'
        WHEN jq."cmdname" LIKE '%%郁南%%' THEN '郁南'
        ELSE '其他'
    END AS "地区"
FROM ywdata."zq_kshddpt_dsjfx_jq" jq
LEFT JOIN (
    SELECT
        aj."ajxx_jqbh" AS "警情编号",
        aj."ajxx_ajbh" AS "案件编号",
        aj."ajxx_ajmc" AS "案件名称",
        LEFT(aj."ajxx_cbdw_bh_dm", 6) AS "地区",
        aj."ajxx_cbdw_mc" AS "办案单位名称"
    FROM ywdata."zq_zfba_wcnr_ajxx" aj
) mza
    ON jq."caseno" = mza."警情编号"
WHERE jq."calltime" BETWEEN %s AND %s
AND jq."casemarkok" ~ '未成年'
{type_condition}
;
"""
    return _fetch_all(sql, params)


def query_jzjy_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
    AND EXISTS (
        SELECT 1
        FROM ywdata."case_type_config" ctc
        WHERE ctc."leixing" = ANY(%s)
          AND COALESCE(zzwx."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
    )"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
WITH minor_fight AS (
    SELECT
        zzwx."ajxx_ajbhs" AS "案件编号",
        zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
        zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
        CASE
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445302' THEN '云城'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445303' THEN '云安'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445381' THEN '罗定'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445321' THEN '新兴'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445322' THEN '郁南'
            ELSE '其他'
        END AS "地区",
        aj."ajxx_cbdw_mc" AS "办案单位名称",
        zzwx."xyrxx_xm" AS "姓名",
        zzwx."xyrxx_sfzh" AS "证件号码",
        '嫌疑人' AS "人员类型",
        zzwx."xyrxx_nl" AS "年龄",
        zzwx."xyrxx_rybh" AS "人员编号"
    FROM ywdata."zq_zfba_wcnr_xyr" zzwx
    LEFT JOIN ywdata."zq_zfba_wcnr_ajxx" aj
        ON zzwx."ajxx_ajbhs" = aj."ajxx_ajbh"
    WHERE zzwx."ajxx_join_ajxx_lasj"::timestamp BETWEEN %s AND %s
{type_condition}
),
target_aj AS (
    SELECT DISTINCT "案件编号"
    FROM minor_fight
),
    doc_hit_raw AS (
        SELECT
            xjs_ajbh AS "案件编号",
            xjs_rybh AS "人员编号",
            '训诫书'  AS "文书名称",
            xjs_xjyy AS "训诫原因"
    FROM "ywdata"."zq_zfba_xjs"
    WHERE xjs_ajbh IS NOT NULL AND xjs_rybh IS NOT NULL
    AND xjs_wszt = '审批通过' AND xjs_isdel = '0'
    UNION ALL
    SELECT
        zltzs_ajbh AS "案件编号",
        zltzs_rybh AS "人员编号",
        '加强监督教育/责令接受家庭教育指导通知书' AS "文书名称",
        zltzs_blxw AS "训诫原因"
    FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs"
    WHERE zltzs_ajbh IS NOT NULL AND zltzs_rybh IS NOT NULL
    AND zltzs_wszt = '审批通过' AND zltzs_isdel = '0'
),
doc_hit AS (
    SELECT
        "案件编号",
        "人员编号",
        string_agg(DISTINCT "文书名称", ',' ORDER BY "文书名称") AS "文书名称",
        string_agg(DISTINCT "训诫原因", ',' ORDER BY "训诫原因") AS "训诫原因"
    FROM doc_hit_raw
    GROUP BY "案件编号", "人员编号"
),
baxgry_distinct AS (
    SELECT DISTINCT
        r.asjbh AS "案件编号",
        NULLIF(TRIM(r.baxgry_xm), '') AS "name",
        NULLIF(TRIM(r.lxdh), '')      AS "phone"
    FROM ywdata."zfba_ry_001" r
    INNER JOIN target_aj t
        ON r.asjbh = t."案件编号"
    WHERE NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
    AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
),
baxgry_json AS (
    SELECT
        d."案件编号",
        jsonb_agg(
            jsonb_build_object('name', d."name", 'phone', d."phone")
            ORDER BY d."name", d."phone"
        ) AS "办案联系人_json",
        jsonb_agg(
            d."phone"
            ORDER BY d."phone"
        ) AS "联系电话_json"
    FROM baxgry_distinct d
    GROUP BY d."案件编号"
)
SELECT
    mf.*,
    dh."训诫原因",
    dh."文书名称",
    CASE WHEN dh."案件编号" IS NOT NULL THEN '是' ELSE '否' END AS "是否开具文书",
    bx."办案联系人_json",
    bx."联系电话_json"
FROM minor_fight mf
LEFT JOIN doc_hit dh
    ON mf."案件编号" = dh."案件编号"
AND mf."人员编号" = dh."人员编号"
LEFT JOIN baxgry_json bx
    ON mf."案件编号" = bx."案件编号"
;
"""
    return _fetch_all(sql, params)


def query_sx_sx_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
AND EXISTS (
    SELECT 1
    FROM ywdata."case_type_config" ctc
    WHERE ctc."leixing" = ANY(%s)
      AND COALESCE(zzwx."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
)"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
SELECT
    zzwx."xyrxx_rybh" AS "人员编号",
    zzwx."xyrxx_xm" AS "姓名",
    zzwx."xyrxx_sfzh" AS "证件号码",
    zzwx."xyrxx_hjdxz" AS "户籍地",
    zzwx."xyrxx_xzdxz" AS "现住地",
    zzwx."xyrxx_nl" AS "年龄",
    '嫌疑人' AS "人员类型",
    zzwx."ajxx_ajbhs" AS "案件编号",
    CASE
        WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445302' THEN '云城'
        WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445303' THEN '云安'
        WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445381' THEN '罗定'
        WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445321' THEN '新兴'
        WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445322' THEN '郁南'
        ELSE '其他'
    END AS "地区",
    zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
    aj."ajxx_jyaq" AS "简要案情",
    zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
    aj."ajxx_cbdw_mc" AS "办案单位名称",
    bx."办案联系人_json",
    bx."联系电话_json",
    CASE
        WHEN sx.is_match = 1 THEN '是'
        ELSE '否'
    END AS "是否送校"
FROM ywdata."zq_zfba_wcnr_xyr" zzwx
LEFT JOIN ywdata."zq_zfba_wcnr_ajxx" aj
    ON zzwx."ajxx_ajbhs" = aj."ajxx_ajbh"
LEFT JOIN LATERAL (
    WITH d AS (
        SELECT DISTINCT
            NULLIF(TRIM(r.baxgry_xm), '') AS name,
            NULLIF(TRIM(r.lxdh), '')      AS phone
        FROM ywdata."zfba_ry_001" r
        WHERE r.asjbh = zzwx."ajxx_ajbhs"
        AND NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
        AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
    )
    SELECT
        jsonb_agg(jsonb_build_object('name', d.name, 'phone', d.phone) ORDER BY d.name, d.phone) AS "办案联系人_json",
        jsonb_agg(d.phone ORDER BY d.phone) AS "联系电话_json"
    FROM d
) bx ON TRUE
LEFT JOIN LATERAL (
    SELECT 1 AS is_match
    FROM "ywdata"."zq_wcnr_sfzxx" z
    WHERE z."sfzhm" = zzwx."xyrxx_sfzh"
    AND z.rx_time::timestamp > zzwx."ajxx_join_ajxx_lasj"::timestamp
    LIMIT 1
) sx ON TRUE
WHERE zzwx."ajxx_join_ajxx_lasj"::timestamp BETWEEN %s AND %s
{type_condition}
AND zzwx."ajxx_join_ajxx_ajlx" = '刑事'
;
"""
    return _fetch_all(sql, params)


def query_zljqjh_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
    AND EXISTS (
        SELECT 1
        FROM ywdata."case_type_config" ctc
        WHERE ctc."leixing" = ANY(%s)
          AND COALESCE(zzwx."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
    )"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
WITH minor_fight AS (
    SELECT
        zzwx."ajxx_ajbhs" AS "案件编号",
        zzwx."ajxx_join_ajxx_ajmc" AS "案件名称",
        zzwx."ajxx_join_ajxx_lasj" AS "立案日期",
        aj."ajxx_cbdw_mc" AS "办案单位名称",
        CASE
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445302' THEN '云城'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445303' THEN '云安'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445381' THEN '罗定'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445321' THEN '新兴'
            WHEN LEFT(zzwx."ajxx_join_ajxx_cbdw_bh_dm", 6) = '445322' THEN '郁南'
            ELSE '其他'
        END AS "地区",
        zzwx."xyrxx_xm" AS "姓名",
        zzwx."xyrxx_sfzh" AS "证件号码",
        '嫌疑人' AS "人员类型",
        zzwx."xyrxx_nl" AS "年龄",
        zzwx."xyrxx_rybh" AS "人员编号"
    FROM ywdata."zq_zfba_wcnr_xyr" zzwx
    LEFT JOIN ywdata."zq_zfba_wcnr_ajxx" aj
        ON zzwx."ajxx_ajbhs" = aj."ajxx_ajbh"
    WHERE zzwx."ajxx_join_ajxx_lasj"::timestamp BETWEEN %s AND %s
{type_condition}
),
target_aj AS (
    SELECT DISTINCT "案件编号"
    FROM minor_fight
),
jtjyzdtzs_hit AS (
    SELECT DISTINCT
        jqjhjyzljsjtjyzdtzs_ajbh AS "案件编号",
        jqjhjyzljsjtjyzdtzs_rybh AS "人员编号"
    FROM "ywdata"."zq_zfba_jtjyzdtzs"
    WHERE jqjhjyzljsjtjyzdtzs_ajbh IS NOT NULL
    AND jqjhjyzljsjtjyzdtzs_rybh IS NOT NULL
    AND jqjhjyzljsjtjyzdtzs_wszt = '审批通过' AND jqjhjyzljsjtjyzdtzs_isdel_dm = '0'
),
baxgry_distinct AS (
    SELECT DISTINCT
        r.asjbh AS "案件编号",
        NULLIF(TRIM(r.baxgry_xm), '') AS "name",
        NULLIF(TRIM(r.lxdh), '')      AS "phone"
    FROM ywdata."zfba_ry_001" r
    INNER JOIN target_aj t
        ON r.asjbh = t."案件编号"
    WHERE NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
    AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
),
baxgry_json AS (
    SELECT
        d."案件编号",
        jsonb_agg(
            jsonb_build_object('name', d."name", 'phone', d."phone")
            ORDER BY d."name", d."phone"
        ) AS "办案联系人_json",
        jsonb_agg(
            d."phone"
            ORDER BY d."phone"
        ) AS "联系电话_json"
    FROM baxgry_distinct d
    GROUP BY d."案件编号"
)
SELECT
    mf.*,
    CASE
        WHEN jh."案件编号" IS NOT NULL THEN '是'
        ELSE '否'
    END AS "是否开具文书",
    bx."办案联系人_json",
    bx."联系电话_json"
FROM minor_fight mf
LEFT JOIN jtjyzdtzs_hit jh
    ON mf."案件编号" = jh."案件编号"
AND mf."人员编号" = jh."人员编号"
LEFT JOIN baxgry_json bx
    ON mf."案件编号" = bx."案件编号";
"""
    return _fetch_all(sql, params)


def query_cs_fa_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
    AND EXISTS (
        SELECT 1
        FROM ywdata."case_type_config" ctc
        WHERE ctc."leixing" = ANY(%s)
          AND COALESCE(aj."ajxx_aymc", '') SIMILAR TO ctc."ay_pattern"
    )"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
WITH aj_list AS (
    SELECT DISTINCT
        aj."ajxx_ajbh" AS "案件编号",
        aj."ajxx_ajmc" AS "案件名称",
        aj."ajxx_lasj" AS "立案日期",
        aj."ajxx_cbdw_mc" AS "办案单位名称",
        aj."ajxx_jyaq" AS "简要案情",
        aj."ajxx_fadd" AS "案件发生地址名称",
        aj."ajxx_fadd" AS "发案地点",
        aj."ajxx_fasj" AS "发案时间",
        aj."ajxx_ajzt" AS "案件状态",
        LEFT(aj."ajxx_cbdw_bh_dm", 6) AS "地区代码",
        CASE
            WHEN LEFT(aj."ajxx_cbdw_bh_dm", 6) = '445302' THEN '云城'
            WHEN LEFT(aj."ajxx_cbdw_bh_dm", 6) = '445303' THEN '云安'
            WHEN LEFT(aj."ajxx_cbdw_bh_dm", 6) = '445381' THEN '罗定'
            WHEN LEFT(aj."ajxx_cbdw_bh_dm", 6) = '445321' THEN '新兴'
            WHEN LEFT(aj."ajxx_cbdw_bh_dm", 6) = '445322' THEN '郁南'
            ELSE '其他'
        END AS "地区"
    FROM ywdata."zq_zfba_wcnr_ajxx" aj
    WHERE aj."ajxx_lasj" BETWEEN %s AND %s
{type_condition}
),

target_aj AS (
    SELECT DISTINCT "案件编号"
    FROM aj_list
),

baxgry_distinct AS (
    SELECT DISTINCT
        r.asjbh AS "案件编号",
        NULLIF(TRIM(r.baxgry_xm), '') AS "name",
        NULLIF(TRIM(r.lxdh), '')      AS "phone"
    FROM ywdata."zfba_ry_001" r
    INNER JOIN target_aj t
        ON r.asjbh = t."案件编号"
    WHERE NULLIF(TRIM(r.baxgry_xm), '') IS NOT NULL
      AND NULLIF(TRIM(r.lxdh), '') IS NOT NULL
),

baxgry_json AS (
    SELECT
        d."案件编号",
        jsonb_agg(
            jsonb_build_object('name', d."name", 'phone', d."phone")
            ORDER BY d."name", d."phone"
        ) AS "办案联系人_json",
        jsonb_agg(
            d."phone"
            ORDER BY d."phone"
        ) AS "联系电话_json"
    FROM baxgry_distinct d
    GROUP BY d."案件编号"
)

SELECT
    a.*,
    bx."办案联系人_json",
    bx."联系电话_json"
FROM aj_list a
LEFT JOIN baxgry_json bx
    ON a."案件编号" = bx."案件编号";
"""
    return _fetch_all(sql, params)


def query_ng_zf_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 构建类型过滤条件
    if case_types and len(case_types) > 0:
        type_condition = """
                AND EXISTS (
                    SELECT 1
                    FROM ywdata."case_type_config" ctc
                    WHERE ctc."leixing" = ANY(%s)
                      AND COALESCE(zzwx."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
                )"""
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    sql = f"""
            WITH fight_suspect AS (
                SELECT
                    zzwx."xyrxx_sfzh" AS zjhm,
                    zzwx."ajxx_join_ajxx_lasj" AS larq
                FROM ywdata."zq_zfba_wcnr_xyr" zzwx
                WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                AND zzwx."xyrxx_sfzh" IS NOT NULL
{type_condition}
            )

            SELECT
                bzr.*,
                CASE
                    WHEN bzr."ssfj_dm" ='445302000000' THEN '云城'
                    WHEN bzr."ssfj_dm" ='445303000000' THEN '云安'
                    WHEN bzr."ssfj_dm" ='445381000000' THEN '罗定'
                    WHEN bzr."ssfj_dm" ='445321000000' THEN '新兴'
                    WHEN bzr."ssfj_dm" ='445322000000' THEN '郁南'
                END AS "地区",
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM fight_suspect fs
                        WHERE fs.zjhm = bzr.zjhm
                        AND bzr.lgsj < fs.larq   -- ✅ 列管时间在立案日期之前才算再犯
                    )
                    THEN '是'
                    ELSE '否'
                END AS "是否再犯"
            FROM "stdata"."b_zdry_ryxx" bzr
            WHERE bzr.sflg = '1'
            AND bzr."deleteflag" = '0';
"""
    return _fetch_all(sql, params)


def query_case_types() -> List[str]:
    """查询案件类型列表"""
    sql = 'SELECT ctc.leixing FROM ywdata.case_type_config ctc'
    rows = _fetch_all(sql, ())
    return [row['leixing'] for row in rows]
