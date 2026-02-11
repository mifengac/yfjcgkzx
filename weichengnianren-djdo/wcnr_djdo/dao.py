from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from psycopg2 import sql

from gonggong.config.database import get_database_connection
from .constants import map_region_name


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


def _attach_region_name(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for row in rows:
        code_or_name = row.get("地区代码")
        if code_or_name in (None, ""):
            code_or_name = row.get("地区")
        row["地区"] = map_region_name(code_or_name)
    return rows


def _resolve_xjs2_join_cols(conn) -> Tuple[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            ("ywdata", "zq_zfba_xjs2"),
        )
        cols = {str(r[0]) for r in cur.fetchall() if r and r[0]}

    ajbh_col = next((c for c in ("ajbh", "AJBH") if c in cols), "")
    xm_col = next((c for c in ("xgry_xm", "XGRY_XM") if c in cols), "")
    if not ajbh_col or not xm_col:
        raise RuntimeError(
            f'无法识别表 ywdata."zq_zfba_xjs2" 的字段：ajbh/xgry_xm（当前列：{sorted(cols)}）'
        )
    return ajbh_col, xm_col


def _resolve_jtjyz_rybh_col(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            ("ywdata", "zq_zfba_jtjyzdtzs"),
        )
        cols = {str(r[0]) for r in cur.fetchall() if r and r[0]}

    for c in ("jqjhjyzljsjtjyzdtzs_rybh", "jqjhjyzljsjtjyzdtzs_wcnrrybh"):
        if c in cols:
            return c
    raise RuntimeError(
        f'无法识别表 ywdata."zq_zfba_jtjyzdtzs" 的人员编号字段（当前列：{sorted(cols)}）'
    )


def query_jq_za_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    if case_types and len(case_types) > 0:
        type_condition = '''
  AND jq.newcharasubclass IN (
      SELECT unnest(ctc.newcharasubclass_list) 
      FROM ywdata.case_type_config ctc 
      WHERE ctc.leixing = ANY(%s)
  )'''
        params = (start_time, end_time, case_types)
    else:
        type_condition = ""
        params = (start_time, end_time)

    q = f"""
SELECT
    jq."caseno" AS "警情编号",
    jq."calltime" AS "报警时间",
    LEFT(jq."cmdid", 6) AS "地区代码",
    jq."dutydeptname" AS "管辖单位",
    jq."casecontents" AS "报警内容",
    zza."ajxx_ajbh" AS "案件编号",
    zza."ajxx_ajmc" AS "案件名称",
    zza."ajxx_cbdw_mc" AS "办案单位名称",
    zza."ajxx_jyaq" AS "简要案情",
    zza."ajxx_ajzt" AS "案件状态"
FROM ywdata.zq_kshddpt_dsjfx_jq jq
LEFT JOIN ywdata.zq_zfba_ajxx zza
    ON jq.caseno = zza.ajxx_jqbh
WHERE jq.calltime BETWEEN %s AND %s
  AND LEFT(jq.newcharasubclass, 2) IN ('01', '02')
  AND jq.casemark ~ '未成年'
{type_condition}
"""
    rows = _fetch_all(q, params)
    return _attach_region_name(rows)


def _query_jzjy_base_details(
    start_time: datetime,
    end_time: datetime,
    case_types: List[str] | None = None,
) -> List[Dict[str, Any]]:
    conn = get_database_connection()
    try:
        xjs2_ajbh_col, xjs2_xm_col = _resolve_xjs2_join_cols(conn)
        jtjyz_rybh_col = _resolve_jtjyz_rybh_col(conn)

        if case_types and len(case_types) > 0:
            type_condition = sql.SQL(
                """
                AND EXISTS (
                    SELECT 1
                    FROM "ywdata"."case_type_config" ctc
                    WHERE ctc."leixing" = ANY(%s)
                      AND vw."案由" SIMILAR TO ctc."ay_pattern"
                )
                """
            )
            params: List[Any] = [start_time, end_time, case_types]
        else:
            type_condition = sql.SQL("")
            params = [start_time, end_time]

        q = sql.SQL(
            """
            WITH violation_counts AS (
                SELECT
                    w."xyrxx_sfzh" AS 身份证号,
                    COUNT(*) AS 违法次数,
                    COUNT(DISTINCT w."ajxx_join_ajxx_ay_dm") AS 不同案由数
                FROM "ywdata"."zq_zfba_wcnr_xyr" w
                WHERE COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
                  AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
                GROUP BY w."xyrxx_sfzh"
            ),
            first_case_xjs AS (
                SELECT DISTINCT
                    vw."身份证号" AS 身份证号,
                    vw."案件编号" AS 当前案件编号,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM "ywdata"."zq_zfba_wcnr_xyr" w
                            JOIN "ywdata"."zq_zfba_xjs2" x
                              ON w."ajxx_join_ajxx_ajbh" = x.{xjs2_ajbh_col}
                             AND w."xyrxx_xm" = x.{xjs2_xm_col}
                            WHERE w."xyrxx_sfzh" = vw."身份证号"
                              AND w."ajxx_join_ajxx_ajbh" <> vw."案件编号"
                              AND COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
                              AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
                        ) THEN 1
                        ELSE 0
                    END AS 有训诫书
                FROM "ywdata"."v_wcnr_wfry_base" vw
            ),
            base_data AS (
                SELECT DISTINCT
                    vw."案件编号",
                    vw."人员编号",
                    vw."案件类型",
                    vw."案由",
                    vw."地区" AS "地区代码",
                    vw."办案单位",
                    TO_CHAR(vw."立案时间", 'YYYY-MM-DD HH24:MI:SS') AS "立案时间",
                    TO_CHAR(vw."立案时间", 'YYYY-MM-DD HH24:MI:SS') AS "立案日期",
                    vw."姓名",
                    vw."身份证号",
                    vw."户籍地",
                    vw."年龄",
                    vw."居住地",
                    vw."居住地" AS "现住地",
                    '嫌疑人' AS "人员类型",
                    CASE
                        WHEN vw."案件类型" = '行政' AND EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                            WHERE x."ajxx_ajbh" = vw."案件编号"
                              AND x."xzcfjds_rybh" = vw."人员编号"
                              AND NULLIF(TRIM(x."xzcfjds_tj_jlts"), '') ~ '^\\d+$'
                              AND CAST(x."xzcfjds_tj_jlts" AS INTEGER) > 4
                        ) THEN '是'
                        ELSE '否'
                    END AS "治拘大于4天",
                    CASE
                        WHEN vw."案件类型" = '行政'
                             AND COALESCE(vc."违法次数", 0) = 2
                             AND COALESCE(vc."不同案由数", 0) = 1
                             AND COALESCE(fcx."有训诫书", 0) = 1
                        THEN '是'
                        ELSE '否'
                    END AS "2次违法且案由相同且第一次违法开具了训诫书",
                    CASE
                        WHEN vw."案件类型" = '行政' AND COALESCE(vc."违法次数", 0) > 2 THEN '是'
                        ELSE '否'
                    END AS "3次及以上违法",
                    CASE
                        WHEN vw."案件类型" = '刑事' AND EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                            WHERE j."ajxx_ajbh" = vw."案件编号" AND j."jlz_rybh" = vw."人员编号"
                        ) THEN '是'
                        ELSE '否'
                    END AS "是否刑拘",
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" z
                            WHERE z."zltzs_ajbh" = vw."案件编号" AND z."zltzs_rybh" = vw."人员编号"
                        ) OR EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_xjs2" x
                            WHERE x.{xjs2_ajbh_col} = vw."案件编号" AND x.{xjs2_xm_col} = vw."姓名"
                        ) THEN '是'
                        ELSE '否'
                    END AS "是否开具矫治文书",
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_jtjyzdtzs" j
                            WHERE j."jqjhjyzljsjtjyzdtzs_ajbh" = vw."案件编号"
                              AND j.{jtjyz_rybh_col} = vw."人员编号"
                        ) THEN '是'
                        ELSE '否'
                    END AS "是否开具家庭教育指导书",
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM "ywdata"."zq_zfba_tqzmjy" t
                            WHERE t."ajbh" = vw."案件编号" AND t."xgry_xm" = vw."姓名"
                        ) THEN '是'
                        ELSE '否'
                    END AS "是否开具专门教育申请书",
                    CASE
                        WHEN (CASE WHEN COALESCE(vw."年龄"::text, '') ~ '^\\d+$' THEN CAST(vw."年龄" AS INTEGER) END) > 11
                             AND (
                                (vw."案件类型" = '行政' AND EXISTS (
                                    SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                                    WHERE x."ajxx_ajbh" = vw."案件编号"
                                      AND x."xzcfjds_rybh" = vw."人员编号"
                                      AND NULLIF(TRIM(x."xzcfjds_tj_jlts"), '') ~ '^\\d+$'
                                      AND CAST(x."xzcfjds_tj_jlts" AS INTEGER) > 4
                                ))
                                OR (vw."案件类型" = '行政'
                                    AND COALESCE(vc."违法次数", 0) = 2
                                    AND COALESCE(vc."不同案由数", 0) = 1
                                    AND COALESCE(fcx."有训诫书", 0) = 1
                                )
                                OR (vw."案件类型" = '行政' AND COALESCE(vc."违法次数", 0) > 2)
                                OR (vw."案件类型" = '刑事' AND EXISTS (
                                    SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                                    WHERE j."ajxx_ajbh" = vw."案件编号" AND j."jlz_rybh" = vw."人员编号"
                                ))
                             )
                        THEN '是'
                        ELSE '否'
                    END AS "是否符合送生",
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM "ywdata"."zq_wcnr_sfzxx" s
                            WHERE s."sfzhm" = vw."身份证号"
                              AND s."rx_time" > vw."立案时间"
                        ) THEN '是'
                        ELSE '否'
                    END AS "是否送校"
                FROM "ywdata"."v_wcnr_wfry_base" vw
                LEFT JOIN violation_counts vc ON vw."身份证号" = vc.身份证号
                LEFT JOIN first_case_xjs fcx ON vw."身份证号" = fcx.身份证号 AND vw."案件编号" = fcx.当前案件编号
                WHERE vw."录入时间" BETWEEN %s AND %s
                {type_condition}
            ),
            target_aj AS (
                SELECT DISTINCT "案件编号"
                FROM base_data
            ),
            baxgry_distinct AS (
                SELECT DISTINCT
                    r."asjbh" AS "案件编号",
                    NULLIF(TRIM(r."baxgry_xm"), '') AS "name",
                    NULLIF(TRIM(r."lxdh"), '') AS "phone"
                FROM "ywdata"."zfba_ry_001" r
                INNER JOIN target_aj t ON r."asjbh" = t."案件编号"
                WHERE NULLIF(TRIM(r."baxgry_xm"), '') IS NOT NULL
                  AND NULLIF(TRIM(r."lxdh"), '') IS NOT NULL
            ),
            baxgry_json AS (
                SELECT
                    d."案件编号",
                    jsonb_agg(jsonb_build_object('name', d."name", 'phone', d."phone") ORDER BY d."name", d."phone") AS "办案联系人_json",
                    jsonb_agg(d."phone" ORDER BY d."phone") AS "联系电话_json"
                FROM baxgry_distinct d
                GROUP BY d."案件编号"
            )
            SELECT
                bd.*,
                bd."身份证号" AS "证件号码",
                aj."ajxx_ajmc" AS "案件名称",
                aj."ajxx_jyaq" AS "简要案情",
                COALESCE(aj."ajxx_cbdw_mc", bd."办案单位") AS "办案单位名称",
                bx."办案联系人_json",
                bx."联系电话_json"
            FROM base_data bd
            LEFT JOIN "ywdata"."zq_zfba_ajxx" aj ON aj."ajxx_ajbh" = bd."案件编号"
            LEFT JOIN baxgry_json bx ON bd."案件编号" = bx."案件编号"
            ORDER BY bd."案件编号", bd."人员编号"
            """
        ).format(
            type_condition=type_condition,
            xjs2_ajbh_col=sql.Identifier(xjs2_ajbh_col),
            xjs2_xm_col=sql.Identifier(xjs2_xm_col),
            jtjyz_rybh_col=sql.Identifier(jtjyz_rybh_col),
        )

        with conn.cursor() as cur:
            cur.execute(q, tuple(params))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        out = [dict(zip(cols, r)) for r in rows]
        return _attach_region_name(out)
    finally:
        conn.close()


def query_jzjy_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    rows = _query_jzjy_base_details(start_time=start_time, end_time=end_time, case_types=case_types)
    for row in rows:
        row["是否开具文书"] = "是" if str(row.get("是否开具矫治文书") or "") == "是" else "否"
    return rows


def query_sx_sx_details(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> List[Dict[str, Any]]:
    # 复用“矫治教育”基础明细口径，保持“是否符合送生/是否送校”字段一致。
    rows = _query_jzjy_base_details(start_time=start_time, end_time=end_time, case_types=case_types)
    out: List[Dict[str, Any]] = []
    for r in rows:
        if str(r.get("人员编号") or "") == "R4453816500002026013043":
            continue
        if str(r.get("案件类型") or "") != "刑事":
            continue
        out.append(r)
    return out


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
    LEFT JOIN ywdata."zq_zfba_ajxx" aj
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
        jqjhjyzljsjtjyzdtzs_wcnrrybh  AS "人员编号"
    FROM "ywdata"."zq_zfba_jtjyzdtzs"
    WHERE jqjhjyzljsjtjyzdtzs_ajbh IS NOT NULL
    AND jqjhjyzljsjtjyzdtzs_wcnrrybh IS NOT NULL
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
