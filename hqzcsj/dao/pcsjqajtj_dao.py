from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


def _normalize_list(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def fetch_leixing_list(conn) -> List[Dict[str, str]]:
    sql_text = """
        SELECT DISTINCT leixing
        FROM ywdata.case_type_config
        WHERE NULLIF(BTRIM(COALESCE(leixing, '')), '') IS NOT NULL
        ORDER BY leixing
    """
    with conn.cursor() as cur:
        cur.execute(sql_text)
        rows = cur.fetchall()
    out: List[Dict[str, str]] = []
    for row in rows:
        value = str(row[0] or "").strip()
        if not value:
            continue
        out.append({"label": value, "value": value})
    return out


def fetch_fenju_list(conn) -> List[Dict[str, str]]:
    sql_text = """
        SELECT DISTINCT ssfjdm, ssfj
        FROM stdata.b_dic_zzjgdm
        WHERE NULLIF(BTRIM(COALESCE(ssfjdm, '')), '') IS NOT NULL
        ORDER BY ssfjdm
    """
    with conn.cursor() as cur:
        cur.execute(sql_text)
        rows = cur.fetchall()
    out: List[Dict[str, str]] = []
    for ssfjdm, ssfj in rows:
        value = str(ssfjdm or "").strip()
        if not value:
            continue
        label = str(ssfj or "").strip() or value
        out.append({"label": label, "value": value})
    return out


def fetch_summary_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> List[Dict[str, Any]]:
    leixing = _normalize_list(leixing_list)
    ssfjdm = _normalize_list(ssfjdm_list)
    sql_text = """
        WITH
        params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[]    AS leixing,
                %s::text[]    AS ssfjdm_list
        ),
        dic AS (
            SELECT DISTINCT ON (d.sspcsdm)
                d.sspcsdm,
                d.sspcs,
                d.ssfjdm,
                d.ssfj
            FROM stdata.b_dic_zzjgdm d
            WHERE NULLIF(BTRIM(COALESCE(d.sspcsdm, '')), '') IS NOT NULL
            ORDER BY d.sspcsdm, d.rksj DESC NULLS LAST
        ),
        high_quality_cases AS (
            SELECT ajxx_ajbh
            FROM ywdata.zq_zfba_jlz
            GROUP BY ajxx_ajbh
            HAVING COUNT(*) >= 3
        ),
        jq_stats AS (
            SELECT
                LEFT(jq.dutydeptno, 8) || '0000' AS pcsdm,
                COUNT(*)                         AS jq_cnt,
                COUNT(aj.ajxx_jqbh)             AS za_cnt
            FROM ywdata.zq_kshddpt_dsjfx_jq jq
            CROSS JOIN params p
            LEFT JOIN ywdata.zq_zfba_ajxx aj
                   ON jq.caseno = aj.ajxx_jqbh
            WHERE jq.calltime::timestamp >= p.start_time
              AND jq.calltime::timestamp <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND jq.neworicharasubclass = ANY(ctc.newcharasubclass_list)
                   )
              )
            GROUP BY LEFT(jq.dutydeptno, 8) || '0000'
        ),
        aj_stats AS (
            SELECT
                LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000' AS pcsdm,
                COUNT(*) FILTER (WHERE aj.ajxx_ajlx = '行政') AS xz_cnt,
                COUNT(*) FILTER (WHERE aj.ajxx_ajlx = '刑事') AS xs_cnt,
                COUNT(*) FILTER (
                    WHERE aj.ajxx_ajlx = '行政'
                      AND aj.ajxx_ajzt NOT IN ('已立案', '已受理')
                ) AS bjxz_cnt,
                COUNT(*) FILTER (
                    WHERE aj.ajxx_ajlx = '刑事'
                      AND aj.ajxx_ajzt NOT IN ('已立案', '已受理')
                ) AS pa_cnt,
                COUNT(DISTINCT aj.ajxx_ajbh) FILTER (
                    WHERE hq.ajxx_ajbh IS NOT NULL
                ) AS gzl_cnt
            FROM ywdata.zq_zfba_ajxx aj
            CROSS JOIN params p
            LEFT JOIN high_quality_cases hq
                   ON hq.ajxx_ajbh = aj.ajxx_ajbh
            WHERE aj.ajxx_lasj >= p.start_time
              AND aj.ajxx_lasj <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND COALESCE(aj.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
                   )
              )
            GROUP BY LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000'
        ),
        zhiju_stats AS (
            SELECT
                LEFT(xz.xzcfjds_cbdw_bh_dm, 8) || '0000' AS pcsdm,
                COUNT(*)                                 AS zhiju_cnt
            FROM ywdata.zq_zfba_xzcfjds xz
            CROSS JOIN params p
            LEFT JOIN ywdata.zq_zfba_ajxx aj_xz
                   ON aj_xz.ajxx_ajbh = xz.ajxx_ajbh
            WHERE xz.xzcfjds_cfzl ~ '拘留'
              AND xz.xzcfjds_spsj >= p.start_time
              AND xz.xzcfjds_spsj <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND COALESCE(aj_xz.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
                   )
              )
            GROUP BY LEFT(xz.xzcfjds_cbdw_bh_dm, 8) || '0000'
        ),
        xingju_stats AS (
            SELECT
                LEFT(jlz.jlz_cbdw_bh_dm, 8) || '0000' AS pcsdm,
                COUNT(*)                              AS xingju_cnt
            FROM ywdata.zq_zfba_jlz jlz
            CROSS JOIN params p
            WHERE jlz.jlz_pzsj >= p.start_time
              AND jlz.jlz_pzsj <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND COALESCE(jlz.jlz_ay_mc, '') SIMILAR TO ctc.ay_pattern
                   )
              )
            GROUP BY LEFT(jlz.jlz_cbdw_bh_dm, 8) || '0000'
        ),
        daibu_stats AS (
            SELECT
                LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000' AS pcsdm,
                COUNT(*)                              AS daibu_cnt
            FROM ywdata.zq_zfba_dbz dbz
            CROSS JOIN params p
            LEFT JOIN ywdata.zq_zfba_ajxx aj_dbz
                   ON aj_dbz.ajxx_ajbh = dbz.ajxx_ajbh
            WHERE dbz.dbz_pzdbsj >= p.start_time
              AND dbz.dbz_pzdbsj <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND COALESCE(aj_dbz.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
                   )
              )
            GROUP BY LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000'
        ),
        qisu_stats AS (
            SELECT
                LEFT(qsryxx.qsryxx_cbdw_bh_dm, 8) || '0000' AS pcsdm,
                COUNT(*)                                    AS qisu_cnt
            FROM ywdata.zq_zfba_qsryxx qsryxx
            CROSS JOIN params p
            LEFT JOIN ywdata.zq_zfba_ajxx aj_qs
                   ON aj_qs.ajxx_ajbh = qsryxx.ajxx_ajbh
            WHERE qsryxx.qsryxx_tfsj >= p.start_time
              AND qsryxx.qsryxx_tfsj <= p.end_time
              AND (
                   COALESCE(cardinality(p.leixing), 0) = 0
                   OR EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(p.leixing)
                          AND COALESCE(aj_qs.ajxx_aymc, '') SIMILAR TO ctc.ay_pattern
                   )
              )
            GROUP BY LEFT(qsryxx.qsryxx_cbdw_bh_dm, 8) || '0000'
        )
        SELECT
            COALESCE(d.ssfj, '')                    AS "所属分局",
            COALESCE(d.ssfjdm, '')                  AS "所属分局代码",
            COALESCE(d.sspcs, '')                   AS "派出所名称",
            COALESCE(d.sspcsdm, '')                 AS "派出所代码",
            COALESCE(j.jq_cnt, 0)                   AS "警情",
            COALESCE(j.za_cnt, 0)                   AS "转案",
            COALESCE(a.xz_cnt, 0)                   AS "行政",
            COALESCE(a.xs_cnt, 0)                   AS "刑事",
            COALESCE(a.bjxz_cnt, 0)                 AS "办结",
            COALESCE(a.pa_cnt, 0)                   AS "破案",
            COALESCE(a.gzl_cnt, 0)                  AS "高质量",
            COALESCE(z.zhiju_cnt, 0)                AS "治拘",
            COALESCE(x.xingju_cnt, 0)               AS "刑拘",
            COALESCE(db.daibu_cnt, 0)               AS "逮捕",
            COALESCE(q.qisu_cnt, 0)                 AS "起诉"
        FROM dic d
        CROSS JOIN params p
        LEFT JOIN jq_stats j
               ON j.pcsdm = d.sspcsdm
        LEFT JOIN aj_stats a
               ON a.pcsdm = d.sspcsdm
        LEFT JOIN zhiju_stats z
               ON z.pcsdm = d.sspcsdm
        LEFT JOIN xingju_stats x
               ON x.pcsdm = d.sspcsdm
        LEFT JOIN daibu_stats db
               ON db.pcsdm = d.sspcsdm
        LEFT JOIN qisu_stats q
               ON q.pcsdm = d.sspcsdm
        WHERE COALESCE(cardinality(p.ssfjdm_list), 0) = 0
           OR d.ssfjdm = ANY(p.ssfjdm_list)
        ORDER BY d.ssfj, d.sspcs
    """
    with conn.cursor() as cur:
        cur.execute(sql_text, [start_time, end_time, leixing, ssfjdm])
        columns = [d[0] for d in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("警情", "转案", "行政", "刑事", "办结", "破案", "高质量", "治拘", "刑拘", "逮捕", "起诉"):
            item[key] = _to_int(item.get(key))
        out.append(item)
    return out


def _fetch_ay_patterns(conn, *, leixing_list: Sequence[str]) -> List[str]:
    leixing = _normalize_list(leixing_list)
    if not leixing:
        return []
    sql_text = """
        SELECT ay_pattern
        FROM ywdata.case_type_config
        WHERE leixing = ANY(%s)
          AND NULLIF(BTRIM(COALESCE(ay_pattern, '')), '') IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql_text, [leixing])
        rows = cur.fetchall()
    out: List[str] = []
    for row in rows:
        pattern = str(row[0] or "").strip()
        if pattern:
            out.append(pattern)
    return out


def fetch_detail_rows(
    conn,
    *,
    metric: str,
    pcsdm: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], bool]:
    metric_text = str(metric or "").strip()
    pcsdm_text = str(pcsdm or "").strip()
    is_all = pcsdm_text in ("", "__ALL__", "全市")
    leixing = _normalize_list(leixing_list)
    patterns = _fetch_ay_patterns(conn, leixing_list=leixing)
    limit_n = int(limit) if limit and int(limit) > 0 else 0
    truncated = False

    def append_limit(query: str, params: List[Any]) -> Tuple[str, List[Any]]:
        if not limit_n:
            return query, params
        return query + " LIMIT %s", params + [limit_n + 1]

    def apply_result(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], bool]:
        nonlocal truncated
        if limit_n and len(rows) > limit_n:
            truncated = True
            return rows[:limit_n], truncated
        return rows, truncated

    with conn.cursor() as cur:
        if metric_text == "警情":
            params: List[Any] = [start_time, end_time]
            where_parts = [
                "jq.calltime::timestamp >= %s",
                "jq.calltime::timestamp <= %s",
            ]
            if leixing:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM ywdata.case_type_config ctc
                        WHERE ctc.leixing = ANY(%s)
                          AND jq.neworicharasubclass = ANY(ctc.newcharasubclass_list)
                    )
                    """
                )
                params.append(leixing)
            if not is_all:
                where_parts.append("LEFT(jq.dutydeptno, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    jq.calltime AS "报警时间",
                    jq.caseno AS "警情编号",
                    jq.dutydeptname AS "管辖单位",
                    jq.cmdname AS "分局",
                    jq.occuraddress AS "警情地址",
                    jq.casecontents AS "报警内容",
                    jq.replies AS "处警情况",
                    jq.casemark AS "警情标注",
                    jq.lngofcriterion AS "经度",
                    jq.latofcriterion AS "纬度",
                    LEFT(jq.dutydeptno, 8) || '0000' AS "派出所代码",
                    aj.ajxx_ajbh AS "转案案件编号",
                    aj.ajxx_lasj AS "立案时间",
                    aj.ajxx_cbdw_mc AS "办案单位"
                FROM ywdata.zq_kshddpt_dsjfx_jq jq
                LEFT JOIN ywdata.zq_zfba_ajxx aj
                       ON jq.caseno = aj.ajxx_jqbh
                WHERE {" AND ".join(where_parts)}
                ORDER BY jq.calltime DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text in ("行政", "刑事", "办结", "破案"):
            params = [start_time, end_time]
            where_parts = [
                "aj.ajxx_lasj >= %s",
                "aj.ajxx_lasj <= %s",
            ]
            if metric_text in ("行政", "刑事"):
                where_parts.append("aj.ajxx_ajlx = %s")
                params.append(metric_text)
            if metric_text == "办结":
                where_parts.append("aj.ajxx_ajlx = '行政'")
                where_parts.append("aj.ajxx_ajzt NOT IN ('已立案', '已受理')")
            if metric_text == "破案":
                where_parts.append("aj.ajxx_ajlx = '刑事'")
                where_parts.append("aj.ajxx_ajzt NOT IN ('已立案', '已受理')")
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(aj.ajxx_aymc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    aj.ajxx_ajbh AS "案件编号",
                    aj.ajxx_jqbh AS "警情编号",
                    aj.ajxx_ajmc AS "案件名称",
                    aj.ajxx_ajlx AS "案件类型",
                    aj.ajxx_ajzt AS "案件状态",
                    aj.ajxx_aymc AS "案由",
                    aj.ajxx_lasj AS "立案时间",
                    aj.ajxx_fasj AS "发案时间",
                    aj.ajxx_cbdw_mc AS "办案单位",
                    LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000' AS "派出所代码"
                FROM ywdata.zq_zfba_ajxx aj
                WHERE {" AND ".join(where_parts)}
                ORDER BY aj.ajxx_lasj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text == "高质量":
            params = [start_time, end_time]
            where_parts = [
                "aj.ajxx_lasj >= %s",
                "aj.ajxx_lasj <= %s",
                "aj.ajxx_ajlx = '刑事'",
            ]
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(aj.ajxx_aymc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    aj.ajxx_ajbh AS "案件编号",
                    aj.ajxx_jqbh AS "警情编号",
                    aj.ajxx_ajmc AS "案件名称",
                    aj.ajxx_ajzt AS "案件状态",
                    aj.ajxx_aymc AS "案由",
                    aj.ajxx_lasj AS "立案时间",
                    aj.ajxx_cbdw_mc AS "办案单位",
                    LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000' AS "派出所代码",
                    COUNT(jlz.jlz_id) AS "刑拘人数"
                FROM ywdata.zq_zfba_ajxx aj
                LEFT JOIN ywdata.zq_zfba_jlz jlz
                       ON aj.ajxx_ajbh = jlz.ajxx_ajbh
                WHERE {" AND ".join(where_parts)}
                GROUP BY
                    aj.ajxx_ajbh,
                    aj.ajxx_jqbh,
                    aj.ajxx_ajmc,
                    aj.ajxx_ajzt,
                    aj.ajxx_aymc,
                    aj.ajxx_lasj,
                    aj.ajxx_cbdw_mc,
                    LEFT(aj.ajxx_cbdw_bh_dm, 8) || '0000'
                HAVING COUNT(jlz.jlz_id) >= 3
                ORDER BY aj.ajxx_lasj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text == "治拘":
            params = [start_time, end_time]
            where_parts = [
                "xz.xzcfjds_cfzl ~ '拘留'",
                "xz.xzcfjds_spsj >= %s",
                "xz.xzcfjds_spsj <= %s",
            ]
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(aj_xz.ajxx_aymc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(xz.xzcfjds_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    xz.xzcfjds_id AS "决定书ID",
                    xz.ajxx_ajbh AS "案件编号",
                    xz.xzcfjds_ajmc AS "案件名称",
                    xz.xzcfjds_ryxm AS "人员姓名",
                    xz.xzcfjds_cfzl AS "处罚种类",
                    xz.xzcfjds_spsj AS "审批时间",
                    xz.xzcfjds_cbdw_mc AS "办案单位",
                    LEFT(xz.xzcfjds_cbdw_bh_dm, 8) || '0000' AS "派出所代码",
                    xz.xzcfjds_wsh AS "文书号"
                FROM ywdata.zq_zfba_xzcfjds xz
                LEFT JOIN ywdata.zq_zfba_ajxx aj_xz
                       ON aj_xz.ajxx_ajbh = xz.ajxx_ajbh
                WHERE {" AND ".join(where_parts)}
                ORDER BY xz.xzcfjds_spsj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text == "刑拘":
            params = [start_time, end_time]
            where_parts = [
                "jlz.jlz_pzsj >= %s",
                "jlz.jlz_pzsj <= %s",
            ]
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(jlz.jlz_ay_mc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(jlz.jlz_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    jlz.jlz_id AS "拘留证ID",
                    jlz.ajxx_ajbh AS "案件编号",
                    jlz.jlz_ajmc AS "案件名称",
                    jlz.jlz_ryxm AS "人员姓名",
                    jlz.jlz_jlyy_c AS "拘留原因",
                    jlz.jlz_pzsj AS "批准时间",
                    jlz.jlz_cbdw_mc AS "办案单位",
                    LEFT(jlz.jlz_cbdw_bh_dm, 8) || '0000' AS "派出所代码",
                    jlz.jlz_wsh AS "文书号"
                FROM ywdata.zq_zfba_jlz jlz
                WHERE {" AND ".join(where_parts)}
                ORDER BY jlz.jlz_pzsj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text == "逮捕":
            params = [start_time, end_time]
            where_parts = [
                "dbz.dbz_pzdbsj >= %s",
                "dbz.dbz_pzdbsj <= %s",
            ]
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(aj_dbz.ajxx_aymc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    dbz.dbz_id AS "逮捕证ID",
                    dbz.ajxx_ajbh AS "案件编号",
                    dbz.dbz_ajmc AS "案件名称",
                    dbz.dbz_ryxm AS "人员姓名",
                    dbz.dbz_dbyy AS "逮捕原因",
                    dbz.dbz_pzdbsj AS "批准逮捕时间",
                    dbz.dbz_cbdw_mc AS "办案单位",
                    LEFT(dbz.dbz_cbdw_bh_dm, 8) || '0000' AS "派出所代码",
                    dbz.dbz_wsh AS "文书号"
                FROM ywdata.zq_zfba_dbz dbz
                LEFT JOIN ywdata.zq_zfba_ajxx aj_dbz
                       ON aj_dbz.ajxx_ajbh = dbz.ajxx_ajbh
                WHERE {" AND ".join(where_parts)}
                ORDER BY dbz.dbz_pzdbsj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

        if metric_text == "起诉":
            params = [start_time, end_time]
            where_parts = [
                "qs.qsryxx_tfsj >= %s",
                "qs.qsryxx_tfsj <= %s",
            ]
            if patterns:
                where_parts.append(
                    """
                    EXISTS (
                        SELECT 1
                        FROM unnest(%s::text[]) p(pattern)
                        WHERE COALESCE(aj_qs.ajxx_aymc, '') SIMILAR TO p.pattern
                    )
                    """
                )
                params.append(patterns)
            if not is_all:
                where_parts.append("LEFT(qs.qsryxx_cbdw_bh_dm, 8) || '0000' = %s")
                params.append(pcsdm_text)
            sql_text = f"""
                SELECT
                    qs.qsryxx_id AS "起诉ID",
                    qs.ajxx_ajbh AS "案件编号",
                    qs.qsryxx_ajmc AS "案件名称",
                    qs.qsryxx_ryxm AS "人员姓名",
                    qs.qsryxx_tfsj AS "提诉时间",
                    qs.qsryxx_cbdw_mc AS "办案单位",
                    LEFT(qs.qsryxx_cbdw_bh_dm, 8) || '0000' AS "派出所代码",
                    qs.qsryxx_wsh AS "文书号"
                FROM ywdata.zq_zfba_qsryxx qs
                LEFT JOIN ywdata.zq_zfba_ajxx aj_qs
                       ON aj_qs.ajxx_ajbh = qs.ajxx_ajbh
                WHERE {" AND ".join(where_parts)}
                ORDER BY qs.qsryxx_tfsj DESC
            """
            sql_text, params = append_limit(sql_text, params)
            cur.execute(sql_text, params)
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return apply_result(rows)

    raise ValueError(f"未知 metric: {metric_text}")
