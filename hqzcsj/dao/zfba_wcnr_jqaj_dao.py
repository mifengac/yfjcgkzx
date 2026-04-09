from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg2 import sql

from gonggong.config.database import DB_CONFIG
from hqzcsj.dao import jzqk_tongji_dao
from hqzcsj.dao.wcnr_case_list_dao import (
    build_minor_case_detail_rows,
    count_minor_case_rows_by_region,
    fetch_minor_case_rows,
    filter_minor_case_rows_by_subclasses,
)
from hqzcsj.dao.zfba_jq_aj_dao import fetch_ay_patterns, fetch_leixing_list


SCHEMA = DB_CONFIG.get("schema") or "ywdata"
_ADDR_MODEL_UNAVAILABLE = False
_KEY_INDUSTRY_ADDR_KEYWORDS = (
    "ktv",
    "酒吧",
    "网吧",
    "酒店",
    "宾馆",
    "旅馆",
    "民宿",
    "出租屋",
    "出租房",
    "公寓",
    "会所",
    "沐足",
    "足疗",
    "按摩",
    "洗浴",
    "桑拿",
    "棋牌",
    "麻将",
    "歌舞",
    "夜总会",
    "台球",
    "电竞",
    "游戏厅",
)


def fetch_newcharasubclass_list(conn, *, leixing_list: Sequence[str]) -> List[str]:
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    if not leixing_list:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT unnest(ctc.newcharasubclass_list) AS subclass
            FROM "ywdata"."case_type_config" ctc
            WHERE ctc.leixing = ANY(%s)
            """,
            (list(leixing_list),),
        )
        rows = cur.fetchall()
    out: List[str] = []
    for r in rows:
        if not r:
            continue
        s = "" if r[0] is None else str(r[0]).strip()
        if s:
            out.append(s)
    return out


def _exists_similar_to_patterns(patterns: Sequence[str], *, field_expr: sql.SQL) -> Tuple[sql.SQL, List[Any]]:
    pats = [str(x).strip() for x in (patterns or []) if str(x).strip()]
    if not pats:
        return sql.SQL(""), []
    frag = sql.SQL(
        " AND EXISTS (SELECT 1 FROM unnest(%s::text[]) p(pattern) WHERE {field} SIMILAR TO p.pattern)"
    ).format(field=field_expr)
    return frag, [pats]


def _cfzl_regex_condition(values: Sequence[str]) -> Tuple[sql.SQL, List[Any]]:
    vals = [str(x).strip() for x in (values or []) if str(x).strip()]
    if not vals:
        return sql.SQL(""), []
    pattern = "(" + "|".join(vals) + ")"
    return sql.SQL(" AND xz.xzcfjds_cfzl ~ %s"), [pattern]


def count_jq_by_diqu(conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]) -> Dict[str, int]:
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    subclass_codes = fetch_newcharasubclass_list(conn, leixing_list=leixing_list) if leixing_list else None
    if leixing_list and not subclass_codes:
        return {}

    rows = fetch_minor_case_rows(start_time=start_time, end_time=end_time)
    filtered_rows = filter_minor_case_rows_by_subclasses(rows, subclass_codes=subclass_codes)
    return count_minor_case_rows_by_region(filtered_rows)


def count_zhuanan_by_diqu(conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]) -> Dict[str, int]:
    """转案数：按警情去重，统计已关联案件编号的警情数量。"""
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    where_type = sql.SQL("")
    params: List[Any] = [start_time, end_time]
    if leixing_list:
        where_type = sql.SQL(
            '''
  AND jq.newcharasubclass IN (
      SELECT unnest(ctc.newcharasubclass_list) 
      FROM ywdata.case_type_config ctc 
      WHERE ctc.leixing = ANY(%s)
  )'''
        )
        params.append(leixing_list)

    q = (
        sql.SQL(
            """
            SELECT LEFT(jq."cmdid", 6) AS diqu, COUNT(DISTINCT jq."caseno") AS cnt
            FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq
            LEFT JOIN "ywdata"."zq_zfba_ajxx" zza ON jq."caseno" = zza."ajxx_jqbh"
            WHERE jq."calltime" BETWEEN %s AND %s
              AND (jq."casemark" ~ '未成年' OR jq."casemarkok" ~ '未成年')
              AND LEFT(jq."newcharasubclass", 2) IN ('01','02')
              AND NULLIF(BTRIM(zza."ajxx_ajbh"), '') IS NOT NULL
              AND 1=1
            """
        )
        + where_type
        + sql.SQL(' GROUP BY LEFT(jq."cmdid", 6)')
    )
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_ajxx_by_diqu_and_ajlx(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str]
) -> Dict[str, Dict[str, int]]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(ajxx_cbdw_bh_dm, 6) AS diqu,
              ajxx_ajlx AS ajlx,
              COUNT(1) AS cnt
            FROM {schema}.zq_zfba_wcnr_ajxx
            WHERE ajxx_lasj BETWEEN %s AND %s
              AND ajxx_ajlx IN ('行政','刑事')
              AND ajxx_ajzt NOT IN ('已撤销','已合并')
              AND ajxx_cbdw_mc !~ '交通'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu, ajlx")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, Dict[str, int]] = {"行政": {}, "刑事": {}}
    for diqu, ajlx, cnt in rows:
        if not diqu or not ajlx:
            continue
        ajlx_s = str(ajlx)
        if ajlx_s in out:
            out[ajlx_s][str(diqu)] = int(cnt or 0)
    return out


def _append_addr_predictions(rows: List[Dict[str, Any]], *, addr_col: str = "发案地点") -> None:
    def _fallback_predict(text: str) -> Tuple[str, float]:
        t = str(text or "").strip().lower()
        if not t:
            return ("", 0.0)
        for kw in _KEY_INDUSTRY_ADDR_KEYWORDS:
            if kw in t:
                return ("重点管控行业", 0.0)
        return ("", 0.0)

    unique_texts: List[str] = []
    seen = set()
    for row in rows:
        text = str((row.get(addr_col) or "")).strip()
        if text in seen:
            continue
        seen.add(text)
        unique_texts.append(text)

    pred_map: Dict[str, Tuple[str, float]] = {}
    if unique_texts:
        global _ADDR_MODEL_UNAVAILABLE  # noqa: PLW0603
        preds: List[Tuple[str, float]]
        if _ADDR_MODEL_UNAVAILABLE:
            preds = [_fallback_predict(x) for x in unique_texts]
        else:
            try:
                from xunfang.service.jiemiansanlei_service import predict_addresses

                preds = predict_addresses(unique_texts)
            except Exception as exc:  # noqa: BLE001
                _ADDR_MODEL_UNAVAILABLE = True
                logging.exception(
                    "地址分类模型不可用，已降级为规则分类（可能导致场所案件统计偏低）: %s",
                    exc,
                )
                preds = [_fallback_predict(x) for x in unique_texts]
        for text, (label, prob) in zip(unique_texts, preds):
            pred_map[text] = (str(label or "").strip(), float(prob or 0.0))

    for row in rows:
        text = str((row.get(addr_col) or "")).strip()
        label, prob = pred_map.get(text, ("", 0.0))
        row["分类结果"] = label
        try:
            row["置信度"] = f"{float(prob):.5f}"
        except Exception:
            row["置信度"] = "0.00000"


def fetch_wcnr_ajxx_changsuo_base_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    patterns: Sequence[str],
    diqu: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
    where_diqu = sql.SQL("")
    params: List[Any] = [start_time, end_time] + list(pat_params)
    if diqu:
        where_diqu = sql.SQL(" AND LEFT(ajxx_cbdw_bh_dm, 6) = %s")
        params.append(str(diqu).strip())

    q = (
        sql.SQL(
            """
            SELECT
              ajxx_ajbh AS "案件编号",
              ajxx_jqbh AS "警情编号",
              ajxx_ajmc AS "案件名称",
              ajxx_ajlx AS "案件类型",
              ajxx_aymc AS "案由名称",
              ajxx_lasj AS "立案时间",
              ajxx_cbdw_mc AS "承办单位",
              LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
              ajxx_jyaq AS "简要案情",
              ajxx_aymc AS "案由",
              ajxx_ajzt AS "案件状态",
              ajxx_fadd AS "发案地点",
              ajxx_fasj AS "发案时间"
            FROM {schema}.zq_zfba_wcnr_ajxx
            WHERE ajxx_lasj BETWEEN %s AND %s
              AND ajxx_ajzt NOT IN ('已撤销','已合并')
              AND ajxx_cbdw_mc !~ '交通'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + where_diqu
        + sql.SQL(" ORDER BY ajxx_lasj DESC")
    )
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def count_changsuo_ajxx_by_diqu(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str]
) -> Dict[str, int]:
    base_rows = fetch_wcnr_ajxx_changsuo_base_rows(
        conn, start_time=start_time, end_time=end_time, patterns=patterns, diqu=None
    )
    if not base_rows:
        return {}

    _append_addr_predictions(base_rows, addr_col="发案地点")
    out: Dict[str, int] = {}
    for row in base_rows:
        if str(row.get("分类结果") or "").strip() != "重点管控行业":
            continue
        diqu = str(row.get("地区") or "").strip()
        if not diqu:
            continue
        out[diqu] = int(out.get(diqu) or 0) + 1
    return out


def count_wcnr_xyr_by_diqu_and_ajlx(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str]
) -> Dict[str, Dict[str, int]]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("xyrxx_ay_mc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(ajxx_join_ajxx_cbdw_bh_dm, 6) AS diqu,
              ajxx_join_ajxx_ajlx AS ajlx,
              COUNT(DISTINCT xyrxx_sfzh) AS cnt
            FROM {schema}.zq_zfba_wcnr_xyr
            WHERE ajxx_join_ajxx_lasj BETWEEN %s AND %s
              AND ajxx_join_ajxx_ajlx IN ('行政','刑事')
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu, ajlx")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, Dict[str, int]] = {"行政": {}, "刑事": {}}
    for diqu, ajlx, cnt in rows:
        if not diqu or not ajlx:
            continue
        ajlx_s = str(ajlx)
        if ajlx_s in out:
            out[ajlx_s][str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_xzcfjds_by_diqu(
    conn,
    *,
    start_time: str,
    end_time: str,
    patterns: Sequence[str],
    za_types: Sequence[str],
    not_execute_only: bool,
) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
    cfzl_sql, cfzl_params = _cfzl_regex_condition(za_types)

    where_not_exec = sql.SQL("")
    if not_execute_only:
        where_not_exec = sql.SQL(" AND xz.xzcfjds_zxqk_text ~ '不送'")

    q = (
        sql.SQL(
            """
            SELECT
              LEFT(xz.xzcfjds_cbdw_bh_dm, 6) AS diqu,
              COUNT(DISTINCT xz.xzcfjds_id) AS cnt
            FROM {schema}.zq_zfba_xzcfjds xz
            INNER JOIN {schema}.zq_zfba_wcnr_xyr xyr
              ON xyr.ajxx_ajbhs = xz.ajxx_ajbh
             AND xyr.xyrxx_rybh = xz.xzcfjds_rybh
            LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
              ON aj.ajxx_ajbh = xz.ajxx_ajbh
            WHERE xz.xzcfjds_spsj BETWEEN %s AND %s
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + cfzl_sql
        + where_not_exec
        + sql.SQL(" GROUP BY diqu")
    )
    params = [start_time, end_time] + pat_params + cfzl_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_jlz_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("jlz_ay_mc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(jlz.jlz_cbdw_bh_dm, 6) AS diqu,
              COUNT(DISTINCT jlz.jlz_id) AS cnt
            FROM {schema}.zq_zfba_jlz jlz
            INNER JOIN {schema}.zq_zfba_wcnr_xyr xyr
              ON xyr.ajxx_ajbhs = jlz.ajxx_ajbh
             AND xyr.xyrxx_rybh = jlz.jlz_rybh
            WHERE jlz.jlz_pzsj BETWEEN %s AND %s
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_xjs_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(aj.ajxx_cbdw_bh_dm, 6) AS diqu,
              COUNT(DISTINCT xjs.xjs_id) AS cnt
            FROM {schema}.zq_zfba_xjs xjs
            LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
              ON aj.ajxx_ajbh = xjs.xjs_ajbh
            WHERE xjs.xjs_tfsj BETWEEN %s AND %s
              AND xjs.xjs_wszt = '审批通过'
              AND xjs.xjs_isdel = '0'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_jtjyzdtzs_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(jt.jqjhjyzljsjtjyzdtzs_cbdw_bh_dm, 6) AS diqu,
              COUNT(DISTINCT jt.jqjhjyzljsjtjyzdtzs_id) AS cnt
            FROM {schema}.zq_zfba_jtjyzdtzs jt
            LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
              ON aj.ajxx_ajbh = jt.jqjhjyzljsjtjyzdtzs_ajbh
            WHERE jt.jqjhjyzljsjtjyzdtzs_tfsj BETWEEN %s AND %s
              AND jt.jqjhjyzljsjtjyzdtzs_wszt = '审批通过'
              AND jt.jqjhjyzljsjtjyzdtzs_isdel_dm = '0'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_fuhe_songxiao_by_diqu(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str], za_types: Sequence[str]
) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL('zzwx."xyrxx_ay_mc"'))
    cfzl_sql, cfzl_params = _cfzl_regex_condition(za_types)

    q = (
        sql.SQL(
            """
            WITH
            jishu AS (
                SELECT DISTINCT zzwx."xyrxx_sfzh"
                FROM {schema}.zq_zfba_wcnr_xyr zzwx
                WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                  AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(
            """
            ),
            lianxu_wf AS (
                SELECT
                    "xyrxx_sfzh",
                    COUNT(*) AS wf_count,
                    COUNT(DISTINCT "xyrxx_ay_mc") AS distinct_ay_count,
                    CASE
                        WHEN COUNT(*) = 2 AND COUNT(DISTINCT "xyrxx_ay_mc") = 1 THEN '是'
                        WHEN COUNT(*) > 2 THEN '是'
                        ELSE '否'
                    END AS is_lianxu_wf
                FROM {schema}.zq_zfba_wcnr_xyr
                WHERE "xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
                  AND "ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                GROUP BY "xyrxx_sfzh"
            )
            SELECT
                LEFT(main."ajxx_join_ajxx_cbdw_bh_dm", 6) AS diqu,
                COUNT(DISTINCT main."xyrxx_sfzh") AS cnt
            FROM {schema}.zq_zfba_wcnr_xyr main
            LEFT JOIN lianxu_wf lw ON lw."xyrxx_sfzh" = main."xyrxx_sfzh"
            WHERE main."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
              AND main."xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
              AND COALESCE(lw.is_lianxu_wf, '否') = '是'
              AND EXISTS (
                  SELECT 1
                  FROM {schema}.zq_zfba_xzcfjds xz
                  WHERE xz."ajxx_ajbh" = main."ajxx_ajbhs"
                    AND xz."xzcfjds_rybh" = main."xyrxx_rybh"
                    AND xz."xzcfjds_spsj" BETWEEN %s AND %s
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + cfzl_sql
        + sql.SQL(
            """
                    AND CAST(xz."xzcfjds_tj_jlts" AS INTEGER) > 4
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {schema}.zq_zfba_jlz jlz
                  WHERE jlz."ajxx_ajbh" = main."ajxx_ajbhs"
                    AND jlz."jlz_rybh" = main."xyrxx_rybh"
                    AND jlz."jlz_pzsj" BETWEEN %s AND %s
              )
              AND 1=1
            GROUP BY diqu
            """
        ).format(schema=sql.Identifier(SCHEMA))
    )

    params: List[Any] = [
        start_time,
        end_time,
        *pat_params,
        start_time,
        end_time,
        start_time,
        end_time,
        start_time,
        end_time,
        *cfzl_params,
        start_time,
        end_time,
    ]
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_wcnr_shr_ajxx_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    """案件数(被侵害)：查询 zq_zfba_wcnr_shr_ajxx 表"""
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
    q = (
        sql.SQL(
            """
            SELECT
              LEFT(ajxx_cbdw_bh_dm, 6) AS diqu,
              COUNT(1) AS cnt
            FROM {schema}.zq_zfba_wcnr_shr_ajxx
            WHERE ajxx_lasj BETWEEN %s AND %s
              AND ajxx_ajzt NOT IN ('已撤销','已合并')
              AND ajxx_cbdw_mc !~ '交通'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def fetch_wcnr_shr_ajxx_base_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    patterns: Sequence[str],
    diqu: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """案件数(被侵害)基础明细，供场所案件(被侵害)二次分类复用。"""
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
    params: List[Any] = [start_time, end_time] + list(pat_params)
    where_diqu = sql.SQL("")
    if diqu:
        where_diqu = sql.SQL(" AND LEFT(ajxx_cbdw_bh_dm, 6) = %s")
        params.append(str(diqu).strip())

    q = (
        sql.SQL(
            """
            SELECT
                ajxx_ajbh AS "案件编号",
                ajxx_jqbh AS "警情编号",
                ajxx_ajmc AS "案件名称",
                ajxx_ajlx AS "案件类型",
                ajxx_aymc AS "案由名称",
                ajxx_lasj AS "立案时间",
                ajxx_cbdw_mc AS "承办单位",
                LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                ajxx_jyaq 简要案情, ajxx_aymc 案由, ajxx_ajzt 案件状态,ajxx_fadd 发案地点,ajxx_fasj 发案时间
            FROM {schema}.zq_zfba_wcnr_shr_ajxx
            WHERE ajxx_lasj BETWEEN %s AND %s
              AND ajxx_ajzt NOT IN ('已撤销','已合并')
              AND ajxx_cbdw_mc !~ '交通'
              AND 1=1
            """
        ).format(schema=sql.Identifier(SCHEMA))
        + pat_sql
        + where_diqu
        + sql.SQL(" ORDER BY ajxx_lasj DESC")
    )
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def count_changsuo_shr_ajxx_by_diqu(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str]
) -> Dict[str, int]:
    """场所案件(被侵害)：在案件数(被侵害)基础明细上做地址分类，保留“重点管控行业”后按地区计数。"""
    rows = fetch_wcnr_shr_ajxx_base_rows(
        conn, start_time=start_time, end_time=end_time, patterns=patterns, diqu=None
    )
    if not rows:
        return {}

    _append_addr_predictions(rows, addr_col="发案地点")
    out: Dict[str, int] = {}
    for row in rows:
        if str(row.get("分类结果") or "").strip() != "重点管控行业":
            continue
        diqu = str(row.get("地区") or "").strip()
        if not diqu:
            continue
        out[diqu] = int(out.get(diqu) or 0) + 1
    return out


def count_songxiao_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("sfz.jzyy"))
    sfz_table = sql.SQL(".").join([sql.Identifier(SCHEMA), sql.Identifier("zq_wcnr_sfzxx")])
    q = (
        sql.SQL(
            """
            SELECT
              CASE
                WHEN sfz.ssbm ILIKE '%%云城%%' THEN '445302'
                WHEN sfz.ssbm ILIKE '%%云安%%' THEN '445303'
                WHEN sfz.ssbm ILIKE '%%罗定%%' THEN '445381'
                WHEN sfz.ssbm ILIKE '%%新兴%%' THEN '445321'
                WHEN sfz.ssbm ILIKE '%%郁南%%' THEN '445322'
                WHEN sfz.ssbm ILIKE '%%市局%%' THEN '445300'
                ELSE NULL
              END AS diqu,
              COUNT(DISTINCT sfz.bh) AS cnt
            FROM {sfz_table} sfz
            WHERE sfz.rx_time::timestamp BETWEEN %s AND %s
              AND 1=1
            """
        ).format(sfz_table=sfz_table)
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def fetch_wcnr_jzqk_rows(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> List[Dict[str, Any]]:
    """复用“矫治情况统计”数据源，返回同口径明细行。"""
    return jzqk_tongji_dao.fetch_jzqk_data(
        conn, start_time=start_time, end_time=end_time, leixing_list=leixing_list
    )


def fetch_detail_rows(
    conn,
    *,
    metric: str,
    diqu: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    za_types: Sequence[str],
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], bool]:
    metric = (metric or "").strip()
    metric_alias = {
        "训诫书": "矫治文书",
    }
    metric = metric_alias.get(metric, metric)
    diqu = (diqu or "").strip()
    is_all = diqu in ("", "__ALL__", "全市")
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    za_types = [str(x).strip() for x in (za_types or []) if str(x).strip()]
    patterns = fetch_ay_patterns(conn, leixing_list=leixing_list)
    jzqk_metrics = {
        "矫治文书",
        "矫治文书(行政)",
        "矫治文书(刑事)",
        "加强监督教育",
        "加强监督教育(行政)",
        "加强监督教育(刑事)",
        "符合送校",
        "送校",
    }
    if leixing_list and not patterns and metric not in {"警情", "转案数"} and metric not in jzqk_metrics:
        return [], False

    limit_n = int(limit) if limit and int(limit) > 0 else 0
    truncated = False

    if metric == "警情":
        subclass_codes = fetch_newcharasubclass_list(conn, leixing_list=leixing_list) if leixing_list else None
        if leixing_list and not subclass_codes:
            return [], False

        rows = fetch_minor_case_rows(start_time=start_time, end_time=end_time)
        filtered_rows = filter_minor_case_rows_by_subclasses(rows, subclass_codes=subclass_codes)
        return build_minor_case_detail_rows(
            filtered_rows,
            diqu="__ALL__" if is_all else diqu,
            limit=limit_n,
        )

    # 复用“矫治情况统计”口径：嫌疑人/矫治文书/加强监督教育（行政+刑事拆分）/符合送校/送校
    if metric in jzqk_metrics:
        rows = fetch_wcnr_jzqk_rows(
            conn, start_time=start_time, end_time=end_time, leixing_list=leixing_list
        )
        filtered: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            diqu_code = str(item.get("地区") or "").strip()
            if not is_all and diqu_code != diqu:
                continue

            ajlx = str(item.get("案件类型") or "").strip()
            if metric in ("矫治文书", "矫治文书(行政)", "矫治文书(刑事)") and str(item.get("是否开具矫治文书") or "").strip() != "是":
                continue
            if metric == "矫治文书(行政)" and ajlx != "行政":
                continue
            if metric == "矫治文书(刑事)" and ajlx != "刑事":
                continue
            if metric in ("加强监督教育", "加强监督教育(行政)", "加强监督教育(刑事)") and str(item.get("是否开具家庭教育指导书") or "").strip() != "是":
                continue
            if metric == "加强监督教育(行政)" and ajlx != "行政":
                continue
            if metric == "加强监督教育(刑事)" and ajlx != "刑事":
                continue
            if metric == "符合送校" and str(item.get("是否符合送生") or "").strip() != "是":
                continue
            if metric == "送校" and str(item.get("是否送校") or "").strip() != "是":
                continue
            filtered.append(item)

        filtered.sort(key=lambda r: str(r.get("立案时间") or ""), reverse=True)
        if limit_n and len(filtered) > limit_n:
            truncated = True
            filtered = filtered[:limit_n]
        return filtered, truncated

    def _exec(cur, q: sql.SQL, params: List[Any]) -> Tuple[List[Dict[str, Any]], bool]:
        nonlocal truncated
        if limit_n:
            q = q + sql.SQL(" LIMIT %s")
            params = list(params) + [limit_n + 1]
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        if limit_n and len(rows) > limit_n:
            truncated = True
            rows = rows[:limit_n]
        return rows, truncated

    with conn.cursor() as cur:
        if metric == "转案数":
            params1: List[Any] = [start_time, end_time]
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(' AND LEFT(jq."cmdid", 6) = %s')
                params1.append(diqu)
            where_type = sql.SQL("")
            if leixing_list:
                where_type = sql.SQL(
                    '''
  AND jq.newcharasubclass IN (
      SELECT unnest(ctc.newcharasubclass_list) 
      FROM ywdata.case_type_config ctc 
      WHERE ctc.leixing = ANY(%s)
  )'''
                )
                params1.append(leixing_list)

            q = (
                sql.SQL(
                    """
                    SELECT DISTINCT ON (jq."caseno")
                      jq."calltime" AS "报警时间",
                      jq."caseno" AS "警情编号",
                      jq."dutydeptname" AS "管辖单位",
                      jq."cmdname" AS "分局",
                      jq."occuraddress" AS "警情地址",
                      jq."casecontents" AS "报警内容",
                      jq."replies" AS "处警情况",
                      jq."casemark" AS "警情标注",
                      jq."lngofcriterion" AS "经度",
                      jq."latofcriterion" AS "纬度",
                      LEFT(jq."cmdid", 6) AS "地区",
                      zza."ajxx_ajbh" AS "案件编号",
                      zza."ajxx_lasj" AS "立案时间",
                      zza."ajxx_cbdw_mc" AS "办案单位"
                    FROM ywdata."zq_kshddpt_dsjfx_jq" jq
                    LEFT JOIN "ywdata"."zq_zfba_ajxx" zza ON jq."caseno" = zza."ajxx_jqbh"
                    WHERE jq."calltime" BETWEEN %s AND %s
                      AND (jq."casemark" ~ '未成年' OR jq."casemarkok" ~ '未成年')
                      AND LEFT(jq."newcharasubclass", 2) IN ('01','02')
                      AND NULLIF(BTRIM(zza."ajxx_ajbh"), '') IS NOT NULL
                      AND 1=1
                    """
                )
                + where_type
                + where_diqu
                + sql.SQL(' ORDER BY jq."caseno", jq."calltime" DESC, zza."ajxx_lasj" DESC NULLS LAST, zza."ajxx_ajbh" DESC')
            )
            return _exec(cur, q, params1)

        if metric in ("行政", "刑事"):
            params2: List[Any] = [start_time, end_time, metric]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
            params2 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(ajxx_cbdw_bh_dm, 6) = %s")
                params2.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      ajxx_ajbh AS "案件编号",
                      ajxx_jqbh AS "警情编号",
                      ajxx_ajmc AS "案件名称",
                      ajxx_ajlx AS "案件类型",
                      ajxx_aymc AS "案由名称",
                      ajxx_lasj AS "立案时间",
                      ajxx_cbdw_mc AS "承办单位",
                      LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                      ajxx_jyaq 简要案情, ajxx_aymc 案由, ajxx_ajzt 案件状态,ajxx_fadd 发案地点,ajxx_fasj 发案时间
                    FROM {schema}.zq_zfba_wcnr_ajxx
                    WHERE ajxx_lasj BETWEEN %s AND %s
                      AND ajxx_ajlx = %s
                      AND ajxx_ajzt NOT IN ('已撤销','已合并')
                      AND ajxx_cbdw_mc !~ '交通'
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY ajxx_lasj DESC")
            )
            return _exec(cur, q, params2)

        if metric == "场所案件":
            base_rows = fetch_wcnr_ajxx_changsuo_base_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
                diqu=None if is_all else diqu,
            )
            _append_addr_predictions(base_rows, addr_col="发案地点")
            filtered = [r for r in base_rows if str(r.get("分类结果") or "").strip() == "重点管控行业"]
            if limit_n and len(filtered) > limit_n:
                truncated = True
                filtered = filtered[:limit_n]
            return filtered, truncated

        if metric in ("治安处罚", "治安处罚(不执行)"):
            params4: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params4 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(xz.xzcfjds_cbdw_bh_dm,6)=%s")
                params4.append(diqu)
            where_cfzl, cfzl_params = _cfzl_regex_condition(za_types)
            params4 += cfzl_params
            where_not_exec = sql.SQL("")
            if metric == "治安处罚(不执行)":
                where_not_exec = sql.SQL(" AND xz.xzcfjds_zxqk_text ~ '不送'")

            q = (
                sql.SQL(
                    """
                    SELECT
                      xz.xzcfjds_id AS "处罚决定书ID",
                      xz.ajxx_ajbh AS "案件编号",
                      xz.xzcfjds_rybh AS "人员编号",
                      aj.ajxx_aymc AS "案由",
                      xyr.xyrxx_sfzh AS "证件号码",
                      xyr.xyrxx_xm AS "姓名",
                      xz.xzcfjds_cfzl AS "处罚种类",
                      xz.xzcfjds_spsj AS "审批时间",
                      xz.xzcfjds_zxqk_text AS "执行情况",
                      xz.xzcfjds_cbdw_mc AS "承办单位",
                      LEFT(xz.xzcfjds_cbdw_bh_dm, 6) AS "地区"
                    FROM {schema}.zq_zfba_xzcfjds xz
                    INNER JOIN {schema}.zq_zfba_wcnr_xyr xyr
                      ON xyr.ajxx_ajbhs = xz.ajxx_ajbh
                     AND xyr.xyrxx_rybh = xz.xzcfjds_rybh
                    LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
                      ON aj.ajxx_ajbh = xz.ajxx_ajbh
                    WHERE xz.xzcfjds_spsj BETWEEN %s AND %s
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + where_cfzl
                + where_not_exec
                + where_diqu
                + sql.SQL(" ORDER BY xz.xzcfjds_spsj DESC")
            )
            return _exec(cur, q, params4)

        if metric == "刑拘":
            params5: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("jlz.jlz_ay_mc"))
            params5 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(jlz.jlz_cbdw_bh_dm,6)=%s")
                params5.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      jlz.jlz_id AS "拘留证ID",
                      jlz.ajxx_ajbh AS "案件编号",
                      jlz.jlz_rybh AS "人员编号",
                      xyr.xyrxx_sfzh AS "证件号码",
                      xyr.xyrxx_xm AS "姓名",
                      jlz.jlz_ay_mc AS "案由",
                      jlz.jlz_pzsj AS "批准时间",
                      jlz.jlz_cbdw_mc AS "承办单位",
                      LEFT(jlz.jlz_cbdw_bh_dm, 6) AS "地区"
                    FROM {schema}.zq_zfba_jlz jlz
                    INNER JOIN {schema}.zq_zfba_wcnr_xyr xyr
                      ON xyr.ajxx_ajbhs = jlz.ajxx_ajbh
                     AND xyr.xyrxx_rybh = jlz.jlz_rybh
                    WHERE jlz.jlz_pzsj BETWEEN %s AND %s
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY jlz.jlz_pzsj DESC")
            )
            return _exec(cur, q, params5)

        if metric == "训诫书":
            params6: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params6 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(aj.ajxx_cbdw_bh_dm,6)=%s")
                params6.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      xjs.xjs_id AS "训诫书ID",
                      xjs.xjs_ajbh AS "案件编号",
                      xjs.xjs_rybh AS "人员编号",
                      xjs.xjs_xjyy AS "训诫原因",
                      xjs.xjs_tfsj AS "提访时间",
                      xjs.xjs_cbdw_mc AS "承办单位",
                      LEFT(aj.ajxx_cbdw_bh_dm, 6) AS "地区"
                    FROM {schema}.zq_zfba_xjs xjs
                    LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
                      ON aj.ajxx_ajbh = xjs.xjs_ajbh
                    WHERE xjs.xjs_tfsj BETWEEN %s AND %s
                      AND xjs.xjs_wszt='审批通过'
                      AND xjs.xjs_isdel='0'
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY xjs.xjs_tfsj DESC")
            )
            return _exec(cur, q, params6)

        if metric == "加强监督教育":
            params7: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params7 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(jt.jqjhjyzljsjtjyzdtzs_cbdw_bh_dm,6)=%s")
                params7.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      jt.jqjhjyzljsjtjyzdtzs_id AS "通知书ID",
                      jt.jqjhjyzljsjtjyzdtzs_ajbh AS "案件编号",
                      jt.jqjhjyzljsjtjyzdtzs_rybh AS "人员编号",
                      jt.jqjhjyzljsjtjyzdtzs_ajmc AS "案件名称",
                      jt.jqjhjyzljsjtjyzdtzs_tfsj AS "提访时间",
                      jt.jqjhjyzljsjtjyzdtzs_cbdw_mc AS "承办单位",
                      LEFT(jt.jqjhjyzljsjtjyzdtzs_cbdw_bh_dm, 6) AS "地区"
                    FROM {schema}.zq_zfba_jtjyzdtzs jt
                    LEFT JOIN {schema}.zq_zfba_wcnr_ajxx aj
                      ON aj.ajxx_ajbh = jt.jqjhjyzljsjtjyzdtzs_ajbh
                    WHERE jt.jqjhjyzljsjtjyzdtzs_tfsj BETWEEN %s AND %s
                      AND jt.jqjhjyzljsjtjyzdtzs_wszt='审批通过'
                      AND jt.jqjhjyzljsjtjyzdtzs_isdel_dm='0'
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY jt.jqjhjyzljsjtjyzdtzs_tfsj DESC")
            )
            return _exec(cur, q, params7)

        if metric == "符合送校":
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL('zzwx."xyrxx_ay_mc"'))
            where_cfzl, cfzl_params = _cfzl_regex_condition(za_types)
            where_diqu = sql.SQL("")
            params8: List[Any] = [start_time, end_time]  # jishu
            params8 += list(pat_params)  # type patterns (optional)
            params8 += [start_time, end_time]  # lianxu_wf
            params8 += [start_time, end_time]  # main
            params8 += [start_time, end_time]  # xz
            params8 += list(cfzl_params)  # za cfzl regex (optional)
            params8 += [start_time, end_time]  # jlz
            if not is_all:
                where_diqu = sql.SQL(' AND LEFT(main."ajxx_join_ajxx_cbdw_bh_dm", 6) = %s')
                params8.append(diqu)
            q = (
                sql.SQL(
                    """
                    WITH
                    jishu AS (
                        SELECT DISTINCT zzwx."xyrxx_sfzh"
                        FROM {schema}.zq_zfba_wcnr_xyr zzwx
                        WHERE zzwx."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                          AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_pat
                + sql.SQL(
                    """
                    ),
                    lianxu_wf AS (
                        SELECT
                            "xyrxx_sfzh",
                            COUNT(*) as wf_count,
                            COUNT(DISTINCT "xyrxx_ay_mc") as distinct_ay_count,
                            CASE
                                WHEN COUNT(*) = 2 AND COUNT(DISTINCT "xyrxx_ay_mc") = 1 THEN '是'
                                WHEN COUNT(*) > 2 THEN '是'
                                ELSE '否'
                            END as is_lianxu_wf
                        FROM {schema}.zq_zfba_wcnr_xyr
                        WHERE "xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
                          AND "ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                        GROUP BY "xyrxx_sfzh"
                    )
                    SELECT
                        main."ajxx_ajbhs" AS "案件编号",
                        main."xyrxx_xm" AS "姓名",
                        main."xyrxx_sfzh" AS "证件号码",
                        main."ajxx_join_ajxx_ajlx" AS "案件类型",
                        main."ajxx_join_ajxx_ajmc" AS "案件名称",
                        main."ajxx_join_ajxx_cbdw_bh" AS "办案单位",
                        main."ajxx_join_ajxx_cbdw_bh_dm" AS "办案单位代码",
                        main."ajxx_join_ajxx_lasj" AS "立案时间",
                        main."xyrxx_ay_mc" AS "案由",
                        main."xyrxx_hjdxz" AS "户籍地",
                        main."xyrxx_rybh" AS "人员编号",
                        main."xyrxx_xzdxz" AS "现住地",
                        '是' AS "治拘5日及以上",
                        COALESCE(lw.is_lianxu_wf, '否') AS "连续2次同样违法/3次及以上违法",
                        '否' AS "刑事刑拘",
                        LEFT(main."ajxx_join_ajxx_cbdw_bh_dm", 6) AS "地区"
                    FROM {schema}.zq_zfba_wcnr_xyr main
                    LEFT JOIN lianxu_wf lw ON lw."xyrxx_sfzh" = main."xyrxx_sfzh"
                    WHERE main."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
                      AND main."xyrxx_sfzh" IN (SELECT "xyrxx_sfzh" FROM jishu)
                      AND COALESCE(lw.is_lianxu_wf, '否') = '是'
                      AND EXISTS (
                          SELECT 1
                          FROM {schema}.zq_zfba_xzcfjds xz
                          WHERE xz."ajxx_ajbh" = main."ajxx_ajbhs"
                            AND xz."xzcfjds_rybh" = main."xyrxx_rybh"
                            AND xz."xzcfjds_spsj" BETWEEN %s AND %s
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_cfzl
                + sql.SQL(
                    """
                            AND CAST(xz."xzcfjds_tj_jlts" AS INTEGER) > 4
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {schema}.zq_zfba_jlz jlz
                          WHERE jlz."ajxx_ajbh" = main."ajxx_ajbhs"
                            AND jlz."jlz_rybh" = main."xyrxx_rybh"
                            AND jlz."jlz_pzsj" BETWEEN %s AND %s
                      )
                      AND 1=1
                    """
                ).format(schema=sql.Identifier(SCHEMA))
                + where_diqu
                + sql.SQL(' ORDER BY main."ajxx_ajbhs", main."xyrxx_sfzh"')
            )
            return _exec(cur, q, params8)

        if metric == "送校":
            params9: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("sfz.jzyy"))
            params9 += pat_params
            where_region = sql.SQL("")
            if not is_all:
                where_region = sql.SQL(
                    """
                    AND (
                      (sfz.ssbm ILIKE '%%云城%%' AND %s='445302') OR
                      (sfz.ssbm ILIKE '%%云安%%' AND %s='445303') OR
                      (sfz.ssbm ILIKE '%%罗定%%' AND %s='445381') OR
                      (sfz.ssbm ILIKE '%%新兴%%' AND %s='445321') OR
                      (sfz.ssbm ILIKE '%%郁南%%' AND %s='445322') OR
                      (sfz.ssbm ILIKE '%%市局%%' AND %s='445300')
                    )
                    """
                )
                params9 += [diqu, diqu, diqu, diqu, diqu, diqu]
            sfz_table = sql.SQL(".").join([sql.Identifier(SCHEMA), sql.Identifier("zq_wcnr_sfzxx")])
            q = (
                sql.SQL(
                    """
                    SELECT
                      sfz.bh AS "编号",
                      sfz.xm AS "姓名",
                      sfz.sfzhm AS "证件号码",
                      sfz.ssbm AS "所属部门",
                      sfz.jzyy AS "就读原因",
                      sfz.rx_time AS "入学时间",
                      sfz.jz_time AS "矫治月数",
                      sfz.lx_time AS "离校时间"
                    FROM {sfz_table} sfz
                    WHERE sfz.rx_time::timestamp BETWEEN %s AND %s
                      AND 1=1
                    """
                ).format(sfz_table=sfz_table)
                + where_pat
                + where_region
                + sql.SQL(" ORDER BY sfz.rx_time DESC")
            )
            return _exec(cur, q, params9)

        # 新增：案件数(被侵害)明细
        if metric == "案件数(被侵害)":
            rows = fetch_wcnr_shr_ajxx_base_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
                diqu=None if is_all else diqu,
            )
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "场所案件(被侵害)":
            rows = fetch_wcnr_shr_ajxx_base_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
                diqu=None if is_all else diqu,
            )
            _append_addr_predictions(rows, addr_col="发案地点")
            filtered = [r for r in rows if str(r.get("分类结果") or "").strip() == "重点管控行业"]
            if limit_n and len(filtered) > limit_n:
                truncated = True
                filtered = filtered[:limit_n]
            return filtered, truncated

    raise ValueError(f"未知 metric: {metric}")
