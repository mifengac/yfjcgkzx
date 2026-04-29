from __future__ import annotations

from typing import Any, Dict, List, Sequence

from hqzcsj.dao import pcsjqajtj_dao


def _normalize_list(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def fetch_leixing_list(conn) -> List[Dict[str, str]]:
    return pcsjqajtj_dao.fetch_leixing_list(conn)


def fetch_fenju_list(conn) -> List[Dict[str, str]]:
    return pcsjqajtj_dao.fetch_fenju_list(conn)


def fetch_rows(
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
        WITH params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[] AS leixing,
                %s::text[] AS ssfjdm_list
        )
        SELECT DISTINCT
            t.ajbh AS "案件编号",
            t.ajmc AS "案件名称",
            t.spsj AS "审批时间",
            t.wsmc AS "文书名称",
            t.xgry_xm AS "姓名",
            x.xyrxx_sfzh AS "身份证号",
            x.ajxx_join_ajxx_ajlx AS "案件类型",
            CASE
                WHEN NULLIF(BTRIM(COALESCE(x.ajxx_join_ajxx_cbdw_bh_dm, '')), '') IS NULL THEN ''
                ELSE LEFT(x.ajxx_join_ajxx_cbdw_bh_dm, 6) || '000000'
            END AS "地区",
            x.ajxx_join_ajxx_cbdw_bh AS "承办单位",
            x.ajxx_join_ajxx_ay AS "案由",
            x.xyrxx_hjdxz AS "户籍地址",
            x.xyrxx_xzdxz AS "现住地"
        FROM ywdata.zq_zfba_tqzmjy t
        CROSS JOIN params p
        LEFT JOIN ywdata.zq_zfba_xyrxx x
               ON t.ajbh = x.ajxx_join_ajxx_ajbh
              AND COALESCE(BTRIM(t.xgry_xm), '') = COALESCE(BTRIM(x.xyrxx_xm), '')
        WHERE t.spsj >= p.start_time
          AND t.spsj <= p.end_time
          AND (
                COALESCE(cardinality(p.ssfjdm_list), 0) = 0
                OR (LEFT(COALESCE(x.ajxx_join_ajxx_cbdw_bh_dm, ''), 6) || '000000') = ANY(p.ssfjdm_list)
          )
          AND (
                COALESCE(cardinality(p.leixing), 0) = 0
                OR EXISTS (
                    SELECT 1
                    FROM ywdata.case_type_config ctc
                    WHERE ctc.leixing = ANY(p.leixing)
                      AND COALESCE(x.ajxx_join_ajxx_ay, '') SIMILAR TO ctc.ay_pattern
                )
          )
        ORDER BY t.spsj DESC NULLS LAST, t.ajbh, t.xgry_xm
    """
    with conn.cursor() as cur:
        cur.execute(sql_text, [start_time, end_time, leixing, ssfjdm])
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
