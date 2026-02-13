from __future__ import annotations

from typing import Any, Dict, List, Sequence


def _as_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def query_9lbq_rows(conn, id_cards: Sequence[str]) -> List[Dict[str, Any]]:
    sql = """
    WITH temp_jztssjhc AS (
        SELECT UNNEST(%s::text[]) AS zjhm
    ),
    input_ids AS (
        SELECT DISTINCT BTRIM(zjhm) AS "证件号码"
        FROM temp_jztssjhc
        WHERE NULLIF(BTRIM(zjhm), '') IS NOT NULL
    ),
    -- 只保留与输入身份证集合匹配的数据，避免每次全表/全视图扫描
    ls AS MATERIALIZED (
        SELECT DISTINCT b.zjhm
        FROM ywdata.b_per_qslswcnr b
        INNER JOIN input_ids a ON a."证件号码" = b.zjhm
    ),
    ybbl AS MATERIALIZED (
        SELECT DISTINCT c.zjhm
        FROM ywdata.b_per_yfsjyjblxwwcnr c
        INNER JOIN input_ids a ON a."证件号码" = c.zjhm
    ),
    sxcx AS MATERIALIZED (
        SELECT DISTINCT d.zjhm
        FROM ywdata.b_per_qscxwcnr d
        INNER JOIN input_ids a ON a."证件号码" = d.zjhm
    ),
    kj AS MATERIALIZED (
        SELECT DISTINCT e.zjhm
        FROM ywdata.b_per_qskjwcnr e
        INNER JOIN input_ids a ON a."证件号码" = e.zjhm
    ),
    yzjz AS MATERIALIZED (
        SELECT DISTINCT f.zjhm
        FROM ywdata.b_per_qsyzjszawcnr f
        INNER JOIN input_ids a ON a."证件号码" = f.zjhm
    ),
    zljscj AS MATERIALIZED (
        SELECT DISTINCT g.zjhm
        FROM ywdata.b_per_qszljscjwcnr g
        INNER JOIN input_ids a ON a."证件号码" = g.zjhm
    ),
    ld AS MATERIALIZED (
        SELECT DISTINCT h.sfzjh
        FROM ywdata.v_per_qsldwcnrxs h
        WHERE h.sfzjh = ANY(%s::text[])
    ),
    yzbl AS (
        SELECT DISTINCT zjhm
        FROM stdata.b_zdry_ryxx
        WHERE "deleteflag" = 0
          AND sflg = 1
          AND zjhm = ANY(%s::text[])
    ),
    j AS (
        SELECT DISTINCT ON (aa.gmsfhm)
            aa.gmsfhm,
            aa.zxsj,
            aa."hjdz_qhnxxdz",
            CASE
                WHEN SUBSTRING(aa.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
                WHEN SUBSTRING(aa.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
                WHEN SUBSTRING(aa.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
                WHEN SUBSTRING(aa.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
                WHEN SUBSTRING(aa.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
                ELSE aa.sjgsdwdm
            END AS ssfj,
            SUBSTRING(aa.sjgsdwdm, 1, 6) || '000000' AS ssfjdm,
            bb.sspcs,
            aa.sjgsdwdm AS sspcsdm
        FROM ywdata.t_dsfb_czrk_jbxx aa
        LEFT JOIN ywdata.b_zzjgdm bb
            ON aa.sjgsdwdm = bb.sspcsdm
        WHERE aa.gmsfhm = ANY(%s::text[])
        ORDER BY aa.gmsfhm, aa.zxsj DESC NULLS LAST
    )
    SELECT
        a."证件号码",
        TO_CHAR(j.zxsj, 'YYYY-MM-DD HH24:MI:SS') AS "户籍注销时间",
        '云浮市公安局' AS "所属市局",
        '445300000000' AS "所属市局代码",
        j.ssfj AS "所属分局",
        j.ssfjdm AS "所属分局代码",
        j.hjdz_qhnxxdz AS "户籍地址",
        j.sspcs AS "所属派出所",
        j.sspcsdm AS "所属派出所代码",
        CONCAT_WS(
            '、',
            CASE WHEN ls.zjhm IS NOT NULL THEN '留守' END,
            CASE WHEN ybbl.zjhm IS NOT NULL THEN '一般不良' END,
            CASE WHEN sxcx.zjhm IS NOT NULL THEN '失学辍学' END,
            CASE WHEN kj.zjhm IS NOT NULL THEN '困境' END,
            CASE WHEN yzjz.zjhm IS NOT NULL THEN '严重精障' END,
            CASE WHEN zljscj.zjhm IS NOT NULL THEN '智力精神残疾' END,
            CASE WHEN ld.sfzjh IS NOT NULL THEN '流动' END,
            CASE WHEN yzbl.zjhm IS NOT NULL THEN '严重不良' END
        ) AS "标签集合"
    FROM input_ids a
    LEFT JOIN ls ON a."证件号码" = ls.zjhm
    LEFT JOIN ybbl ON a."证件号码" = ybbl.zjhm
    LEFT JOIN sxcx ON a."证件号码" = sxcx.zjhm
    LEFT JOIN kj ON a."证件号码" = kj.zjhm
    LEFT JOIN yzjz ON a."证件号码" = yzjz.zjhm
    LEFT JOIN zljscj ON a."证件号码" = zljscj.zjhm
    LEFT JOIN ld ON a."证件号码" = ld.sfzjh
    LEFT JOIN yzbl ON a."证件号码" = yzbl.zjhm
    LEFT JOIN j ON a."证件号码" = j.gmsfhm
    ORDER BY a."证件号码"
    """

    with conn.cursor() as cur:
        params = (list(id_cards), list(id_cards), list(id_cards), list(id_cards))
        cur.execute(sql, params)
        return _as_dict_rows(cur)
