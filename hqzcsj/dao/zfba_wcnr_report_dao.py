from __future__ import annotations

from typing import Dict, Sequence, Tuple

from psycopg2 import sql


def _normalize_leixing_list(leixing_list: Sequence[str]) -> list[str]:
    return [str(x).strip() for x in (leixing_list or []) if str(x).strip()]


def count_graduate_reoffense_by_region(
    conn,
    *,
    start_date: str,
    end_date: str,
    leixing_list: Sequence[str],
    only_xingshi: bool,
) -> Tuple[Dict[str, int], int]:
    leixing_list = _normalize_leixing_list(leixing_list)

    if leixing_list:
        type_condition = sql.SQL('AND zws."jzyy" = ANY(%s)')
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    if only_xingshi:
        xingshi_condition = sql.SQL('AND zzx."ajxx_join_ajxx_ajlx" = %s')
        xingshi_params = ["刑事"]
        under18_condition = sql.SQL(
            """
            AND zzx."xyrxx_sfzh" ~ '^[0-9]{17}[0-9Xx]$'
            AND age(
                  zzx."ajxx_join_ajxx_lasj"::date,
                  to_date(substr(zzx."xyrxx_sfzh", 7, 8), 'YYYYMMDD')
                ) < interval '18 years'
            """
        )
    else:
        xingshi_condition = sql.SQL("")
        xingshi_params = []
        under18_condition = sql.SQL("")

    query = sql.SQL(
        """
        WITH graduates AS (
            SELECT DISTINCT ON (zws."sfzhm")
                zws."sfzhm" AS sfzhm,
                COALESCE(NULLIF(LEFT(COALESCE(zws."hjdq", ''), 6), ''), 'UNKNOWN') AS region_code,
                zws."lx_time" AS graduate_time
            FROM "ywdata"."zq_zfba_wcnr_sfzxx" zws
            WHERE zws."lx_time" BETWEEN %s AND %s
              AND zws."jz_time" < 6
              AND NULLIF(TRIM(COALESCE(zws."sfzhm", '')), '') IS NOT NULL
              {type_condition}
            ORDER BY zws."sfzhm", zws."lx_time" DESC
        )
        SELECT g.region_code, COUNT(*)::INT AS cnt
        FROM graduates g
        WHERE EXISTS (
            SELECT 1
            FROM "ywdata"."zq_zfba_wcnr_xyr" zzx
            WHERE zzx."xyrxx_sfzh" = g.sfzhm
              AND zzx."ajxx_join_ajxx_lasj" > g.graduate_time
              {xingshi_condition}
              {under18_condition}
        )
        GROUP BY g.region_code
        """
    ).format(
        type_condition=type_condition,
        xingshi_condition=xingshi_condition,
        under18_condition=under18_condition,
    )

    params = [start_date, end_date] + type_params + xingshi_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    out = {str(row[0]): int(row[1]) for row in rows if row and row[0]}
    return out, sum(out.values())
