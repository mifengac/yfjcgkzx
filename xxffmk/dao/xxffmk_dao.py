from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


SCHOOL_SUFFIX_PATTERN = r"(?:幼儿园|小学|中学|高中|高级中学|完全中学|九年一贯制学校|十二年一贯制学校|学校|大学|学院|职中|职高|技工学校|技校|实验学校|职业技术学校|中等职业学校)"
SCHOOL_NAME_PATTERN = rf"([A-Za-z0-9一-龥·（）()\-]{{2,40}}{SCHOOL_SUFFIX_PATTERN})"
REFRESHABLE_MATERIALIZED_VIEWS = [
    '"ywdata"."mv_xxffmk_school_dim"',
    '"ywdata"."mv_xxffmk_student_school_rel"',
    '"ywdata"."mv_xxffmk_dim5_night_day"',
]


def _fetch_all_dicts(cursor: Any) -> List[Dict[str, Any]]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def build_school_dim_cte(alias: str = "school_dim") -> str:
    return f"""
{alias} AS (
    WITH school_sources AS (
        SELECT
            s."xxbsm",
            s."xxmc",
            s."zgjyxzbmmc",
            'zzxj' AS source_type,
            1 AS source_priority,
            MAX(COALESCE(s."gkrksj", s."bzkrksj", s."cd_time", s."add_time")) AS latest_time
        FROM "ywdata"."sh_yf_zzxj_xx" s
        WHERE NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
        GROUP BY s."xxbsm", s."xxmc", s."zgjyxzbmmc"

        UNION ALL

        SELECT
            s."xxbsm",
            s."xxmc",
            s."zgjyxzbmmc",
            'zxxj' AS source_type,
            2 AS source_priority,
            MAX(COALESCE(s."bzkrksj", s."cd_time", s."add_time")) AS latest_time
        FROM "ywdata"."sh_gd_zxxxsxj_xx" s
        WHERE NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
        GROUP BY s."xxbsm", s."xxmc", s."zgjyxzbmmc"
    ),
    ranked AS (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY ss."xxbsm", ss."xxmc", ss."zgjyxzbmmc"
                ORDER BY ss.source_priority, ss.latest_time DESC NULLS LAST
            ) AS rn
        FROM school_sources ss
    )
    SELECT
        r."xxbsm",
        r."xxmc",
        r."zgjyxzbmmc",
        r.source_type,
        UPPER(REGEXP_REPLACE(COALESCE(r."xxmc", ''), '[[:space:][:punct:]]', '', 'g')) AS normalized_xxmc
    FROM ranked r
    WHERE r.rn = 1
)
""".strip()


def build_student_school_rel_cte(alias: str = "student_school_rel") -> str:
    return f"""
{alias} AS (
    WITH student_sources AS (
        SELECT
            s."sfzjh",
            s."xxbsm",
            s."xxmc",
            s."zgjyxzbmmc",
            'zzxj' AS source_type,
            s."njmc",
            s."bjmc",
            1 AS source_priority,
            COALESCE(s."gkrksj", s."bzkrksj", s."cd_time", s."add_time") AS latest_time,
            s."id"
        FROM "ywdata"."sh_yf_zzxj_xx" s
        WHERE NULLIF(BTRIM(COALESCE(s."sfzjh", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL

        UNION ALL

        SELECT
            s."sfzjh",
            s."xxbsm",
            s."xxmc",
            s."zgjyxzbmmc",
            'zxxj' AS source_type,
            s."njmc",
            s."bjmc",
            2 AS source_priority,
            COALESCE(s."bzkrksj", s."cd_time", s."add_time") AS latest_time,
            s."id"
        FROM "ywdata"."sh_gd_zxxxsxj_xx" s
        WHERE NULLIF(BTRIM(COALESCE(s."sfzjh", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxbsm", '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(s."xxmc", '')), '') IS NOT NULL
    ),
    ranked AS (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY ss."sfzjh"
                ORDER BY ss.source_priority, ss.latest_time DESC NULLS LAST, ss."id" DESC
            ) AS rn
        FROM student_sources ss
    )
    SELECT
        r."sfzjh",
        r."xxbsm",
        r."xxmc",
        r."zgjyxzbmmc",
        r.source_type,
        r."njmc",
        r."bjmc"
    FROM ranked r
    WHERE r.rn = 1
)
""".strip()


def build_student_school_rel_ref(alias: str = "student_school_rel") -> str:
    return f"""
{alias} AS (
    SELECT
        r."sfzjh",
        r."xxbsm",
        r."xxmc",
        r."zgjyxzbmmc",
        r.source_type,
        r."njmc",
        r."bjmc"
FROM "ywdata"."mv_xxffmk_student_school_rel" r
)
""".strip()


def build_fetch_school_records_query() -> str:
    return f"""
WITH
school_dim AS (
    SELECT
        s."xxbsm",
        s."xxmc",
        s."zgjyxzbmmc",
        s.source_type,
        s.normalized_xxmc
FROM "ywdata"."mv_xxffmk_school_dim" s
)
SELECT
    s."xxbsm",
    s."xxmc",
    s."zgjyxzbmmc",
    s.source_type,
    s.normalized_xxmc
FROM school_dim s
ORDER BY s."xxmc", s."xxbsm"
""".strip()


def build_dimension1_query() -> str:
    return """
SELECT
    BTRIM(COALESCE(z."yxx", '')) AS raw_school_name,
    COUNT(*) AS raw_count
FROM "ywdata"."zq_zfba_wcnr_sfzxx" z
WHERE z."rx_time" >= %s
  AND z."rx_time" <= %s
  AND NULLIF(BTRIM(COALESCE(z."yxx", '')), '') IS NOT NULL
GROUP BY BTRIM(COALESCE(z."yxx", ''))
ORDER BY raw_count DESC, raw_school_name
""".strip()


def build_dimension2_query() -> str:
    return f"""
WITH source_rows AS (
    SELECT
        j."caseno",
        j."calltime",
        j."occuraddress",
        j."casecontents",
        j."replies",
        COALESCE(
            (REGEXP_MATCH(COALESCE(j."occuraddress", ''), '{SCHOOL_NAME_PATTERN}') )[1],
            (REGEXP_MATCH(COALESCE(j."casecontents", ''), '{SCHOOL_NAME_PATTERN}') )[1],
            (REGEXP_MATCH(COALESCE(j."replies", ''), '{SCHOOL_NAME_PATTERN}') )[1]
        ) AS extracted_school_name
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" j
    WHERE j."calltime" >= %s
      AND j."calltime" <= %s
      AND j."newcharasubclass" IN ('01','02','04','05','06','08','09')
      AND (
            COALESCE(j."occuraddress", '') ~ '{SCHOOL_SUFFIX_PATTERN}'
         OR COALESCE(j."casecontents", '') ~ '{SCHOOL_SUFFIX_PATTERN}'
         OR COALESCE(j."replies", '') ~ '{SCHOOL_SUFFIX_PATTERN}'
      )
)
SELECT
    s."caseno",
    s."calltime",
    s."occuraddress",
    s."casecontents",
    s."replies",
    s.extracted_school_name
FROM source_rows s
WHERE NULLIF(BTRIM(COALESCE(s.extracted_school_name, '')), '') IS NOT NULL
ORDER BY s."calltime" DESC, s."caseno"
""".strip()
def build_dimension3_query() -> str:
    return f"""
WITH
minor_people AS (
    SELECT
        x."ajxx_join_ajxx_ajbh" AS ajbh,
        x."xyrxx_sfzh" AS sfzjh,
        x."xyrxx_xm" AS xm,
        x."xyrxx_lrsj"
    FROM "ywdata"."zq_zfba_xyrxx" x
    WHERE x."xyrxx_lrsj" >= %s
      AND x."xyrxx_lrsj" <= %s
      AND COALESCE(x."ajxx_join_ajxx_isdel_dm", '0') = '0'
      AND COALESCE(x."xyrxx_sfzh", '') ~ '^[0-9]{{17}}[0-9Xx]$'
      AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^[0-9]{{8}}$'
      AND AGE(COALESCE(x."ajxx_join_ajxx_lasj", x."xyrxx_lrsj")::date, TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD')) < INTERVAL '18 years'
),
gang_cases AS (
    SELECT
        m.ajbh
    FROM minor_people m
    GROUP BY m.ajbh
    HAVING COUNT(*) >= 3
)
SELECT
    m.ajbh,
    m.sfzjh,
    m.xm,
    m."xyrxx_lrsj",
    r."xxbsm",
    r."xxmc",
    r."zgjyxzbmmc"
FROM minor_people m
JOIN gang_cases g
  ON g.ajbh = m.ajbh
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = m.sfzjh
ORDER BY m."xyrxx_lrsj" DESC, m.ajbh, m.sfzjh
""".strip()


def build_dimension4_query() -> str:
    return f"""
SELECT
    q."zjhm",
    q."xm",
    q."xq",
    q."jxqk",
    q."jtzz",
    q."hjdz",
    q."jhrdh",
    q."xb",
    r."xxbsm",
    r."xxmc",
    r."zgjyxzbmmc",
    r."njmc",
    r."bjmc"
FROM "ywdata"."b_per_qscxwcnr" q
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = q."zjhm"
WHERE NULLIF(BTRIM(COALESCE(q."zjhm", '')), '') IS NOT NULL
ORDER BY q."xm", q."zjhm"
""".strip()


def build_dimension5_query() -> str:
    return f"""
WITH
params AS (
    SELECT
        %s::date AS begin_date,
        %s::date AS end_date
),
night_days AS (
    SELECT
        n.sfzjh,
        MAX(n.xm) AS xm,
        COUNT(*) AS night_days
    FROM "ywdata"."mv_xxffmk_dim5_night_day" n
    JOIN params p
      ON n.shot_date >= p.begin_date
     AND n.shot_date <= p.end_date
    GROUP BY n.sfzjh
    HAVING COUNT(*) >= 10
)
SELECT
    q.sfzjh,
    q.xm,
    q.night_days,
    r."xxbsm",
    r."xxmc",
    r."zgjyxzbmmc",
    r."njmc",
    r."bjmc"
FROM night_days q
JOIN "ywdata"."mv_xxffmk_student_school_rel" r
  ON r."sfzjh" = q.sfzjh
ORDER BY q.night_days DESC, q.sfzjh
""".strip()


def build_refresh_materialized_views_statements() -> List[str]:
    return [f'REFRESH MATERIALIZED VIEW {view}' for view in REFRESHABLE_MATERIALIZED_VIEWS]


def build_dimension5_night_day_mv_query() -> str:
    return """
SELECT
    t."id_number" AS sfzjh,
    MAX(t."name") AS xm,
    TO_DATE(SUBSTRING(t."shot_time", 1, 8), 'YYYYMMDD') AS shot_date
FROM "ywdata"."t_spy_ryrlgj_xx" t
WHERE COALESCE(t."id_number", '') ~ '^[0-9]{17}[0-9Xx]$'
  AND SUBSTRING(t."shot_time", 9, 6) >= '000000'
  AND SUBSTRING(t."shot_time", 9, 6) <= '050000'
GROUP BY t."id_number", TO_DATE(SUBSTRING(t."shot_time", 1, 8), 'YYYYMMDD')
""".strip()


def refresh_materialized_views(conn: Any) -> List[str]:
    refreshed: List[str] = []
    with conn.cursor() as cursor:
        for statement in build_refresh_materialized_views_statements():
            cursor.execute(statement)
            refreshed.append(statement)
    return refreshed


def fetch_school_records(conn: Any) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_fetch_school_records_query())
        return _fetch_all_dicts(cursor)


def fetch_dimension1_rows(conn: Any, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_dimension1_query(), (start_time, end_time))
        return _fetch_all_dicts(cursor)


def fetch_dimension2_rows(conn: Any, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_dimension2_query(), (start_time, end_time))
        return _fetch_all_dicts(cursor)


def fetch_dimension3_rows(conn: Any, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_dimension3_query(), (start_time, end_time))
        return _fetch_all_dicts(cursor)


def fetch_dimension4_rows(conn: Any) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_dimension4_query())
        return _fetch_all_dicts(cursor)


def fetch_dimension5_rows(conn: Any, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(build_dimension5_query(), (start_time, end_time))
        return _fetch_all_dicts(cursor)


def paginate_rows(rows: Sequence[Dict[str, Any]], page: int, page_size: int) -> Dict[str, Any]:
    safe_page = max(1, page)
    safe_page_size = max(1, min(page_size, 500))
    start = (safe_page - 1) * safe_page_size
    end = start + safe_page_size
    return {
        "rows": list(rows[start:end]),
        "total": len(rows),
        "page": safe_page,
        "page_size": safe_page_size,
    }


def summarize_unmatched(rows: Iterable[Dict[str, Any]], *, name_key: str, count_key: str = "count", limit: int = 10) -> List[Dict[str, Any]]:
    sorted_rows = sorted(
        rows,
        key=lambda item: (-int(item.get(count_key, 0) or 0), str(item.get(name_key, "") or "")),
    )
    return sorted_rows[:limit]

