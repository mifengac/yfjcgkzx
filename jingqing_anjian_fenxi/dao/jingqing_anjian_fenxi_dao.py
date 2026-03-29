from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple


TIMESTAMP_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$"


def _normalize_list(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _table_has_data_col(conn, *, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = 'data'
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _text_expr(alias: str, col: str, *, has_data: bool) -> str:
    if has_data:
        return f'COALESCE({alias}."{col}", {alias}."data"->>\'{col}\')'
    return f'{alias}."{col}"'


def _ts_expr(alias: str, col: str, *, has_data: bool) -> str:
    if has_data:
        return (
            f'COALESCE({alias}."{col}", '
            f'CASE WHEN COALESCE({alias}."data"->>\'{col}\', \'\') ~ \'{TIMESTAMP_PATTERN}\' '
            f'THEN ({alias}."data"->>\'{col}\')::timestamp END)'
        )
    return f'{alias}."{col}"'


def _jq_calltime_ts_expr(alias: str = "jq") -> str:
    return (
        f'CASE WHEN COALESCE({alias}."calltime", \'\') ~ \'{TIMESTAMP_PATTERN}\' '
        f'THEN {alias}."calltime"::timestamp END'
    )


def _group_mode_sql(group_mode: str, aj_group_field_expr: str) -> Dict[str, str]:
    mode = str(group_mode or "county").strip().lower()
    if mode == "station":
        return {
            "map_cte": """
                group_map AS (
                    SELECT
                        ranked.group_code,
                        ranked.group_name,
                        ranked.fenju_code,
                        ranked.fenju_name,
                        ranked.ssfjdm_raw
                    FROM (
                        SELECT
                            LEFT(COALESCE(d."sspcsdm", ''), 8) || '0000' AS group_code,
                            COALESCE(d."sspcs", '') AS group_name,
                            LEFT(COALESCE(d."ssfjdm", ''), 6) || '000000' AS fenju_code,
                            COALESCE(d."ssfj", '') AS fenju_name,
                            COALESCE(d."ssfjdm", '') AS ssfjdm_raw,
                            ROW_NUMBER() OVER (
                                PARTITION BY LEFT(COALESCE(d."sspcsdm", ''), 8)
                                ORDER BY d."rksj" DESC NULLS LAST
                            ) AS rn
                        FROM "stdata"."b_dic_zzjgdm" d
                        WHERE NULLIF(BTRIM(COALESCE(d."sspcsdm", '')), '') IS NOT NULL
                    ) ranked
                    WHERE ranked.rn = 1
                )
            """,
            "jq_group_expr": 'LEFT(jq."dutydeptno", 8) || \'0000\'',
            "aj_group_expr": f"LEFT({aj_group_field_expr}, 8) || '0000'",
        }
    return {
        "map_cte": """
            group_map AS (
                SELECT
                    ranked.group_code,
                    ranked.group_name,
                    ranked.fenju_code,
                    ranked.fenju_name,
                    ranked.ssfjdm_raw
                FROM (
                    SELECT
                        LEFT(COALESCE(d."ssfjdm", ''), 6) || '000000' AS group_code,
                        COALESCE(d."ssfj", '') AS group_name,
                        LEFT(COALESCE(d."ssfjdm", ''), 6) || '000000' AS fenju_code,
                        COALESCE(d."ssfj", '') AS fenju_name,
                        COALESCE(d."ssfjdm", '') AS ssfjdm_raw,
                        ROW_NUMBER() OVER (
                            PARTITION BY LEFT(COALESCE(d."ssfjdm", ''), 6)
                            ORDER BY d."rksj" DESC NULLS LAST
                        ) AS rn
                    FROM "stdata"."b_dic_zzjgdm" d
                    WHERE NULLIF(BTRIM(COALESCE(d."ssfjdm", '')), '') IS NOT NULL
                ) ranked
                WHERE ranked.rn = 1
            )
        """,
        "jq_group_expr": 'LEFT(jq."cmdid", 6) || \'000000\'',
        "aj_group_expr": f"LEFT({aj_group_field_expr}, 6) || '000000'",
    }


def _fenju_filter_sql() -> str:
    return """
        AND (
            COALESCE(cardinality(p.ssfjdm_list), 0) = 0
            OR EXISTS (
                SELECT 1
                FROM unnest(p.ssfjdm_list) sel(code)
                WHERE LEFT(COALESCE(gm.ssfjdm_raw, ''), 6) = LEFT(COALESCE(sel.code, ''), 6)
            )
        )
    """


def _jq_type_filter_sql() -> str:
    return """
        AND (
            COALESCE(cardinality(p.leixing_list), 0) = 0
            OR EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(p.leixing_list)
                  AND jq."neworicharasubclass" = ANY(ctc."newcharasubclass_list")
            )
        )
    """


def _aj_type_filter_sql(field_expr: str) -> str:
    return f"""
        AND (
            COALESCE(cardinality(p.leixing_list), 0) = 0
            OR EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(p.leixing_list)
                  AND COALESCE({field_expr}, '') SIMILAR TO ctc."ay_pattern"
            )
        )
    """


def _query_rows(conn, sql_text: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql_text, list(params))
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _append_limit(sql_text: str, params: List[Any], limit: Optional[int]) -> Tuple[str, List[Any]]:
    limit_n = int(limit or 0)
    if limit_n <= 0:
        return sql_text, params
    return f"{sql_text}\nLIMIT %s", params + [limit_n + 1]


def fetch_leixing_list(conn) -> List[Dict[str, str]]:
    rows = _query_rows(
        conn,
        """
        SELECT DISTINCT "leixing" AS value
        FROM "ywdata"."case_type_config"
        WHERE NULLIF(BTRIM(COALESCE("leixing", '')), '') IS NOT NULL
        ORDER BY "leixing"
        """,
        [],
    )
    return [{"label": row["value"], "value": row["value"]} for row in rows]


def fetch_fenju_list(conn) -> List[Dict[str, str]]:
    rows = _query_rows(
        conn,
        """
        SELECT DISTINCT "ssfj" AS label, "ssfjdm" AS value
        FROM "stdata"."b_dic_zzjgdm"
        WHERE NULLIF(BTRIM(COALESCE("ssfjdm", '')), '') IS NOT NULL
        ORDER BY "ssfjdm"
        """,
        [],
    )
    out: List[Dict[str, str]] = []
    for row in rows:
        value = str(row.get("value") or "").strip()
        if not value:
            continue
        label = str(row.get("label") or "").strip() or value
        out.append({"label": label, "value": value})
    return out


def fetch_group_rows(
    conn,
    *,
    group_mode: str,
    ssfjdm_list: Sequence[str],
) -> List[Dict[str, Any]]:
    group_sql = _group_mode_sql(group_mode, "''")
    sql_text = f"""
        WITH
        params AS (
            SELECT %s::text[] AS ssfjdm_list
        ),
        {group_sql["map_cte"]}
        SELECT
            gm.fenju_name AS fenju_name,
            gm.fenju_code AS fenju_code,
            gm.group_name AS group_name,
            gm.group_code AS group_code
        FROM group_map gm
        CROSS JOIN params p
        WHERE 1 = 1
        {_fenju_filter_sql()}
        ORDER BY gm.fenju_name, gm.group_name
    """
    return _query_rows(conn, sql_text, [_normalize_list(ssfjdm_list)])


def _build_summary_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        group_code = str(row.get("group_code") or "").strip()
        if not group_code:
            continue
        out[group_code] = row
    return out


def fetch_timely_filing_summary(
    conn,
    *,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    has_aj = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_ajxx")
    aj_jqbh = _text_expr("aj", "ajxx_jqbh", has_data=has_aj)
    aj_lasj = _ts_expr("aj", "ajxx_lasj", has_data=has_aj)
    aj_cbdw_dm = _text_expr("aj", "ajxx_cbdw_bh_dm", has_data=has_aj)
    group_sql = _group_mode_sql(group_mode, aj_cbdw_dm)
    jq_calltime = _jq_calltime_ts_expr("jq")
    sql_text = f"""
        WITH
        params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[] AS leixing_list,
                %s::text[] AS ssfjdm_list
        ),
        {group_sql["map_cte"]},
        source_rows AS (
            SELECT
                gm.fenju_name,
                gm.fenju_code,
                gm.group_name,
                gm.group_code,
                EXTRACT(EPOCH FROM (({aj_lasj}) - ({jq_calltime}))) / 3600.0 AS diff_hours
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
              ON jq."caseno" = {aj_jqbh}
            JOIN group_map gm
              ON gm.group_code = {group_sql["jq_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {jq_calltime} IS NOT NULL
              {_jq_type_filter_sql()}
              {_fenju_filter_sql()}
        )
        SELECT
            fenju_name,
            fenju_code,
            group_name,
            group_code,
            SUM(diff_hours) AS sum_hours,
            COUNT(*) AS row_count
        FROM source_rows
        WHERE diff_hours >= 0
        GROUP BY fenju_name, fenju_code, group_name, group_code
        ORDER BY fenju_name, group_name
    """
    rows = _query_rows(
        conn,
        sql_text,
        [start_time, end_time, _normalize_list(leixing_list), _normalize_list(ssfjdm_list)],
    )
    return _build_summary_map(rows)


def fetch_timely_arrest_summary(
    conn,
    *,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    has_aj = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_ajxx")
    has_xyr = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_xyrxx")
    aj_ajbh = _text_expr("aj", "ajxx_ajbh", has_data=has_aj)
    aj_jqbh = _text_expr("aj", "ajxx_jqbh", has_data=has_aj)
    aj_lasj = _ts_expr("aj", "ajxx_lasj", has_data=has_aj)
    aj_aymc = _text_expr("aj", "ajxx_aymc", has_data=has_aj)
    aj_cbdw_dm = _text_expr("aj", "ajxx_cbdw_bh_dm", has_data=has_aj)
    xyr_ajbh = _text_expr("x", "ajxx_join_ajxx_ajbh", has_data=has_xyr)
    xyr_lrsj = _ts_expr("x", "xyrxx_lrsj", has_data=has_xyr)
    group_sql = _group_mode_sql(group_mode, aj_cbdw_dm)
    jq_calltime = _jq_calltime_ts_expr("jq")
    sql_text = f"""
        WITH
        params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[] AS leixing_list,
                %s::text[] AS ssfjdm_list
        ),
        {group_sql["map_cte"]},
        first_suspect AS (
            SELECT ajbh, xyrxx_lrsj
            FROM (
                SELECT
                    {xyr_ajbh} AS ajbh,
                    {xyr_lrsj} AS xyrxx_lrsj,
                    ROW_NUMBER() OVER (
                        PARTITION BY {xyr_ajbh}
                        ORDER BY {xyr_lrsj} ASC NULLS LAST
                    ) AS rn
                FROM "ywdata"."zq_zfba_xyrxx" x
                WHERE {xyr_ajbh} IS NOT NULL
                  AND {xyr_lrsj} IS NOT NULL
            ) ranked
            WHERE rn = 1
        ),
        source_rows AS (
            SELECT
                gm.fenju_name,
                gm.fenju_code,
                gm.group_name,
                gm.group_code,
                EXTRACT(EPOCH FROM (fs.xyrxx_lrsj - ({jq_calltime}))) / 3600.0 AS diff_hours
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN first_suspect fs
              ON fs.ajbh = {aj_ajbh}
            JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
              ON jq."caseno" = {aj_jqbh}
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND LEFT(COALESCE({aj_jqbh}, ''), 1) = '4'
              AND {jq_calltime} IS NOT NULL
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}

            UNION ALL

            SELECT
                gm.fenju_name,
                gm.fenju_code,
                gm.group_name,
                gm.group_code,
                EXTRACT(EPOCH FROM (fs.xyrxx_lrsj - ({aj_lasj}))) / 3600.0 AS diff_hours
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN first_suspect fs
              ON fs.ajbh = {aj_ajbh}
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND LEFT(COALESCE({aj_jqbh}, ''), 1) <> '4'
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}
        )
        SELECT
            fenju_name,
            fenju_code,
            group_name,
            group_code,
            SUM(diff_hours) AS sum_hours,
            COUNT(*) AS row_count
        FROM source_rows
        WHERE diff_hours >= 0
        GROUP BY fenju_name, fenju_code, group_name, group_code
        ORDER BY fenju_name, group_name
    """
    rows = _query_rows(
        conn,
        sql_text,
        [start_time, end_time, _normalize_list(leixing_list), _normalize_list(ssfjdm_list)],
    )
    return _build_summary_map(rows)


def fetch_timely_solve_summary(
    conn,
    *,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    has_aj = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_ajxx")
    aj_lasj = _ts_expr("aj", "ajxx_lasj", has_data=has_aj)
    aj_pxjarq = _ts_expr("aj", "ajxx_pxjarq", has_data=has_aj)
    aj_aymc = _text_expr("aj", "ajxx_aymc", has_data=has_aj)
    aj_ajlx = _text_expr("aj", "ajxx_ajlx", has_data=has_aj)
    aj_pxjabs = _text_expr("aj", "ajxx_pxjabs_dm", has_data=has_aj)
    aj_cbdw_dm = _text_expr("aj", "ajxx_cbdw_bh_dm", has_data=has_aj)
    group_sql = _group_mode_sql(group_mode, aj_cbdw_dm)
    sql_text = f"""
        WITH
        params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[] AS leixing_list,
                %s::text[] AS ssfjdm_list
        ),
        {group_sql["map_cte"]},
        source_rows AS (
            SELECT
                gm.fenju_name,
                gm.fenju_code,
                gm.group_name,
                gm.group_code,
                EXTRACT(EPOCH FROM (({aj_pxjarq}) - ({aj_lasj}))) / 3600.0 AS diff_hours
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {aj_pxjarq} IS NOT NULL
              AND COALESCE({aj_ajlx}, '') = '刑事'
              AND COALESCE({aj_pxjabs}, '') = '1'
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}
        )
        SELECT
            fenju_name,
            fenju_code,
            group_name,
            group_code,
            SUM(diff_hours) AS sum_hours,
            COUNT(*) AS row_count
        FROM source_rows
        WHERE diff_hours >= 0
        GROUP BY fenju_name, fenju_code, group_name, group_code
        ORDER BY fenju_name, group_name
    """
    rows = _query_rows(
        conn,
        sql_text,
        [start_time, end_time, _normalize_list(leixing_list), _normalize_list(ssfjdm_list)],
    )
    return _build_summary_map(rows)


def fetch_timely_close_summary(
    conn,
    *,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    has_aj = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_ajxx")
    aj_lasj = _ts_expr("aj", "ajxx_lasj", has_data=has_aj)
    aj_cfsj = _ts_expr("aj", "ajxx_cfsj", has_data=has_aj)
    aj_aymc = _text_expr("aj", "ajxx_aymc", has_data=has_aj)
    aj_ajlx = _text_expr("aj", "ajxx_ajlx", has_data=has_aj)
    aj_ajzt_dm = _text_expr("aj", "ajxx_ajzt_dm", has_data=has_aj)
    aj_cbdw_dm = _text_expr("aj", "ajxx_cbdw_bh_dm", has_data=has_aj)
    group_sql = _group_mode_sql(group_mode, aj_cbdw_dm)
    sql_text = f"""
        WITH
        params AS (
            SELECT
                %s::timestamp AS start_time,
                %s::timestamp AS end_time,
                %s::text[] AS leixing_list,
                %s::text[] AS ssfjdm_list
        ),
        {group_sql["map_cte"]},
        source_rows AS (
            SELECT
                gm.fenju_name,
                gm.fenju_code,
                gm.group_name,
                gm.group_code,
                EXTRACT(EPOCH FROM (({aj_cfsj}) - ({aj_lasj}))) / 3600.0 AS diff_hours
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {aj_cfsj} IS NOT NULL
              AND COALESCE({aj_ajlx}, '') = '行政'
              AND COALESCE({aj_ajzt_dm}, '') NOT IN ('0101', '0104', '0112', '0114')
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}
        )
        SELECT
            fenju_name,
            fenju_code,
            group_name,
            group_code,
            SUM(diff_hours) AS sum_hours,
            COUNT(*) AS row_count
        FROM source_rows
        WHERE diff_hours >= 0
        GROUP BY fenju_name, fenju_code, group_name, group_code
        ORDER BY fenju_name, group_name
    """
    rows = _query_rows(
        conn,
        sql_text,
        [start_time, end_time, _normalize_list(leixing_list), _normalize_list(ssfjdm_list)],
    )
    return _build_summary_map(rows)


def fetch_detail_rows(
    conn,
    *,
    metric: str,
    group_code: str,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], bool]:
    metric_key = str(metric or "").strip()
    selected_group = str(group_code or "__ALL__").strip()
    is_all = selected_group in ("", "__ALL__", "全市")

    has_aj = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_ajxx")
    has_xyr = _table_has_data_col(conn, schema="ywdata", table="zq_zfba_xyrxx")

    aj_ajbh = _text_expr("aj", "ajxx_ajbh", has_data=has_aj)
    aj_jqbh = _text_expr("aj", "ajxx_jqbh", has_data=has_aj)
    aj_lasj = _ts_expr("aj", "ajxx_lasj", has_data=has_aj)
    aj_aymc = _text_expr("aj", "ajxx_aymc", has_data=has_aj)
    aj_ajlx = _text_expr("aj", "ajxx_ajlx", has_data=has_aj)
    aj_pxjabs = _text_expr("aj", "ajxx_pxjabs_dm", has_data=has_aj)
    aj_pxjarq = _ts_expr("aj", "ajxx_pxjarq", has_data=has_aj)
    aj_ajzt_dm = _text_expr("aj", "ajxx_ajzt_dm", has_data=has_aj)
    aj_cfsj = _ts_expr("aj", "ajxx_cfsj", has_data=has_aj)
    aj_cbdw_dm = _text_expr("aj", "ajxx_cbdw_bh_dm", has_data=has_aj)
    aj_cbdw_mc = _text_expr("aj", "ajxx_cbdw_mc", has_data=has_aj)
    xyr_ajbh = _text_expr("x", "ajxx_join_ajxx_ajbh", has_data=has_xyr)
    xyr_lrsj = _ts_expr("x", "xyrxx_lrsj", has_data=has_xyr)
    xyr_name = _text_expr("x", "xyrxx_xm", has_data=has_xyr)
    jq_calltime = _jq_calltime_ts_expr("jq")
    group_sql = _group_mode_sql(group_mode, aj_cbdw_dm)
    base_params: List[Any] = [
        start_time,
        end_time,
        _normalize_list(leixing_list),
        _normalize_list(ssfjdm_list),
    ]
    group_filter_sql = "" if is_all else "AND gm.group_code = %s"

    def _run(sql_text: str, params: List[Any]) -> Tuple[List[Dict[str, Any]], bool]:
        final_sql, final_params = _append_limit(sql_text, params, limit)
        rows = _query_rows(conn, final_sql, final_params)
        limit_n = int(limit or 0)
        truncated = limit_n > 0 and len(rows) > limit_n
        if truncated:
            rows = rows[:limit_n]
        return rows, truncated

    if metric_key == "timely_filing":
        params = list(base_params)
        if not is_all:
            params.append(selected_group)
        sql_text = f"""
            WITH
            params AS (
                SELECT
                    %s::timestamp AS start_time,
                    %s::timestamp AS end_time,
                    %s::text[] AS leixing_list,
                    %s::text[] AS ssfjdm_list
            ),
            {group_sql["map_cte"]}
            SELECT
                gm.fenju_name AS "分局",
                gm.group_name AS "当前分组名称",
                jq."caseno" AS "警情编号",
                {aj_ajbh} AS "案件编号",
                {aj_aymc} AS "案由",
                {jq_calltime} AS "接警时间",
                {aj_lasj} AS "立案时间",
                ROUND((EXTRACT(EPOCH FROM (({aj_lasj}) - ({jq_calltime}))) / 3600.0)::numeric, 2) AS "时差(小时)",
                {aj_cbdw_mc} AS "办案单位"
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
              ON jq."caseno" = {aj_jqbh}
            JOIN group_map gm
              ON gm.group_code = {group_sql["jq_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {jq_calltime} IS NOT NULL
              {_jq_type_filter_sql()}
              {_fenju_filter_sql()}
              {group_filter_sql}
              AND EXTRACT(EPOCH FROM (({aj_lasj}) - ({jq_calltime}))) / 3600.0 >= 0
            ORDER BY {aj_lasj} DESC, jq."caseno"
        """
        return _run(sql_text, params)

    if metric_key == "timely_arrest":
        params = list(base_params)
        if not is_all:
            params.append(selected_group)
        sql_text = f"""
            WITH
            params AS (
                SELECT
                    %s::timestamp AS start_time,
                    %s::timestamp AS end_time,
                    %s::text[] AS leixing_list,
                    %s::text[] AS ssfjdm_list
            ),
            {group_sql["map_cte"]},
            first_suspect AS (
                SELECT ajbh, xyrxx_lrsj, xyrxx_xm
                FROM (
                    SELECT
                        {xyr_ajbh} AS ajbh,
                        {xyr_lrsj} AS xyrxx_lrsj,
                        {xyr_name} AS xyrxx_xm,
                        ROW_NUMBER() OVER (
                            PARTITION BY {xyr_ajbh}
                            ORDER BY {xyr_lrsj} ASC NULLS LAST
                        ) AS rn
                    FROM "ywdata"."zq_zfba_xyrxx" x
                    WHERE {xyr_ajbh} IS NOT NULL
                      AND {xyr_lrsj} IS NOT NULL
                ) ranked
                WHERE rn = 1
            ),
            source_rows AS (
                SELECT
                    gm.fenju_name AS "分局",
                    gm.group_name AS "当前分组名称",
                    {aj_ajbh} AS "案件编号",
                    {aj_jqbh} AS "警情编号",
                    {aj_aymc} AS "案由",
                    '有关联警情' AS "研判抓人类型",
                    fs.xyrxx_xm AS "首嫌疑人姓名",
                    {jq_calltime} AS "接警时间",
                    {aj_lasj} AS "立案时间",
                    fs.xyrxx_lrsj AS "首嫌疑人录入时间",
                    ROUND((EXTRACT(EPOCH FROM (fs.xyrxx_lrsj - ({jq_calltime}))) / 3600.0)::numeric, 2) AS "时差(小时)",
                    {aj_cbdw_mc} AS "办案单位",
                    gm.group_code
                FROM "ywdata"."zq_zfba_ajxx" aj
                CROSS JOIN params p
                JOIN first_suspect fs
                  ON fs.ajbh = {aj_ajbh}
                JOIN "ywdata"."zq_kshddpt_dsjfx_jq" jq
                  ON jq."caseno" = {aj_jqbh}
                JOIN group_map gm
                  ON gm.group_code = {group_sql["aj_group_expr"]}
                WHERE {aj_lasj} >= p.start_time
                  AND {aj_lasj} <= p.end_time
                  AND {aj_lasj} IS NOT NULL
                  AND LEFT(COALESCE({aj_jqbh}, ''), 1) = '4'
                  AND {jq_calltime} IS NOT NULL
                  {_aj_type_filter_sql(aj_aymc)}
                  {_fenju_filter_sql()}

                UNION ALL

                SELECT
                    gm.fenju_name AS "分局",
                    gm.group_name AS "当前分组名称",
                    {aj_ajbh} AS "案件编号",
                    {aj_jqbh} AS "警情编号",
                    {aj_aymc} AS "案由",
                    '无关联警情' AS "研判抓人类型",
                    fs.xyrxx_xm AS "首嫌疑人姓名",
                    NULL::timestamp AS "接警时间",
                    {aj_lasj} AS "立案时间",
                    fs.xyrxx_lrsj AS "首嫌疑人录入时间",
                    ROUND((EXTRACT(EPOCH FROM (fs.xyrxx_lrsj - ({aj_lasj}))) / 3600.0)::numeric, 2) AS "时差(小时)",
                    {aj_cbdw_mc} AS "办案单位",
                    gm.group_code
                FROM "ywdata"."zq_zfba_ajxx" aj
                CROSS JOIN params p
                JOIN first_suspect fs
                  ON fs.ajbh = {aj_ajbh}
                JOIN group_map gm
                  ON gm.group_code = {group_sql["aj_group_expr"]}
                WHERE {aj_lasj} >= p.start_time
                  AND {aj_lasj} <= p.end_time
                  AND {aj_lasj} IS NOT NULL
                  AND LEFT(COALESCE({aj_jqbh}, ''), 1) <> '4'
                  {_aj_type_filter_sql(aj_aymc)}
                  {_fenju_filter_sql()}
            )
            SELECT
                "分局",
                "当前分组名称",
                "案件编号",
                "警情编号",
                "案由",
                "研判抓人类型",
                "首嫌疑人姓名",
                "接警时间",
                "立案时间",
                "首嫌疑人录入时间",
                "时差(小时)",
                "办案单位"
            FROM source_rows
            WHERE "时差(小时)" >= 0
              {group_filter_sql.replace('gm.group_code', 'group_code')}
            ORDER BY "首嫌疑人录入时间" DESC, "案件编号"
        """
        return _run(sql_text, params)

    if metric_key == "timely_solve":
        params = list(base_params)
        if not is_all:
            params.append(selected_group)
        sql_text = f"""
            WITH
            params AS (
                SELECT
                    %s::timestamp AS start_time,
                    %s::timestamp AS end_time,
                    %s::text[] AS leixing_list,
                    %s::text[] AS ssfjdm_list
            ),
            {group_sql["map_cte"]}
            SELECT
                gm.fenju_name AS "分局",
                gm.group_name AS "当前分组名称",
                {aj_ajbh} AS "案件编号",
                {aj_jqbh} AS "警情编号",
                {aj_aymc} AS "案由",
                {aj_lasj} AS "立案时间",
                {aj_pxjarq} AS "破案时间",
                ROUND((EXTRACT(EPOCH FROM (({aj_pxjarq}) - ({aj_lasj}))) / 3600.0)::numeric, 2) AS "时差(小时)",
                {aj_cbdw_mc} AS "办案单位"
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {aj_pxjarq} IS NOT NULL
              AND COALESCE({aj_ajlx}, '') = '刑事'
              AND COALESCE({aj_pxjabs}, '') = '1'
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}
              {group_filter_sql}
              AND EXTRACT(EPOCH FROM (({aj_pxjarq}) - ({aj_lasj}))) / 3600.0 >= 0
            ORDER BY {aj_pxjarq} DESC, {aj_ajbh}
        """
        return _run(sql_text, params)

    if metric_key == "timely_close":
        params = list(base_params)
        if not is_all:
            params.append(selected_group)
        sql_text = f"""
            WITH
            params AS (
                SELECT
                    %s::timestamp AS start_time,
                    %s::timestamp AS end_time,
                    %s::text[] AS leixing_list,
                    %s::text[] AS ssfjdm_list
            ),
            {group_sql["map_cte"]}
            SELECT
                gm.fenju_name AS "分局",
                gm.group_name AS "当前分组名称",
                {aj_ajbh} AS "案件编号",
                {aj_jqbh} AS "警情编号",
                {aj_aymc} AS "案由",
                {aj_lasj} AS "立案时间",
                {aj_cfsj} AS "处罚时间",
                ROUND((EXTRACT(EPOCH FROM (({aj_cfsj}) - ({aj_lasj}))) / 3600.0)::numeric, 2) AS "时差(小时)",
                {aj_cbdw_mc} AS "办案单位"
            FROM "ywdata"."zq_zfba_ajxx" aj
            CROSS JOIN params p
            JOIN group_map gm
              ON gm.group_code = {group_sql["aj_group_expr"]}
            WHERE {aj_lasj} >= p.start_time
              AND {aj_lasj} <= p.end_time
              AND {aj_lasj} IS NOT NULL
              AND {aj_cfsj} IS NOT NULL
              AND COALESCE({aj_ajlx}, '') = '行政'
              AND COALESCE({aj_ajzt_dm}, '') NOT IN ('0101', '0104', '0112', '0114')
              {_aj_type_filter_sql(aj_aymc)}
              {_fenju_filter_sql()}
              {group_filter_sql}
              AND EXTRACT(EPOCH FROM (({aj_cfsj}) - ({aj_lasj}))) / 3600.0 >= 0
            ORDER BY {aj_cfsj} DESC, {aj_ajbh}
        """
        return _run(sql_text, params)

    raise ValueError(f"未知 metric: {metric_key}")
