from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from psycopg2 import sql

from gonggong.config.database import DB_CONFIG, get_database_connection


SCHEMA = DB_CONFIG.get("schema") or "ywdata"


def fetch_leixing_list(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute('SELECT leixing FROM "ywdata"."case_type_config" ORDER BY leixing')
        rows = cur.fetchall()
    return [str(r[0]).strip() for r in rows if r and str(r[0]).strip()]


def fetch_ay_patterns(conn, *, leixing_list: Sequence[str]) -> List[str]:
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    if not leixing_list:
        return []
    with conn.cursor() as cur:
        cur.execute(
            'SELECT ay_pattern FROM "ywdata"."case_type_config" WHERE leixing = ANY(%s)',
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


def _table_has_data_col(conn, *, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name='data'
            LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def _text(alias: str, col: str, *, has_data: bool) -> sql.SQL:
    if has_data:
        return sql.SQL("COALESCE({a}.{c}, {a}.data->>{k})").format(
            a=sql.Identifier(alias),
            c=sql.Identifier(col),
            k=sql.Literal(col),
        )
    return sql.SQL("{a}.{c}").format(a=sql.Identifier(alias), c=sql.Identifier(col))


def _left6(expr: sql.SQL) -> sql.SQL:
    return sql.SQL("LEFT({}, 6)").format(expr)


def _ts(alias: str, col: str, *, has_data: bool) -> sql.SQL:
    if has_data:
        # 兼容 fast(JSONB) 模式：data 里可能是 "-"，必须先正则判断再 cast
        return sql.SQL(
            "COALESCE({a}.{c}, CASE WHEN ({a}.data->>{k}) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}} [0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}$' "
            "THEN ({a}.data->>{k})::timestamp END)"
        ).format(a=sql.Identifier(alias), c=sql.Identifier(col), k=sql.Literal(col))
    return sql.SQL("{a}.{c}").format(a=sql.Identifier(alias), c=sql.Identifier(col))


def _exists_similar_to_patterns(patterns: Sequence[str], *, field_expr: sql.SQL) -> Tuple[sql.SQL, List[Any]]:
    pats = [str(x).strip() for x in (patterns or []) if str(x).strip()]
    if not pats:
        return sql.SQL(""), []
    frag = sql.SQL(
        " AND EXISTS (SELECT 1 FROM unnest(%s::text[]) p(pattern) WHERE {field} SIMILAR TO p.pattern)"
    ).format(field=field_expr)
    return frag, [pats]


def count_jq_by_diqu(conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]) -> Dict[str, int]:
    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    where_leixing = sql.SQL("")
    params: List[Any] = [start_time, end_time]
    if leixing_list:
        where_leixing = sql.SQL(' AND fc."leixing" = ANY(%s)')
        params.append(leixing_list)

    q = (
        sql.SQL(
            """
            WITH flat_config AS (
                SELECT ctc.leixing,
                    unnest(ctc.newcharasubclass_list) AS single_code,
                    'original' AS code_type
                FROM ywdata.case_type_config ctc
                UNION ALL
                SELECT ctc.leixing,
                    unnest(ctc.newcharasubclass_list) AS single_code,
                    'confirmed' AS code_type
                FROM ywdata.case_type_config ctc
            )
            SELECT LEFT(jq."cmdid", 6) AS diqu, COUNT(DISTINCT (jq.id, fc.code_type)) AS cnt
            FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq
            JOIN flat_config fc ON (
                (jq.neworicharasubclass = fc.single_code AND fc.code_type = 'original')
                --OR (jq.newcharasubclass = fc.single_code AND fc.code_type = 'confirmed')
            )
            WHERE jq."calltime" BETWEEN %s AND %s
            AND 1=1
            """
        )
        + where_leixing
        + sql.SQL(' GROUP BY LEFT(jq."cmdid", 6)')
    )
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if diqu is None:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_ajxx_by_diqu_and_ajlx(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, Dict[str, int]]:
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("aj", "ajxx_cbdw_bh_dm", has_data=has_data))
    ajlx_expr = _text("aj", "ajxx_ajlx", has_data=has_data)
    aymc_expr = _text("aj", "ajxx_aymc", has_data=has_data)
    time_expr = _ts("aj", "ajxx_lasj", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)

    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, {ajlx} AS ajlx, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_ajxx aj "
            "WHERE {t} BETWEEN %s AND %s "
            "AND {ajlx} IN ('行政','刑事') "
            "AND 1=1 "
        ).format(diqu=diqu_expr, ajlx=ajlx_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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


def count_ajxx_all_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    """案件数：不区分行政/刑事，统计所有案件"""
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("aj", "ajxx_cbdw_bh_dm", has_data=has_data))
    aymc_expr = _text("aj", "ajxx_aymc", has_data=has_data)
    time_expr = _ts("aj", "ajxx_lasj", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)

    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_ajxx aj "
            "WHERE {t} BETWEEN %s AND %s "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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


def count_ajxx_banjie_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    """办结：案件状态 IN ('已立案','已受案','已受理')"""
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("aj", "ajxx_cbdw_bh_dm", has_data=has_data))
    ajzt_expr = _text("aj", "ajxx_ajzt", has_data=has_data)
    aymc_expr = _text("aj", "ajxx_aymc", has_data=has_data)
    time_expr = _ts("aj", "ajxx_lasj", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)

    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_ajxx aj "
            "WHERE {t} BETWEEN %s AND %s "
            "AND {ajzt} IN ('已立案','已受案','已受理') "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr, ajzt=ajzt_expr)
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


def count_gaozhiliang_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    """高质量：刑事案件关联拘留证（刑拘人数>2）"""
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    aj_has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_jlz")
    diqu_expr = _left6(_text("aj", "ajxx_cbdw_bh_dm", has_data=has_data))
    aymc_expr = _text("aj", "ajxx_aymc", has_data=has_data)
    time_expr = _ts("aj", "ajxx_lasj", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)

    # 计数口径必须与“详情”一致：按案件编号聚合后 HAVING 刑拘人数>2，再按地区计数
    q = (
        sql.SQL(
            "SELECT diqu, COUNT(1) AS cnt FROM ("
            "SELECT {diqu} AS diqu, aj.ajxx_ajbh AS ajbh "
            "FROM {schema}.zq_zfba_ajxx aj "
            "LEFT JOIN {schema}.zq_zfba_jlz jlz ON aj.ajxx_ajbh = jlz.ajxx_ajbh "
            "WHERE {t} BETWEEN %s AND %s "
            "AND aj.ajxx_ajlx = '刑事' "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
        + pat_sql
        + sql.SQL(" GROUP BY {diqu}, aj.ajxx_ajbh HAVING COUNT(jlz.jlz_id) > 2").format(diqu=diqu_expr)
        + sql.SQL(") t GROUP BY diqu")
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


def count_xzcfjds_zhiju_by_diqu(
    conn, *, start_time: str, end_time: str, patterns: Sequence[str], za_types: Sequence[str] = ()
) -> Dict[str, int]:
    """治安处罚：支持按处罚种类过滤（警告、罚款、拘留）"""
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_xzcfjds")
    aj_has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("xz", "xzcfjds_cbdw_bh_dm", has_data=has_data))
    time_expr = _ts("xz", "xzcfjds_spsj", has_data=has_data)
    cfzl_expr = _text("xz", "xzcfjds_cfzl", has_data=has_data)
    aymc_expr = _text("aj", "ajxx_aymc", has_data=aj_has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)

    # 构建处罚种类过滤条件
    za_types = [str(x).strip() for x in (za_types or []) if str(x).strip()]
    cfzl_conditions = []
    cfzl_params: List[Any] = []
    if za_types:
        for zt in za_types:
            cfzl_conditions.append(f"{cfzl_expr.as_string(conn)} ~ %s")
            cfzl_params.append(zt)

    if cfzl_conditions:
        cfzl_where = " AND (" + " OR ".join(cfzl_conditions) + ")"
    else:
        # 不选治安处罚类型：默认全量（不加过滤条件）
        cfzl_where = ""

    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_xzcfjds xz "
            "LEFT JOIN {schema}.zq_zfba_ajxx aj ON aj.ajxx_ajbh = xz.ajxx_ajbh "
            "WHERE {t} BETWEEN %s AND %s "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
        + sql.SQL(cfzl_where)
        + pat_sql
        + sql.SQL(" GROUP BY diqu")
    )
    with conn.cursor() as cur:
        cur.execute(q, [start_time, end_time] + cfzl_params + pat_params)
        rows = cur.fetchall()
    out: Dict[str, int] = {}
    for diqu, cnt in rows:
        if not diqu:
            continue
        out[str(diqu)] = int(cnt or 0)
    return out


def count_jlz_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_jlz")
    diqu_expr = _left6(_text("jlz", "jlz_cbdw_bh_dm", has_data=has_data))
    time_expr = _ts("jlz", "jlz_pzsj", has_data=has_data)
    aymc_expr = _text("jlz", "jlz_ay_mc", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)
    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_jlz jlz "
            "WHERE {t} BETWEEN %s AND %s "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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


def count_dbz_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_dbz")
    diqu_expr = _left6(_text("dbz", "dbz_cbdw_bh_dm", has_data=has_data))
    time_expr = _ts("dbz", "dbz_pzdbsj", has_data=has_data)
    dbyy_expr = _text("dbz", "dbz_dbyy", has_data=has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=dbyy_expr)
    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_dbz dbz "
            "WHERE {t} BETWEEN %s AND %s "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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


def count_qsryxx_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_qsryxx")
    aj_has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("aj", "ajxx_cbdw_bh_dm", has_data=aj_has_data))
    time_expr = _ts("qs", "qsryxx_tfsj", has_data=has_data)
    aymc_expr = _text("aj", "ajxx_aymc", has_data=aj_has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)
    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_qsryxx qs "
            "LEFT JOIN {schema}.zq_zfba_ajxx aj ON aj.ajxx_ajbh = qs.ajxx_ajbh "
            "WHERE {t} BETWEEN %s AND %s "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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


def count_ysajtzs_by_diqu(conn, *, start_time: str, end_time: str, patterns: Sequence[str]) -> Dict[str, int]:
    has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ysajtzs")
    aj_has_data = _table_has_data_col(conn, schema=SCHEMA, table="zq_zfba_ajxx")
    diqu_expr = _left6(_text("ys", "ysajtzs_cbdw_bh_dm", has_data=has_data))
    time_expr = _ts("ys", "ysajtzs_pzsj", has_data=has_data)
    aymc_expr = _text("aj", "ajxx_aymc", has_data=aj_has_data)
    pat_sql, pat_params = _exists_similar_to_patterns(patterns, field_expr=aymc_expr)
    q = (
        sql.SQL(
            "SELECT {diqu} AS diqu, COUNT(1) AS cnt "
            "FROM {schema}.zq_zfba_ysajtzs ys "
            "LEFT JOIN {schema}.zq_zfba_ajxx aj ON aj.ajxx_ajbh = ys.ajxx_ajbh "
            "WHERE {t} BETWEEN %s AND %s "
            "AND 1=1 "
        ).format(diqu=diqu_expr, schema=sql.Identifier(SCHEMA), t=time_expr)
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
    """
    返回 (rows, truncated)；rows 仅包含"常用字段"。
    metric: 警情/案件数/行政/刑事/治安处罚/刑拘/逮捕/起诉/移送案件/办结/高质量
    diqu: 6位地区码 或 "__ALL__"(全市)
    """
    metric = (metric or "").strip()
    diqu = (diqu or "").strip()
    is_all = diqu in ("", "__ALL__", "全市")
    patterns = fetch_ay_patterns(conn, leixing_list=leixing_list)
    limit_n = int(limit) if limit and int(limit) > 0 else 0
    truncated = False

    with conn.cursor() as cur:
        if metric == "警情":
            leixing_list2 = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
            where_leixing = sql.SQL("")
            params: List[Any] = [start_time, end_time]
            if leixing_list2:
                where_leixing = sql.SQL(' AND fc."leixing" = ANY(%s)')
                params.append(leixing_list2)
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(' AND LEFT(jq."cmdid", 6) = %s')
                params.append(diqu)
            q = (
                sql.SQL(
                    """
                    WITH flat_config AS (
                        SELECT ctc.leixing,
                            unnest(ctc.newcharasubclass_list) AS single_code,
                            'original' AS code_type
                        FROM ywdata.case_type_config ctc
                        UNION ALL
                        SELECT ctc.leixing,
                            unnest(ctc.newcharasubclass_list) AS single_code,
                            'confirmed' AS code_type
                        FROM ywdata.case_type_config ctc
                    )
                    SELECT DISTINCT ON (jq.id, fc.code_type)
                      jq."calltime" AS "报警时间",
                      jq."caseno" AS "警情编号",
                      jq."dutydeptname" AS "管辖单位",
                      jq."cmdname" AS "分局",
                      jq."occuraddress" AS "警情地址",
                      jq."casecontents" AS "报警内容",
                      jq."replies" AS "处警情况",
                      jq."casemarkok" AS "警情标注",
                      jq."lngofcriterion" AS "经度",
                      jq."latofcriterion" AS "纬度",
                      LEFT(jq."cmdid", 6) AS "地区",
                      jq.id,
                      fc.code_type
                    FROM "ywdata"."zq_kshddpt_dsjfx_jq" jq
                    JOIN flat_config fc ON (
                        (jq.neworicharasubclass = fc.single_code AND fc.code_type = 'original')
                        --OR (jq.newcharasubclass = fc.single_code AND fc.code_type = 'confirmed')
                    )
                    WHERE jq."calltime" BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_leixing
                + where_diqu
                + sql.SQL(' ORDER BY jq.id, fc.code_type, jq."calltime" DESC')
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params.append(limit_n + 1)
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            rows_raw = cur.fetchall()
            # 移除用于去重的 id 和 code_type 字段
            display_cols = [c for c in cols if c not in ('id', 'code_type')]
            rows = []
            for r in rows_raw:
                row_dict = dict(zip(cols, r))
                # 仅保留显示字段
                display_row = {k: v for k, v in row_dict.items() if k in display_cols}
                rows.append(display_row)
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        # 其他指标：使用宽表字段（若启用了 fast(JSONB) 仍能工作，因为列仍在；但新插入行宽表列可能为空）
        if metric in ("行政", "刑事"):
            ajlx = metric
            params2: List[Any] = [start_time, end_time, ajlx]
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
                      ajxx_ajzt AS "案件状态",
                      ajxx_ay AS "案由",
                      ajxx_ay_dm AS "案由代码",
                      ajxx_fasj AS "发案时间",
                      ajxx_lasj AS "立案时间",
                      ajxx_sldw_mc AS "受理单位",
                      ajxx_cbdw_mc AS "承办单位",
                      LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                      ajxx_zbbj AS "在办标记",
                      ajxx_ajly AS "案件来源"
                    FROM "ywdata"."zq_zfba_ajxx"
                    WHERE ajxx_lasj BETWEEN %s AND %s
                    AND ajxx_ajlx = %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY ajxx_lasj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params2.append(limit_n + 1)
            cur.execute(q, params2)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "治安处罚":
            params3: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params3 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(xz.xzcfjds_cbdw_bh_dm,6)=%s")
                params3.append(diqu)

            # 构建处罚种类过滤条件
            za_types_list = [str(x).strip() for x in (za_types or []) if str(x).strip()]
            cfzl_where = ""
            if za_types_list:
                cfzl_conditions = " OR ".join([f"xz.xzcfjds_cfzl ~ %s" for _ in za_types_list])
                cfzl_where = f" AND ({cfzl_conditions})"
                params3 = [start_time, end_time] + za_types_list + pat_params
                if not is_all:
                    params3.append(diqu)
            else:
                # 不选治安处罚类型：默认全量（不加过滤条件）
                cfzl_where = ""

            q = (
                sql.SQL(
                    f"""
                    SELECT
                      xz.xzcfjds_id AS "决定书ID",
                      xz.ajxx_ajbh AS "案件编号",
                      xz.xzcfjds_ajmc AS "案件名称",
                      xz.xzcfjds_ryxm AS "人员姓名",
                      xz.xzcfjds_cfzl AS "处罚种类",
                      xz.xzcfjds_wfss AS "违法事实",
                      xz.xzcfjds_spsj AS "审批时间",
                      xz.xzcfjds_wszt AS "文书状态",
                      xz.xzcfjds_cbdw_mc AS "承办单位",
                      LEFT(xz.xzcfjds_cbdw_bh_dm, 6) AS "地区",
                      xz.xzcfjds_wsh AS "文书号"
                    FROM "ywdata"."zq_zfba_xzcfjds" xz
                    LEFT JOIN "ywdata"."zq_zfba_ajxx" aj ON aj.ajxx_ajbh = xz.ajxx_ajbh
                    WHERE xz.xzcfjds_spsj BETWEEN %s AND %s
                    {cfzl_where}
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY xz.xzcfjds_spsj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params3.append(limit_n + 1)
            cur.execute(q, params3)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "刑拘":
            params4: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("jlz.jlz_ay_mc"))
            params4 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(jlz.jlz_cbdw_bh_dm,6)=%s")
                params4.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      jlz.jlz_id AS "拘留证ID",
                      jlz.ajxx_ajbh AS "案件编号",
                      jlz.jlz_ajmc AS "案件名称",
                      jlz.jlz_ryxm AS "人员姓名",
                      jlz.jlz_xyrxb AS "性别",
                      jlz.jlz_xyrcsrq AS "出生日期",
                      jlz.jlz_xyrzz AS "住址",
                      jlz.jlz_jlyy_c AS "拘留原因",
                      jlz.jlz_kss_mc AS "看守所",
                      jlz.jlz_pzsj AS "批准时间",
                      jlz.jlz_wszt AS "文书状态",
                      jlz.jlz_cbdw_mc AS "承办单位",
                      LEFT(jlz.jlz_cbdw_bh_dm, 6) AS "地区",
                      jlz.jlz_wsh AS "文书号"
                    FROM "ywdata"."zq_zfba_jlz" jlz
                    WHERE jlz.jlz_pzsj BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY jlz.jlz_pzsj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params4.append(limit_n + 1)
            cur.execute(q, params4)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "逮捕":
            params5: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("dbz.dbz_dbyy"))
            params5 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(dbz.dbz_cbdw_bh_dm,6)=%s")
                params5.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      dbz.dbz_id AS "逮捕证ID",
                      dbz.ajxx_ajbh AS "案件编号",
                      dbz.dbz_ajmc AS "案件名称",
                      dbz.dbz_ryxm AS "人员姓名",
                      dbz.dbz_xyrcsrq AS "出生日期",
                      dbz.dbz_xyrzz AS "住址",
                      dbz.dbz_dbyy AS "逮捕原因",
                      dbz.dbz_pzwh AS "批准文号",
                      dbz.dbz_pzdbsj AS "批准逮捕时间",
                      dbz.dbz_wszt AS "文书状态",
                      dbz.dbz_cbdw_mc AS "承办单位",
                      LEFT(dbz.dbz_cbdw_bh_dm, 6) AS "地区",
                      dbz.dbz_wsh AS "文书号"
                    FROM "ywdata"."zq_zfba_dbz" dbz
                    WHERE dbz.dbz_pzdbsj BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY dbz.dbz_pzdbsj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params5.append(limit_n + 1)
            cur.execute(q, params5)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "起诉":
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
                      qs.qsryxx_id AS "起诉ID",
                      qs.ajxx_ajbh AS "案件编号",
                      qs.qsryxx_ajmc AS "案件名称",
                      qs.qsryxx_rybh AS "人员编号",
                      qs.qsryxx_sfzh AS "身份证号",
                      qs.qsryxx_ryxm AS "姓名",
                      qs.qsryxx_xb AS "性别",
                      qs.qsryxx_nl AS "年龄",
                      qs.qsryxx_lxfs AS "联系方式",
                      qs.qsryxx_tfsj AS "提访时间",
                      qs.qsryxx_wszt AS "文书状态",
                      qs.qsryxx_cbdw_mc AS "承办单位",
                      LEFT(aj.ajxx_cbdw_bh_dm, 6) AS "地区",
                      qs.qsryxx_wsh AS "文书号"
                    FROM "ywdata"."zq_zfba_qsryxx" qs
                    LEFT JOIN "ywdata"."zq_zfba_ajxx" aj ON aj.ajxx_ajbh = qs.ajxx_ajbh
                    WHERE qs.qsryxx_tfsj BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY qs.qsryxx_tfsj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params6.append(limit_n + 1)
            cur.execute(q, params6)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        if metric == "移送案件":
            params7: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params7 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(ys.ysajtzs_cbdw_bh_dm,6)=%s")
                params7.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      ys.ysajtzs_id AS "移送ID",
                      ys.ajxx_ajbh AS "案件编号",
                      ys.ysajtzs_ajmc AS "案件名称",
                      ys.ysajtzs_tfr_xm AS "通知人",
                      ys.ysajtzs_pzsj AS "批准时间",
                      ys.ysajtzs_wszt AS "文书状态",
                      ys.ysajtzs_ysyy AS "移送原因",
                      ys.ysajtzs_swdw AS "受文单位",
                      ys.ysajtzs_swdwmc AS "受文单位名称",
                      LEFT(ys.ysajtzs_cbdw_bh_dm, 6) AS "地区",
                      ys.ysajtzs_wsh AS "文书号"
                    FROM "ywdata"."zq_zfba_ysajtzs" ys
                    LEFT JOIN "ywdata"."zq_zfba_ajxx" aj ON aj.ajxx_ajbh = ys.ajxx_ajbh
                    WHERE ys.ysajtzs_pzsj BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY ys.ysajtzs_pzsj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params7.append(limit_n + 1)
            cur.execute(q, params7)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        # 新增指标：案件数（不区分行政/刑事）
        if metric == "案件数":
            params8: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
            params8 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(ajxx_cbdw_bh_dm, 6) = %s")
                params8.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      ajxx_ajbh AS "案件编号",
                      ajxx_jqbh AS "警情编号",
                      ajxx_ajmc AS "案件名称",
                      ajxx_ajlx AS "案件类型",
                      ajxx_ajzt AS "案件状态",
                      ajxx_ay AS "案由",
                      ajxx_ay_dm AS "案由代码",
                      ajxx_fasj AS "发案时间",
                      ajxx_lasj AS "立案时间",
                      ajxx_sldw_mc AS "受理单位",
                      ajxx_cbdw_mc AS "承办单位",
                      LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                      ajxx_zbbj AS "在办标记",
                      ajxx_ajly AS "案件来源"
                    FROM "ywdata"."zq_zfba_ajxx"
                    WHERE ajxx_lasj BETWEEN %s AND %s
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY ajxx_lasj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params8.append(limit_n + 1)
            cur.execute(q, params8)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        # 新增指标：办结（案件状态 IN ('已立案','已受案','已受理')）
        if metric == "办结":
            params9: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.Identifier("ajxx_aymc"))
            params9 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(ajxx_cbdw_bh_dm, 6) = %s")
                params9.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      ajxx_ajbh AS "案件编号",
                      ajxx_jqbh AS "警情编号",
                      ajxx_ajmc AS "案件名称",
                      ajxx_ajlx AS "案件类型",
                      ajxx_ajzt AS "案件状态",
                      ajxx_ay AS "案由",
                      ajxx_ay_dm AS "案由代码",
                      ajxx_fasj AS "发案时间",
                      ajxx_lasj AS "立案时间",
                      ajxx_sldw_mc AS "受理单位",
                      ajxx_cbdw_mc AS "承办单位",
                      LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
                      ajxx_zbbj AS "在办标记",
                      ajxx_ajly AS "案件来源"
                    FROM "ywdata"."zq_zfba_ajxx"
                    WHERE ajxx_lasj BETWEEN %s AND %s
                    AND ajxx_ajzt IN ('已立案','已受案','已受理')
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL(" ORDER BY ajxx_lasj DESC")
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params9.append(limit_n + 1)
            cur.execute(q, params9)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

        # 新增指标：高质量（刑事案件关联拘留证）
        if metric == "高质量":
            params10: List[Any] = [start_time, end_time]
            where_pat, pat_params = _exists_similar_to_patterns(patterns, field_expr=sql.SQL("aj.ajxx_aymc"))
            params10 += pat_params
            where_diqu = sql.SQL("")
            if not is_all:
                where_diqu = sql.SQL(" AND LEFT(aj.ajxx_cbdw_bh_dm,6)=%s")
                params10.append(diqu)
            q = (
                sql.SQL(
                    """
                    SELECT
                      aj.ajxx_ajbh AS "案件编号",
                      aj.ajxx_jqbh AS "警情编号",
                      aj.ajxx_ajmc AS "案件名称",
                      aj.ajxx_ajlx AS "案件类型",
                      aj.ajxx_ajzt AS "案件状态",
                      aj.ajxx_ay AS "案由",
                      aj.ajxx_ay_dm AS "案由代码",
                      aj.ajxx_fasj AS "发案时间",
                      aj.ajxx_lasj AS "立案时间",
                      aj.ajxx_sldw_mc AS "受理单位",
                      aj.ajxx_cbdw_mc AS "承办单位",
                      LEFT(aj.ajxx_cbdw_bh_dm, 6) AS "地区",
                      aj.ajxx_zbbj AS "在办标记",
                      aj.ajxx_ajly AS "案件来源",
                      COUNT(jlz.jlz_id) AS "刑拘人数"
                    FROM "ywdata"."zq_zfba_ajxx" aj
                    LEFT JOIN "ywdata"."zq_zfba_jlz" jlz ON aj.ajxx_ajbh = jlz.ajxx_ajbh
                    WHERE aj.ajxx_lasj BETWEEN %s AND %s
                    AND aj.ajxx_ajlx = '刑事'
                    AND 1=1
                    """
                )
                + where_pat
                + where_diqu
                + sql.SQL("""
                    GROUP BY
                      aj.ajxx_ajbh, aj.ajxx_jqbh, aj.ajxx_ajmc, aj.ajxx_ajlx,
                      aj.ajxx_ajzt, aj.ajxx_ay, aj.ajxx_ay_dm, aj.ajxx_fasj,
                      aj.ajxx_lasj, aj.ajxx_sldw_mc, aj.ajxx_cbdw_mc,
                      LEFT(aj.ajxx_cbdw_bh_dm, 6), aj.ajxx_zbbj, aj.ajxx_ajly
                    HAVING COUNT(jlz.jlz_id) > 2
                    ORDER BY aj.ajxx_lasj DESC
                """)
            )
            if limit_n:
                q = q + sql.SQL(" LIMIT %s")
                params10.append(limit_n + 1)
            cur.execute(q, params10)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if limit_n and len(rows) > limit_n:
                truncated = True
                rows = rows[:limit_n]
            return rows, truncated

    raise ValueError(f"未知 metric: {metric}")

