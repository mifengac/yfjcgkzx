from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from psycopg2 import sql


def fetch_leixing_list(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute('SELECT DISTINCT "leixing" FROM "ywdata"."case_type_config" ORDER BY "leixing"')
        rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _as_dict_rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _normalize_leixing_list(leixing_list: Sequence[str]) -> List[str]:
    return [str(x).strip() for x in (leixing_list or []) if str(x).strip()]


def _count_by_diqu(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        code = str(row.get("地区") or "").strip() or "未知"
        out[code] = out.get(code, 0) + 1
    return out


def _resolve_sfzxx_case_col(conn) -> str:
    """
    解析 zq_wcnr_sfzxx 中可用于“案件编号”关联的字段名。
    优先使用显式案件编号字段；若不存在则回退到 bh。
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            ("ywdata", "zq_wcnr_sfzxx"),
        )
        cols = {str(r[0]) for r in cur.fetchall() if r and r[0]}

    for c in ("ajxx_ajbh", "ajbh", "bh"):
        if c in cols:
            return c
    raise RuntimeError(
        f'无法识别表 ywdata."zq_wcnr_sfzxx" 的案件编号字段（当前列：{sorted(cols)}）'
    )


def count_wfzf_wcnr_by_diqu(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> Tuple[Dict[str, int], int]:
    leixing_list = _normalize_leixing_list(leixing_list)

    if leixing_list:
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
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    query = sql.SQL(
        """
        WITH
        filtered_data AS (
            SELECT DISTINCT ON (vw."身份证号", vw."案件编号")
                vw."案件编号" AS 案件编号,
                vw."人员编号" AS 人员编号,
                vw."案件类型" AS 案件类型,
                vw."案由" AS 案由,
                vw."案由代码" AS 案由代码,
                vw."地区" AS 地区,
                vw."办案单位" AS 办案单位,
                vw."立案时间" AS 立案时间,
                vw."姓名" AS 姓名,
                vw."身份证号" AS 身份证号,
                vw."户籍地" AS 户籍地,
                vw."年龄" AS 年龄,
                vw."居住地" AS 居住地
            FROM "ywdata"."v_wcnr_wfry_base" vw
            WHERE vw."录入时间" BETWEEN %s AND %s
            {type_condition}
            ORDER BY vw."身份证号", vw."案件编号", vw."录入时间" DESC
        )
        SELECT COALESCE(fd."地区", '未知') AS 地区, COUNT(*)::INT AS cnt
        FROM filtered_data fd
        GROUP BY COALESCE(fd."地区", '未知')
        """
    ).format(type_condition=type_condition)

    params = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    out: Dict[str, int] = {str(r[0]): int(r[1]) for r in rows if r and r[0]}
    total = sum(out.values())
    return out, total


def fetch_wfzf_wcnr_detail(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str], diqu: str | None
) -> List[Dict[str, Any]]:
    leixing_list = _normalize_leixing_list(leixing_list)

    if leixing_list:
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
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    if diqu and str(diqu).strip() and str(diqu).strip().upper() != "ALL":
        diqu_condition = sql.SQL(' AND fd."地区" = %s ')
        diqu_params = [str(diqu).strip()]
    else:
        diqu_condition = sql.SQL("")
        diqu_params = []

    query = sql.SQL(
        """
        WITH
        filtered_data AS (
            SELECT DISTINCT ON (vw."身份证号", vw."案件编号")
                vw."案件编号" AS 案件编号,
                vw."人员编号" AS 人员编号,
                vw."案件类型" AS 案件类型,
                vw."案由" AS 案由,
                vw."案由代码" AS 案由代码,
                vw."地区" AS 地区,
                vw."办案单位" AS 办案单位,
                vw."立案时间" AS 立案时间,
                vw."姓名" AS 姓名,
                vw."身份证号" AS 身份证号,
                vw."户籍地" AS 户籍地,
                vw."年龄" AS 年龄,
                vw."居住地" AS 居住地
            FROM "ywdata"."v_wcnr_wfry_base" vw
            WHERE vw."录入时间" BETWEEN %s AND %s
            {type_condition}
            ORDER BY vw."身份证号", vw."案件编号", vw."录入时间" DESC
        )
        SELECT
            fd."案件编号" AS 案件编号,
            fd."人员编号" AS 人员编号,
            fd."案件类型" AS 案件类型,
            fd."案由" AS 案由,
            fd."案由代码" AS 案由代码,
            fd."地区" AS 地区,
            fd."办案单位" AS 办案单位,
            TO_CHAR(fd."立案时间", 'YYYY-MM-DD HH24:MI:SS') AS 立案时间,
            fd."姓名" AS 姓名,
            fd."身份证号" AS 身份证号,
            fd."户籍地" AS 户籍地,
            fd."年龄" AS 年龄,
            fd."居住地" AS 居住地
        FROM filtered_data fd
        WHERE 1=1
        {diqu_condition}
        ORDER BY fd."立案时间" DESC, fd."姓名"
        """
    ).format(type_condition=type_condition, diqu_condition=diqu_condition)

    params = [start_time, end_time] + type_params + diqu_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        return _as_dict_rows(cur)


def count_jyh_after_cases_by_diqu(
    conn,
    *,
    start_date: str,
    end_date: str,
    leixing_list: Sequence[str],
    only_xingshi: bool,
) -> Tuple[Dict[str, int], int]:
    leixing_list = _normalize_leixing_list(leixing_list)
    sfz_case_col = _resolve_sfzxx_case_col(conn)

    if leixing_list:
        type_condition = sql.SQL(
            """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND zzx."案由" SIMILAR TO ctc."ay_pattern"
            )
            """
        )
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    xingshi_condition = sql.SQL(' AND zzx."案件类型" = \'刑事\' ') if only_xingshi else sql.SQL("")

    query = sql.SQL(
        """
        SELECT
            COALESCE(LEFT(COALESCE(zws."hjdq", ''), 6), '未知') AS 地区,
            COUNT(*)::INT AS cnt
        FROM "ywdata"."zq_wcnr_sfzxx" zws
        INNER JOIN "ywdata"."v_wcnr_wfry_base" zzx
            ON zws."sfzhm" = zzx."身份证号"
           AND COALESCE(zws.{sfz_case_col}::text, '') = COALESCE(zzx."案件编号"::text, '')
        WHERE zws."lx_time" BETWEEN %s AND %s
          AND zws."lx_time" < zzx."立案时间"
          {xingshi_condition}
          {type_condition}
        GROUP BY COALESCE(LEFT(COALESCE(zws."hjdq", ''), 6), '未知')
        """
    ).format(
        xingshi_condition=xingshi_condition,
        type_condition=type_condition,
        sfz_case_col=sql.Identifier(sfz_case_col),
    )

    params = [start_date, end_date] + type_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    out: Dict[str, int] = {str(r[0]): int(r[1]) for r in rows if r and r[0]}
    total = sum(out.values())
    return out, total


def fetch_jyh_after_cases_detail(
    conn,
    *,
    start_date: str,
    end_date: str,
    leixing_list: Sequence[str],
    only_xingshi: bool,
    diqu: str | None,
) -> List[Dict[str, Any]]:
    leixing_list = _normalize_leixing_list(leixing_list)
    sfz_case_col = _resolve_sfzxx_case_col(conn)

    if leixing_list:
        type_condition = sql.SQL(
            """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND zzx."案由" SIMILAR TO ctc."ay_pattern"
            )
            """
        )
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    xingshi_condition = sql.SQL(' AND zzx."案件类型" = \'刑事\' ') if only_xingshi else sql.SQL("")

    if diqu and str(diqu).strip() and str(diqu).strip().upper() != "ALL":
        diqu_condition = sql.SQL(" AND LEFT(COALESCE(zws.\"hjdq\", ''), 6) = %s ")
        diqu_params = [str(diqu).strip()]
    else:
        diqu_condition = sql.SQL("")
        diqu_params = []

    query = sql.SQL(
        """
        SELECT
            LEFT(COALESCE(zws."hjdq", ''), 6) AS 地区,
            zws."xm" AS 学生姓名,
            zws."xb" AS 性别,
            zws."sfzhm" AS 身份证号,
            zws."hjdq" AS 户籍地区,
            zws."hjdz" AS 户籍地址,
            zws."nj" AS 年级,
            TO_CHAR(zws."rx_time", 'YYYY-MM-DD') AS 入校时间,
            TO_CHAR(zws."lx_time", 'YYYY-MM-DD') AS 离校时间,
            zws."lxdh" AS 联系电话,
            zws."yxx" AS 学校,

            zzx."案件编号" AS 案件编号,
            zzx."案件类型" AS 案件类型,
            zzx."案由" AS 案由,
            TO_CHAR(zzx."立案时间", 'YYYY-MM-DD HH24:MI:SS') AS 立案时间
        FROM "ywdata"."zq_wcnr_sfzxx" zws
        INNER JOIN "ywdata"."v_wcnr_wfry_base" zzx
            ON zws."sfzhm" = zzx."身份证号"
           AND COALESCE(zws.{sfz_case_col}::text, '') = COALESCE(zzx."案件编号"::text, '')
        WHERE zws."lx_time" BETWEEN %s AND %s
          AND zws."lx_time" < zzx."立案时间"
          {xingshi_condition}
          {type_condition}
          {diqu_condition}
        ORDER BY zws."lx_time" DESC, zws."sfzhm", zzx."立案时间" DESC
        """
    ).format(
        xingshi_condition=xingshi_condition,
        type_condition=type_condition,
        diqu_condition=diqu_condition,
        sfz_case_col=sql.Identifier(sfz_case_col),
    )

    params = [start_date, end_date] + type_params + diqu_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        return _as_dict_rows(cur)


def fetch_bqh_ajxx_base_detail(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str], diqu: str | None
) -> List[Dict[str, Any]]:
    leixing_list = _normalize_leixing_list(leixing_list)

    if leixing_list:
        type_condition = sql.SQL(
            """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND zzws."ajxx_aymc" SIMILAR TO ctc."ay_pattern"
            )
            """
        )
        type_params = [list(leixing_list)]
    else:
        type_condition = sql.SQL("")
        type_params = []

    if diqu and str(diqu).strip() and str(diqu).strip().upper() != "ALL":
        diqu_condition = sql.SQL(' AND LEFT(zzws."ajxx_cbdw_bh_dm", 6) = %s ')
        diqu_params = [str(diqu).strip()]
    else:
        diqu_condition = sql.SQL("")
        diqu_params = []

    query = sql.SQL(
        """
        SELECT
            zzws."ajxx_ajbh" AS 案件编号,
            zzws."ajxx_ajlx" AS 案件类型,
            zzws."ajxx_ajmc" AS 案件名称,
            LEFT(zzws."ajxx_cbdw_bh_dm", 6) AS 地区,
            zzws."ajxx_cbdw_mc" AS 办案单位,
            zzws."ajxx_jyaq" AS 简要案情,
            TO_CHAR(zzws."ajxx_lasj", 'YYYY-MM-DD HH24:MI:SS') AS 立案时间,
            zzws."ajxx_fadd" AS 发案地点,
            zzws."ajxx_ajzt" AS 案件状态,
            TO_CHAR(zzws."ajxx_fasj", 'YYYY-MM-DD HH24:MI:SS') AS 发案时间
        FROM "ywdata"."zq_zfba_wcnr_shr_ajxx" zzws
        WHERE zzws."ajxx_lasj" BETWEEN %s AND %s
        {type_condition}
        {diqu_condition}
        ORDER BY zzws."ajxx_lasj" DESC
        """
    ).format(type_condition=type_condition, diqu_condition=diqu_condition)

    params = [start_time, end_time] + type_params + diqu_params
    with conn.cursor() as cur:
        cur.execute(query, params)
        return _as_dict_rows(cur)


def count_yzbl_jzjy_cover_by_diqu(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> Tuple[Dict[str, int], Dict[str, int], int, int]:
    # 口径统一：复用“矫治情况统计”数据源逻辑，避免 1393 与 jzqk 口径偏差。
    from hqzcsj.dao import jzqk_tongji_dao

    rows = jzqk_tongji_dao.fetch_jzqk_data(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=_normalize_leixing_list(leixing_list),
    )
    denom_by = _count_by_diqu(rows)
    num_rows = [r for r in rows if str(r.get("是否开具矫治文书") or "").strip() == "是"]
    num_by = _count_by_diqu(num_rows)
    return num_by, denom_by, sum(num_by.values()), sum(denom_by.values())


def fetch_yzbl_jzjy_cover_detail(
    conn, *, start_time: str, end_time: str, leixing_list: Sequence[str], diqu: str | None
) -> List[Dict[str, Any]]:
    # 口径统一：复用“矫治情况统计”数据源逻辑，输出明细时仅按地区筛选。
    from hqzcsj.dao import jzqk_tongji_dao

    rows = jzqk_tongji_dao.fetch_jzqk_data(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=_normalize_leixing_list(leixing_list),
    )
    if diqu and str(diqu).strip() and str(diqu).strip().upper() != "ALL":
        code = str(diqu).strip()
        rows = [r for r in rows if str(r.get("地区") or "").strip() == code]
    return rows


def _resolve_xjs2_join_cols(conn) -> Tuple[str, str]:
    """
    解析 zq_zfba_xjs2 的关键字段名（兼容大小写差异）。
    返回: (ajbh_col, xgry_xm_col)
    """
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
