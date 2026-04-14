from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from hqzcsj.dao import zfba_jq_aj_dao, zfba_wcnr_jqaj_dao


GRADUATE_SOURCE_TABLE_SQL = '"ywdata"."zq_zfba_wcnr_sfzxx_lxxx"'


REGION_CODE_NAME: Dict[str, str] = {
    "445300": "市局",
    "445302": "云城",
    "445303": "云安",
    "445381": "罗定",
    "445321": "新兴",
    "445322": "郁南",
}
REGION_CODES: Tuple[str, ...] = tuple(REGION_CODE_NAME.keys())
REGION_NAME_CODE: Dict[str, str] = {v: k for k, v in REGION_CODE_NAME.items()}
TARGET_CHANGSUO_LABELS = {"重点管控行业"}
_CHANGSUO_JYAQ_KEYWORDS: Tuple[Tuple[str, str], ...] = (
    ("ktv", "KTV"),
    ("酒吧", "酒吧"),
    ("夜总会", "夜总会"),
    ("迪厅", "迪厅"),
    ("网吧", "网吧"),
    ("清吧", "清吧"),
    ("台球", "台球"),
    ("桌球", "桌球"),
    ("俱乐部", "俱乐部"),
    ("棋牌", "棋牌"),
    ("麻将", "麻将"),
    ("打牌", "打牌"),
    ("打扑克", "打扑克"),
)
_PLACE_TEXT_KEYWORDS: Tuple[str, ...] = tuple(keyword.lower() for keyword, _label in _CHANGSUO_JYAQ_KEYWORDS)
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

def fetch_leixing_list(conn) -> List[str]:
    return zfba_jq_aj_dao.fetch_leixing_list(conn)


def _normalize_leixing_list(leixing_list: Sequence[str]) -> List[str]:
    return [str(x).strip() for x in (leixing_list or []) if str(x).strip()]


def _normalize_leixing_for_query(conn, leixing_list: Sequence[str]) -> List[str]:
    """
    统一“类型”口径：
    - 未传/空：视为全量，不加类型过滤
    - 传入集合与 case_type_config 全量完全一致：同样视为全量，不加类型过滤
    - 其他：按所选类型过滤
    """
    selected = _normalize_leixing_list(leixing_list)
    if not selected:
        return []
    all_types = _normalize_leixing_list(fetch_leixing_list(conn))
    if not all_types:
        return selected
    if set(selected) == set(all_types):
        return []
    return selected


def _build_ay_fuzzy_filter(column_expr: str, leixing_list: Sequence[str]) -> Tuple[str, List[Any]]:
    leixing = _normalize_leixing_list(leixing_list)
    if not leixing:
        return "", []
    return (
        f"""
          AND EXISTS (
              SELECT 1
              FROM unnest(%s::text[]) kw(keyword)
              WHERE COALESCE({column_expr}, '') ILIKE ('%%' || kw.keyword || '%%')
          )
        """,
        [list(leixing)],
    )


def _build_ay_similar_filter(column_expr: str, leixing_list: Sequence[str]) -> Tuple[str, List[Any]]:
    """类型过滤：通过 case_type_config.ay_pattern 做 SIMILAR TO 匹配。
    - 未选类型（空列表）：返回空字符串，不加任何过滤（查全量）
    - 选了类型：AND EXISTS (SELECT 1 FROM case_type_config WHERE leixing=ANY(...) AND 列 SIMILAR TO ay_pattern)
    """
    leixing = _normalize_leixing_list(leixing_list)
    if not leixing:
        return "", []
    return (
        f"""
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."case_type_config" ctc
              WHERE ctc.leixing = ANY(%s::text[])
                AND COALESCE({column_expr}, '') SIMILAR TO ctc."ay_pattern"
          )
        """,
        [list(leixing)],
    )


def _extract_region_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text in REGION_CODE_NAME:
        return text
    if text in REGION_NAME_CODE:
        return REGION_NAME_CODE[text]
    if text.isdigit() and len(text) >= 6:
        code = text[:6]
        if code in REGION_CODE_NAME:
            return code
    return ""


def _region_name(code: str) -> str:
    return REGION_CODE_NAME.get(code, "未知")


def _safe_int(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _to_int_or_none(v: Any) -> int | None:
    text = str(v or "").strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def _is_yes(v: Any) -> bool:
    return str(v or "").strip() == "是"


def _is_no(v: Any) -> bool:
    return str(v or "").strip() == "否"


def _is_yzbl_num(row: Dict[str, Any]) -> bool:
    return (
        _is_yes(row.get("是否送校"))
        or (_is_yes(row.get("治拘大于4天")) and _is_no(row.get("是否治拘不送")))
        or _is_yes(row.get("是否刑拘"))
        or _is_yes(row.get("是否开具矫治文书"))
    )


def _is_zmjz_ratio_den_row(row: Dict[str, Any]) -> bool:
    return _is_yes(
        row.get("是否符合专门(矫治)教育")
        or row.get("是否符合专门教育")
    )


def _is_zmjz_ratio_num_row(row: Dict[str, Any]) -> bool:
    return _is_zmjz_ratio_den_row(row) and _is_yes(
        row.get("是否开具专门(矫治)教育申请书")
        or row.get("是否开具专门教育申请书")
    )


def _relation_exists(
    conn,
    *,
    schema: str,
    name: str,
    relkinds: Sequence[str] | None = None,
) -> bool:
    q = """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relname = %s
    """
    params: List[Any] = [schema, name]
    if relkinds:
        q += " AND c.relkind = ANY(%s)"
        params.append(list(relkinds))
    q += " LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(q, params)
        return cur.fetchone() is not None


def _row_region_code(row: Dict[str, Any]) -> str:
    return _extract_region_code(row.get("地区代码") or row.get("地区") or row.get("分局代码") or row.get("地区编号"))


def _normalize_count_map(raw: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {code: 0 for code in REGION_CODES}
    total = 0
    for key, value in (raw or {}).items():
        n = _safe_int(value)
        total += n
        code = _extract_region_code(key)
        if code in out:
            out[code] += n
    out["__ALL__"] = total
    return out


def _count_rows_by_region(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {code: 0 for code in REGION_CODES}
    total = 0
    for row in rows or []:
        total += 1
        code = _row_region_code(row)
        if code in out:
            out[code] += 1
    out["__ALL__"] = total
    return out


def _sum_field_by_region(rows: Sequence[Dict[str, Any]], *, value_key: str) -> Dict[str, int]:
    out: Dict[str, int] = {code: 0 for code in REGION_CODES}
    total = 0
    for row in rows or []:
        value = _safe_int(row.get(value_key))
        total += value
        code = _row_region_code(row)
        if code in out:
            out[code] += value
    out["__ALL__"] = total
    return out


def _count_distinct_by_region(rows: Sequence[Dict[str, Any]], *, id_key: str) -> Dict[str, int]:
    out_sets: Dict[str, set[str]] = {code: set() for code in REGION_CODES}
    all_set: set[str] = set()
    for row in rows or []:
        rid = str(row.get(id_key) or "").strip()
        if not rid:
            continue
        all_set.add(rid)
        code = _row_region_code(row)
        if code in out_sets:
            out_sets[code].add(rid)

    out: Dict[str, int] = {code: len(s) for code, s in out_sets.items()}
    out["__ALL__"] = len(all_set)
    return out


def _attach_region_fields(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        item = dict(row)
        code = _row_region_code(item)
        if code:
            item["地区代码"] = code
            item["地区"] = _region_name(code)
        else:
            if "地区代码" not in item:
                item["地区代码"] = ""
            item["地区"] = str(item.get("地区") or "未知")
        out.append(item)
    return out


def _rows_to_count_map(rows: Sequence[Tuple[Any, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for code, cnt in rows or []:
        key = _extract_region_code(code)
        if not key:
            continue
        out[key] = int(cnt or 0)
    return _normalize_count_map(out)


def _normalize_value_for_output(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    return v


def normalize_rows_for_output(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        item: Dict[str, Any] = {}
        for k, v in row.items():
            item[str(k)] = _normalize_value_for_output(v)
        out.append(item)
    return out


def _filter_changsuo_bqh_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        text = str(row.get("简要案情") or row.get("案情") or row.get("ajxx_jyaq") or "").strip()
        lowered = text.lower()
        hits = [label for keyword, label in _CHANGSUO_JYAQ_KEYWORDS if keyword in lowered]
        if not hits:
            continue
        item = dict(row)
        item["匹配关键词"] = "、".join(hits)
        out.append(item)
    return out


def _text_has_place_keyword(text: Any) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    return any(keyword in lowered for keyword in _PLACE_TEXT_KEYWORDS)


def _filter_rows_by_place_keywords(
    rows: Sequence[Dict[str, Any]],
    *,
    field_names: Sequence[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        text = ""
        for field in field_names or []:
            value = row.get(field)
            if value is None:
                continue
            candidate = str(value).strip()
            if candidate:
                text = candidate
                break
        if not _text_has_place_keyword(text):
            continue
        out.append(dict(row))
    return out


def _classify_bqh_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], bool]:
    if not rows:
        return [], False

    texts: List[str] = []
    for row in rows:
        text = str(row.get("发案地点") or row.get("案件发生地址名称") or "").strip()
        texts.append(text)

    degraded = False
    labels: List[Tuple[str, float]] = []
    try:
        from xunfang.service.jiemiansanlei_service import predict_addresses

        labels = predict_addresses(texts)
    except Exception as exc:  # noqa: BLE001
        degraded = True
        logging.exception("wcnr_10lv 地址分类模型不可用，降级到关键词识别: %s", exc)
        for text in texts:
            t = text.lower()
            hit = any(kw in t for kw in _KEY_INDUSTRY_ADDR_KEYWORDS)
            labels.append(("重点管控行业" if hit else "", 0.0))

    out: List[Dict[str, Any]] = []
    for row, (label, prob) in zip(rows, labels):
        item = dict(row)
        item["分类结果"] = str(label or "").strip()
        try:
            item["置信度"] = f"{float(prob):.5f}"
        except Exception:
            item["置信度"] = "0.00000"
        out.append(item)
    return out, degraded


def _fetch_graduates(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    jz_time_lt6: bool,
) -> List[Dict[str, Any]]:
    leixing = _normalize_leixing_list(leixing_list)
    jz_cond = 'AND COALESCE(zws."jz_time", 0) < 6' if jz_time_lt6 else ""

    type_cond = ""
    params: List[Any] = [start_time, end_time]
    if leixing:
        type_cond = """
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."zq_zfba_wcnr_xyr" x
              JOIN "ywdata"."case_type_config" ctc
                ON ctc."leixing" = ANY(%s)
             WHERE x."xyrxx_sfzh" = g."证件号码"
               AND x."ajxx_join_ajxx_lasj" > g."离校时间_raw"
               AND COALESCE(x."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
          )
        """
        params.append(list(leixing))

    q = f"""
        WITH grads AS (
            SELECT DISTINCT ON (zws."sfzhm")
                zws."sfzhm" AS "证件号码",
                zws."xm" AS "姓名",
                zws."rx_time" AS "入校时间_raw",
                zws."lx_time" AS "离校时间_raw",
                zws."jz_time" AS "矫治月数",
                LEFT(COALESCE(zws."hjdq", ''), 6) AS "地区代码",
                zws."hjdq" AS "户籍地区",
                zws."hjdz" AS "户籍地址",
                zws."nj" AS "年级",
                zws."yxx" AS "学校",
                zws."lxdh" AS "联系电话"
                        FROM {GRADUATE_SOURCE_TABLE_SQL} zws
            WHERE zws."lx_time" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(zws."sfzhm", '')), '') IS NOT NULL
              {jz_cond}
            ORDER BY zws."sfzhm", zws."lx_time" DESC
        )
        SELECT
            g."证件号码",
            g."姓名",
            g."地区代码",
            TO_CHAR(g."入校时间_raw", 'YYYY-MM-DD HH24:MI:SS') AS "入校时间",
            TO_CHAR(g."离校时间_raw", 'YYYY-MM-DD HH24:MI:SS') AS "离校时间",
            g."矫治月数",
            g."户籍地区",
            g."户籍地址",
            g."年级",
            g."学校",
            g."联系电话"
        FROM grads g
        WHERE 1=1
        {type_cond}
        ORDER BY g."离校时间_raw" DESC, g."证件号码"
    """

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _count_graduates_by_region(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    jz_time_lt6: bool,
) -> Dict[str, int]:
    leixing = _normalize_leixing_list(leixing_list)
    jz_cond = 'AND COALESCE(zws."jz_time", 0) < 6' if jz_time_lt6 else ""

    type_cond = ""
    params: List[Any] = [start_time, end_time]
    if leixing:
        type_cond = """
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."zq_zfba_wcnr_xyr" x
              JOIN "ywdata"."case_type_config" ctc
                ON ctc."leixing" = ANY(%s)
             WHERE x."xyrxx_sfzh" = g."证件号码"
               AND x."ajxx_join_ajxx_lasj" > g."离校时间_raw"
               AND COALESCE(x."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
          )
        """
        params.append(list(leixing))

    q = f"""
        WITH grads AS (
            SELECT DISTINCT ON (zws."sfzhm")
                zws."sfzhm" AS "证件号码",
                zws."lx_time" AS "离校时间_raw",
                LEFT(COALESCE(zws."hjdq", ''), 6) AS "地区代码"
                        FROM {GRADUATE_SOURCE_TABLE_SQL} zws
            WHERE zws."lx_time" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(zws."sfzhm", '')), '') IS NOT NULL
              {jz_cond}
            ORDER BY zws."sfzhm", zws."lx_time" DESC
        )
        SELECT g."地区代码", COUNT(1) AS cnt
        FROM grads g
        WHERE 1=1
        {type_cond}
        GROUP BY g."地区代码"
    """
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return _rows_to_count_map(rows)


def _fetch_graduate_reoffend(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    jz_time_lt6: bool,
    xingshi_only: bool,
    minor_only: bool,
) -> List[Dict[str, Any]]:
    leixing = _normalize_leixing_list(leixing_list)
    jz_cond = 'AND COALESCE(zws."jz_time", 0) < 6' if jz_time_lt6 else ""

    type_cond = ""
    xingshi_cond = ""
    minor_cond = ""
    params: List[Any] = [start_time, end_time]

    if leixing:
        type_cond = """
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."case_type_config" ctc
              WHERE ctc."leixing" = ANY(%s)
                AND COALESCE(x."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
          )
        """
        params.append(list(leixing))

    if xingshi_only:
        xingshi_cond = " AND x.\"ajxx_join_ajxx_ajlx\" = '刑事'"

    if minor_only:
        minor_cond = """
          AND x."xyrxx_sfzh" ~ '^[0-9]{17}[0-9Xx]$'
          AND age(
                x."ajxx_join_ajxx_lasj"::date,
                to_date(substr(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD')
              ) < interval '18 years'
        """

    q = f"""
        WITH grads AS (
            SELECT DISTINCT ON (zws."sfzhm")
                zws."sfzhm" AS "证件号码",
                zws."xm" AS "姓名",
                zws."lx_time" AS "离校时间_raw",
                LEFT(COALESCE(zws."hjdq", ''), 6) AS "地区代码",
                zws."hjdq" AS "户籍地区",
                zws."hjdz" AS "户籍地址",
                zws."yxx" AS "学校",
                zws."lxdh" AS "联系电话"
                        FROM {GRADUATE_SOURCE_TABLE_SQL} zws
            WHERE zws."lx_time" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(zws."sfzhm", '')), '') IS NOT NULL
              {jz_cond}
            ORDER BY zws."sfzhm", zws."lx_time" DESC
        )
        SELECT
            g."证件号码",
            g."姓名",
            g."地区代码",
            g."户籍地区",
            g."户籍地址",
            g."学校",
            g."联系电话",
            TO_CHAR(g."离校时间_raw", 'YYYY-MM-DD HH24:MI:SS') AS "离校时间",
            x."ajxx_join_ajxx_ajbh" AS "再犯案件编号",
            x."ajxx_join_ajxx_ajmc" AS "再犯案件名称",
            x."ajxx_join_ajxx_ajlx" AS "再犯案件类型",
            COALESCE(x."xyrxx_ay_mc", '') AS "再犯案由",
            TO_CHAR(x."ajxx_join_ajxx_lasj", 'YYYY-MM-DD HH24:MI:SS') AS "再犯立案时间"
        FROM grads g
        JOIN "ywdata"."zq_zfba_wcnr_xyr" x
          ON x."xyrxx_sfzh" = g."证件号码"
         AND x."ajxx_join_ajxx_lasj" > g."离校时间_raw"
        WHERE 1=1
        {xingshi_cond}
        {minor_cond}
        {type_cond}
        ORDER BY g."证件号码", x."ajxx_join_ajxx_lasj" DESC
    """

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _count_graduate_reoffend_by_region(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    jz_time_lt6: bool,
    xingshi_only: bool,
    minor_only: bool,
) -> Dict[str, int]:
    leixing = _normalize_leixing_list(leixing_list)
    jz_cond = 'AND COALESCE(zws."jz_time", 0) < 6' if jz_time_lt6 else ""

    type_cond = ""
    xingshi_cond = ""
    minor_cond = ""
    params: List[Any] = [start_time, end_time]

    if leixing:
        type_cond = """
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."case_type_config" ctc
              WHERE ctc."leixing" = ANY(%s)
                AND COALESCE(x."xyrxx_ay_mc", '') SIMILAR TO ctc."ay_pattern"
          )
        """
        params.append(list(leixing))

    if xingshi_only:
        xingshi_cond = " AND x.\"ajxx_join_ajxx_ajlx\" = '刑事'"

    if minor_only:
        minor_cond = """
          AND x."xyrxx_sfzh" ~ '^[0-9]{17}[0-9Xx]$'
          AND age(
                x."ajxx_join_ajxx_lasj"::date,
                to_date(substr(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD')
              ) < interval '18 years'
        """

    q = f"""
        WITH grads AS (
            SELECT DISTINCT ON (zws."sfzhm")
                zws."sfzhm" AS "证件号码",
                zws."lx_time" AS "离校时间_raw",
                LEFT(COALESCE(zws."hjdq", ''), 6) AS "地区代码"
                        FROM {GRADUATE_SOURCE_TABLE_SQL} zws
            WHERE zws."lx_time" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(zws."sfzhm", '')), '') IS NOT NULL
              {jz_cond}
            ORDER BY zws."sfzhm", zws."lx_time" DESC
        )
        SELECT g."地区代码", COUNT(DISTINCT g."证件号码") AS cnt
        FROM grads g
        JOIN "ywdata"."zq_zfba_wcnr_xyr" x
          ON x."xyrxx_sfzh" = g."证件号码"
         AND x."ajxx_join_ajxx_lasj" > g."离校时间_raw"
        WHERE 1=1
        {xingshi_cond}
        {minor_cond}
        {type_cond}
        GROUP BY g."地区代码"
    """
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return _rows_to_count_map(rows)


def _fetch_naguan_base(conn) -> List[Dict[str, Any]]:
    q = """
        SELECT
            bzr."xm" AS "姓名",
            bzr."zjhm" AS "证件号码",
            bzr."lxdh" AS "联系电话",
            bzr."hjdz" AS "户籍地",
            bzr."jzdz" AS "居住地",
            bzr."lgsj" AS "列管时间_raw",
            LEFT(COALESCE(bzr."ssfj_dm", ''), 6) AS "地区代码",
            bzr."ssfj_dm" AS "所属分局代码",
            bdz."ssfj" AS "分局",
            bdz."sspcs" AS "派出所",
            bzr."sspcs_dm" AS "所属派出所代码"
        FROM "stdata"."b_zdry_ryxx" bzr
        LEFT JOIN "stdata"."b_dic_zzjgdm" bdz
          ON bzr."sspcs_dm" = bdz."sspcsdm"
        WHERE COALESCE(NULLIF(bzr."deleteflag"::text, ''), '0') = '0'
          AND COALESCE(NULLIF(bzr."sflg"::text, ''), '0') = '1'
    """
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["列管时间"] = _normalize_value_for_output(item.get("列管时间_raw"))
        item.pop("列管时间_raw", None)
        out.append(item)
    return _attach_region_fields(out)


def _fetch_zljiaqjh_detail_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    use_base_view = _relation_exists(
        conn,
        schema="ywdata",
        name="v_wcnr_zljiaqjh_ratio_base",
        relkinds=("v", "m"),
    )

    if use_base_view:
        type_condition, type_params = _build_ay_similar_filter('src."案由名称"', leixing_list)
        q = f"""
            SELECT src.*
            FROM "ywdata"."v_wcnr_zljiaqjh_ratio_base" src
            WHERE src."录入时间" BETWEEN %s AND %s
              {type_condition}
            ORDER BY src."立案时间" DESC NULLS LAST, src."案件编号"
        """
        params: List[Any] = [start_time, end_time] + type_params
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return _attach_region_fields(rows)

    type_condition, type_params = _build_ay_similar_filter('"ajxx_join_ajxx_ay"', leixing_list)
    q = f"""
        WITH minor_agg AS MATERIALIZED (
            SELECT
                "ajxx_join_ajxx_ajbh" AS ajbh,
                MAX("xyrxx_lrsj") AS lrsj,
                MAX("ajxx_join_ajxx_ajlx") AS ajlx,
                MAX("ajxx_join_ajxx_ajmc") AS ajmc,
                MAX("ajxx_join_ajxx_ay") AS ay,
                MAX("ajxx_join_ajxx_cbdw_bh") AS cbdw_bh,
                MAX("ajxx_join_ajxx_cbdw_bh_dm") AS cbdw_bh_dm,
                MAX("ajxx_join_ajxx_lasj") AS lasj,
                COUNT(DISTINCT "xyrxx_sfzh") AS yzt_count
            FROM "ywdata"."v_wcnr_wfry_jbxx_base"
            WHERE "xyrxx_lrsj" BETWEEN %s AND %s
              {type_condition}
            GROUP BY "ajxx_join_ajxx_ajbh"
        ),
        jgh_agg AS MATERIALIZED (
            SELECT
                ajbh,
                COUNT(*) AS yzt_done
            FROM "ywdata"."zq_zfba_jtjyzdtzs2"
            GROUP BY ajbh
        )
        SELECT
            m.ajbh AS "案件编号",
            m.ajlx AS "案件类型",
            m.ajmc AS "案件名称",
            m.ay AS "案由名称",
            m.cbdw_bh AS "办案单位",
            m.cbdw_bh_dm AS "办案单位代码",
            LEFT(COALESCE(m.cbdw_bh_dm, ''), 6) AS "地区代码",
            m.lrsj AS "录入时间",
            m.lasj AS "立案时间",
            m.yzt_count AS "应责令加强监护数",
            COALESCE(j.yzt_done, 0) AS "已责令加强监护数"
        FROM minor_agg m
        LEFT JOIN jgh_agg j ON m.ajbh = j.ajbh
        ORDER BY m.lasj DESC NULLS LAST, m.ajbh
    """
    params: List[Any] = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _count_naguan_base_by_region(conn) -> Dict[str, int]:
    q = """
        SELECT
            LEFT(COALESCE(bzr."ssfj_dm", ''), 6) AS "地区代码",
            COUNT(1) AS cnt
        FROM "stdata"."b_zdry_ryxx" bzr
        WHERE COALESCE(NULLIF(bzr."deleteflag"::text, ''), '0') = '0'
          AND COALESCE(NULLIF(bzr."sflg"::text, ''), '0') = '1'
        GROUP BY LEFT(COALESCE(bzr."ssfj_dm", ''), 6)
    """
    with conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    return _rows_to_count_map(rows)


def _fetch_naguan_reoffend(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    leixing = _normalize_leixing_list(leixing_list)
    type_cond = ""
    params: List[Any] = [start_time, end_time]
    if leixing:
        type_cond = """
          AND EXISTS (
              SELECT 1
              FROM "ywdata"."case_type_config" ctc
              WHERE ctc."leixing" = ANY(%s)
                AND COALESCE(c."案由", '') SIMILAR TO ctc."ay_pattern"
          )
        """
        params.append(list(leixing))

    q = f"""
        WITH managed AS (
            SELECT
                bzr."xm" AS "姓名",
                bzr."zjhm" AS "证件号码",
                bzr."lxdh" AS "联系电话",
                bzr."hjdz" AS "户籍地",
                bzr."jzdz" AS "居住地",
                bzr."lgsj" AS "列管时间_raw",
                LEFT(COALESCE(bzr."ssfj_dm", ''), 6) AS "地区代码",
                bzr."ssfj_dm" AS "所属分局代码",
                bdz."ssfj" AS "分局",
                bdz."sspcs" AS "派出所",
                bzr."sspcs_dm" AS "所属派出所代码"
            FROM "stdata"."b_zdry_ryxx" bzr
            LEFT JOIN "stdata"."b_dic_zzjgdm" bdz
              ON bzr."sspcs_dm" = bdz."sspcsdm"
            WHERE COALESCE(NULLIF(bzr."deleteflag"::text, ''), '0') = '0'
              AND COALESCE(NULLIF(bzr."sflg"::text, ''), '0') = '1'
              AND NULLIF(BTRIM(COALESCE(bzr."zjhm", '')), '') IS NOT NULL
              AND bzr."lgsj" IS NOT NULL
        ),
        case_rows AS (
            SELECT
                x."xyrxx_sfzh" AS "证件号码",
                x."ajxx_join_ajxx_ajbh" AS "案件编号",
                x."ajxx_join_ajxx_ajmc" AS "案件名称",
                x."ajxx_join_ajxx_ajlx" AS "案件类型",
                COALESCE(x."xyrxx_ay_mc", '') AS "案由",
                x."ajxx_join_ajxx_lasj" AS "立案时间_raw"
            FROM "ywdata"."zq_zfba_xyrxx" x
            WHERE x."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(x."xyrxx_sfzh", '')), '') IS NOT NULL
              AND COALESCE(NULLIF(x."xyrxx_isdel_dm", ''), '0')::integer = 0
        )
        SELECT
            m."姓名",
            m."证件号码",
            m."联系电话",
            m."户籍地",
            m."居住地",
            TO_CHAR(m."列管时间_raw", 'YYYY-MM-DD HH24:MI:SS') AS "列管时间",
            m."地区代码",
            m."所属分局代码",
            m."分局",
            m."派出所",
            m."所属派出所代码",
            c."案件编号",
            c."案件名称",
            c."案件类型",
            c."案由",
            TO_CHAR(c."立案时间_raw", 'YYYY-MM-DD HH24:MI:SS') AS "再犯立案时间"
        FROM managed m
        JOIN case_rows c
          ON c."证件号码" = m."证件号码"
         AND m."列管时间_raw" < c."立案时间_raw"
        WHERE 1=1
        {type_cond}
        ORDER BY m."证件号码", c."立案时间_raw" DESC
    """

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _count_naguan_reoffend_by_region(
    conn,
    *,
    start_time: str,
    end_time: str,
    patterns: Sequence[str],
) -> Dict[str, int]:
    pat_values = [str(x).strip() for x in (patterns or []) if str(x).strip()]
    pat_cond = ""
    params: List[Any] = [start_time, end_time]
    if pat_values:
        pat_cond = """
          AND EXISTS (
              SELECT 1
              FROM unnest(%s::text[]) p(pattern)
              WHERE COALESCE(x."xyrxx_ay_mc", '') SIMILAR TO p.pattern
          )
        """
        params.append(pat_values)

    q = f"""
        WITH managed AS (
            SELECT
                bzr."zjhm" AS "证件号码",
                LEFT(COALESCE(bzr."ssfj_dm", ''), 6) AS "地区代码",
                MIN(bzr."lgsj") AS "首列管时间_raw"
            FROM "stdata"."b_zdry_ryxx" bzr
            WHERE COALESCE(NULLIF(bzr."deleteflag"::text, ''), '0') = '0'
              AND COALESCE(NULLIF(bzr."sflg"::text, ''), '0') = '1'
              AND NULLIF(BTRIM(COALESCE(bzr."zjhm", '')), '') IS NOT NULL
              AND bzr."lgsj" IS NOT NULL
            GROUP BY bzr."zjhm", LEFT(COALESCE(bzr."ssfj_dm", ''), 6)
        ),
        case_max AS (
            SELECT
                x."xyrxx_sfzh" AS "证件号码",
                MAX(x."ajxx_join_ajxx_lasj") AS "最大立案时间_raw"
            FROM "ywdata"."zq_zfba_xyrxx" x
            WHERE x."ajxx_join_ajxx_lasj" BETWEEN %s AND %s
              AND NULLIF(BTRIM(COALESCE(x."xyrxx_sfzh", '')), '') IS NOT NULL
              AND COALESCE(NULLIF(x."xyrxx_isdel_dm", ''), '0')::integer = 0
              {pat_cond}
            GROUP BY x."xyrxx_sfzh"
        )
        SELECT
            m."地区代码",
            COUNT(1) AS cnt
        FROM managed m
        JOIN case_max c
          ON c."证件号码" = m."证件号码"
         AND m."首列管时间_raw" < c."最大立案时间_raw"
        GROUP BY m."地区代码"
    """
    with conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return _rows_to_count_map(rows)


def _fetch_bqh_addr_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    patterns: Sequence[str],
) -> List[Dict[str, Any]]:
    params: List[Any] = [start_time, end_time]
    pat_cond = ""
    pat_values = [str(x).strip() for x in (patterns or []) if str(x).strip()]
    if pat_values:
        pat_cond = """
          AND EXISTS (
              SELECT 1
              FROM unnest(%s::text[]) p(pattern)
              WHERE COALESCE(ajxx_aymc, '') SIMILAR TO p.pattern
          )
        """
        params.append(pat_values)

    q = f"""
        SELECT
            LEFT(ajxx_cbdw_bh_dm, 6) AS "地区",
            ajxx_fadd AS "发案地点"
        FROM "ywdata"."zq_zfba_wcnr_shr_ajxx"
        WHERE ajxx_lasj BETWEEN %s AND %s
          AND ajxx_ajzt NOT IN ('已撤销','已合并')
          AND ajxx_cbdw_mc !~ '交通'
          AND 1=1
          {pat_cond}
    """
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_jzqk_compact_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    leixing = _normalize_leixing_list(leixing_list)
    if leixing:
        type_condition = """
            AND EXISTS (
                SELECT 1
                FROM "ywdata"."case_type_config" ctc
                WHERE ctc."leixing" = ANY(%s)
                  AND vw.案由 SIMILAR TO ctc."ay_pattern"
            )
        """
        type_params: List[Any] = [list(leixing)]
    else:
        type_condition = ""
        type_params = []

    q = f"""
        WITH violation_counts AS (
            SELECT
                w.xyrxx_sfzh AS 身份证号,
                COUNT(*) AS 违法次数,
                COUNT(DISTINCT w.ajxx_join_ajxx_ay_dm) AS 不同案由数
            FROM "ywdata"."zq_zfba_wcnr_xyr" w
            WHERE COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
              AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
            GROUP BY w.xyrxx_sfzh
        ),
        first_case_xjs AS (
            SELECT DISTINCT
                vw.身份证号,
                vw.案件编号 AS 当前案件编号,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM "ywdata"."zq_zfba_wcnr_xyr" w
                        JOIN "ywdata"."zq_zfba_xjs2" x
                          ON w."ajxx_join_ajxx_ajbh" = x.ajbh
                         AND w.xyrxx_xm = TRIM(x.xgry_xm)
                        WHERE w.xyrxx_sfzh = vw.身份证号
                          AND w."ajxx_join_ajxx_ajbh" <> vw.案件编号
                          AND COALESCE(NULLIF(w."xyrxx_isdel_dm", ''), '0')::integer = 0
                          AND COALESCE(NULLIF(w."ajxx_join_ajxx_isdel_dm", ''), '0')::integer = 0
                    ) THEN 1
                    ELSE 0
                END AS 有训诫书
            FROM "ywdata"."v_wcnr_wfry_base" vw
        ),
        base_data AS (
            SELECT DISTINCT
                vw.案件编号,
                vw.人员编号,
                vw.案件类型,
                vw.案由,
                vw.地区,
                vw.立案时间,
                vw.姓名,
                vw.身份证号,
                CASE WHEN vw.年龄::text ~ '^\\d+$' THEN CAST(vw.年龄 AS INTEGER) END AS 年龄数值,
                COALESCE(vc.违法次数, 0) AS 违法次数,
                COALESCE(vc.不同案由数, 0) AS 不同案由数,
                COALESCE(fcx.有训诫书, 0) AS 有训诫书
            FROM "ywdata"."v_wcnr_wfry_base" vw
            LEFT JOIN violation_counts vc ON vw.身份证号 = vc.身份证号
            LEFT JOIN first_case_xjs fcx ON vw.身份证号 = fcx.身份证号 AND vw.案件编号 = fcx.当前案件编号
            WHERE vw.录入时间 BETWEEN %s AND %s
            {type_condition}
        )
        SELECT
            bd.地区,
            bd.案件类型,
            bd.年龄数值,
            CASE
                WHEN bd.案件类型 = '行政' AND EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                    WHERE x.ajxx_ajbh = bd.案件编号
                      AND x.xzcfjds_rybh = bd.人员编号
                      AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
                ) THEN 1 ELSE 0
            END AS is_zhiju_gt4,
            CASE
                WHEN bd.案件类型 = '行政' AND EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_xzcfjds" x
                    WHERE x.ajxx_ajbh = bd.案件编号
                      AND x.xzcfjds_rybh = bd.人员编号
                      AND CAST(x.xzcfjds_tj_jlts AS INTEGER) > 4
                      AND x.xzcfjds_zxqk_text ~ '(不送|不执行)'
                ) THEN 1 ELSE 0
            END AS is_zhiju_busong,
            CASE
                WHEN bd.案件类型 = '行政'
                     AND bd.违法次数 = 2
                     AND bd.不同案由数 = 1
                     AND bd.有训诫书 = 1
                THEN 1 ELSE 0
            END AS is_second_same_ay_with_xjs,
            CASE
                WHEN bd.案件类型 = '行政' AND bd.违法次数 > 2 THEN 1 ELSE 0
            END AS is_third_plus,
            CASE
                WHEN bd.案件类型 = '刑事' AND EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_jlz" j
                    WHERE j.ajxx_ajbh = bd.案件编号 AND j.jlz_rybh = bd.人员编号
                ) THEN 1 ELSE 0
            END AS is_xingju,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" z
                    WHERE z.zltzs_ajbh = bd.案件编号 AND z.zltzs_rybh = bd.人员编号
                ) OR EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_xjs2" x
                    WHERE x.ajbh = bd.案件编号 AND TRIM(x.xgry_xm) = bd.姓名
                ) THEN 1 ELSE 0
            END AS is_jiaozhi_wenshu,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_zfba_tqzmjy" t
                    WHERE t.ajbh = bd.案件编号 AND TRIM(t.xgry_xm) = bd.姓名
                ) THEN 1 ELSE 0
            END AS is_zhuanmen_shenqingshu,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM "ywdata"."zq_wcnr_sfzxx" s
                    WHERE s.sfzhm = bd.身份证号
                      AND TO_CHAR(s.rx_time, 'YYYY-MM-DD') >= TO_CHAR(bd.立案时间, 'YYYY-MM-DD')
                ) THEN 1 ELSE 0
            END AS is_songxiao
        FROM base_data bd
    """
    params = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _fetch_wfzf_people_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    type_condition, type_params = _build_ay_similar_filter('v."案由"', leixing_list)
    q = f"""
        SELECT
            v.*,
            LEFT(COALESCE(v."办案部门编码", ''), 6) AS "地区代码"
        FROM "ywdata"."v_wcnr_wfry_jbxx" v
        WHERE v."录入时间" BETWEEN %s AND %s
        {type_condition}
        ORDER BY v."录入时间" DESC NULLS LAST, v."身份证号", v."姓名"
    """
    params: List[Any] = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _fetch_yzbl_ratio_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    use_base_view = _relation_exists(
        conn,
        schema="ywdata",
        name="v_wcnr_yzbl_ratio_base",
        relkinds=("v", "m"),
    )

    if use_base_view:
        type_condition, type_params = _build_ay_similar_filter('src."案由"', leixing_list)
        q = f"""
            SELECT
                src.*
            FROM "ywdata"."v_wcnr_yzbl_ratio_base" src
            WHERE src."录入时间" BETWEEN %s AND %s
              {type_condition}
            ORDER BY src."是否应采取矫治教育措施" DESC, src."是否开具矫治文书" DESC, src."身份证号"
        """
        params: List[Any] = [start_time, end_time] + type_params
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return _attach_region_fields(rows)

    type_condition, type_params = _build_ay_similar_filter('v."案由"', leixing_list)
    q = f"""
        WITH wenshu_pre AS MATERIALIZED (
            SELECT ajbh, xgry_xm, wsmc
            FROM "ywdata"."zq_zfba_wenshu"
            WHERE wsmc ~ '训诫书|责令未成年'
        ),
        jzws_agg AS MATERIALIZED (
            SELECT
                xy.xyrxx_sfzh,
                STRING_AGG(DISTINCT w.wsmc, ' / ') AS wsmc_list
            FROM wenshu_pre w
            JOIN "ywdata"."zq_zfba_xyrxx" xy
              ON w.ajbh = xy.ajxx_join_ajxx_ajbh
             AND TRIM(w.xgry_xm) = xy.xyrxx_xm
            GROUP BY xy.xyrxx_sfzh
        )
        SELECT
            v."姓名",
            v."身份证号",
            v."性别",
            v."最近一次发案年龄",
            v."现在年龄",
            v."录入时间",
            v."违法次数",
            v."案件类型",
            v."立案时间链",
            v."案由",
            v."户籍行政区",
            v."户籍地代码",
            v."户籍地",
            v."现住地",
            v."办案部门",
            v."办案部门编码",
            LEFT(COALESCE(v."办案部门编码", ''), 6) AS "地区代码",
            v."学校名称",
            v."年级名称",
            v."班级名称",
            v."就读状态",
            CASE
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_xzcfjds" xz
                    JOIN "ywdata"."zq_zfba_xyrxx" xy
                      ON xz.ajxx_ajbh = xy.ajxx_join_ajxx_ajbh
                     AND xz.xzcfjds_rybh = xy.xyrxx_rybh
                    WHERE xy.xyrxx_sfzh = v."身份证号"
                      AND xz.xzcfjds_zxqk_text !~ '不执行|不送'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_jlz" jl
                    JOIN "ywdata"."zq_zfba_xyrxx" xy
                      ON jl.ajxx_ajbh = xy.ajxx_join_ajxx_ajbh
                     AND jl.jlz_rybh = xy.xyrxx_rybh
                    WHERE xy.xyrxx_sfzh = v."身份证号"
                )
                THEN '是' ELSE '否'
            END AS "是否应采取矫治教育措施",
            CASE
                WHEN jz.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否'
            END AS "是否开具矫治文书",
            COALESCE(jz.wsmc_list, '') AS "开具文书名称"
        FROM "ywdata"."v_wcnr_wfry_jbxx" v
        LEFT JOIN jzws_agg jz ON v."身份证号" = jz.xyrxx_sfzh
        WHERE v."录入时间" BETWEEN %s AND %s
        {type_condition}
        ORDER BY "是否应采取矫治教育措施" DESC, "是否开具矫治文书" DESC, v."身份证号"
    """
    params: List[Any] = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _fetch_sx_songjiao_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    require_songjiao: bool,
) -> List[Dict[str, Any]]:
    use_base_view = _relation_exists(
        conn,
        schema="ywdata",
        name="v_wcnr_sx_songjiao_ratio_base",
        relkinds=("v", "m"),
    )
    having_condition = "HAVING BOOL_OR(is_songxue)" if require_songjiao else ""

    if use_base_view:
        type_condition, type_params = _build_ay_similar_filter('src."ajxx_join_ajxx_ay"', leixing_list)
        q = f"""
        WITH base_rows AS MATERIALIZED (
            SELECT
                src.*
            FROM "ywdata"."v_wcnr_sx_songjiao_ratio_base" src
            WHERE src."xyrxx_lrsj" BETWEEN %s AND %s
              {type_condition}
        ),
        ws_pre AS MATERIALIZED (
            SELECT ajbh, xgry_xm, wsmc
            FROM "ywdata"."zq_zfba_wenshu"
            WHERE wsmc ~ '不予立案通知书|撤销案件决定书|不予行政处罚决定书|提请专门'
        ),
        sx_wsmc AS MATERIALIZED (
            SELECT
                l."xyrxx_sfzh",
                STRING_AGG(DISTINCT p.wsmc, ';' ORDER BY p.wsmc) AS ksws_mc
            FROM base_rows l
            JOIN ws_pre p ON (
                (p.wsmc ~ '不予立案通知书|撤销案件决定书'
                    AND p.ajbh = l."ajxx_join_ajxx_ajbh")
                OR (p.wsmc ~ '不予行政处罚决定书'
                    AND p.ajbh = l."ajxx_join_ajxx_ajbh"
                    AND TRIM(p.xgry_xm) = l."xyrxx_xm")
                OR (p.wsmc ~ '提请专门'
                    AND p.ajbh = l."ajxx_join_ajxx_ajbh"
                    AND TRIM(p.xgry_xm) = l."xyrxx_xm")
            )
            GROUP BY l."xyrxx_sfzh"
        )
        SELECT
            MAX("xyrxx_xm")  AS "姓名",
            "xyrxx_sfzh"     AS "身份证号",
            MAX("xyrxx_hjdxzqh")    AS "户籍区域",
            MAX("xyrxx_hjdxzqh_dm") AS "户籍区域代码",
            MAX("xyrxx_xzdxz")      AS "现住地",
            MAX("xyrxx_lrsj")       AS "录入时间",
            (array_agg("ajxx_join_ajxx_ay"          ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1] AS "案由",
            (array_agg("ajxx_join_ajxx_cbdw_bh_dm"  ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1] AS "办案部门编码",
            LEFT(COALESCE((array_agg("ajxx_join_ajxx_cbdw_bh_dm" ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1], ''), 6) AS "地区代码",
            MAX(w.ksws_mc)   AS "开具文书名",
            CASE WHEN BOOL_OR(is_songxue) THEN '是' ELSE '否' END AS "是否送校"
        FROM base_rows
        LEFT JOIN sx_wsmc w USING ("xyrxx_sfzh")
        GROUP BY "xyrxx_sfzh"
        {having_condition}
        ORDER BY "是否送校" DESC, "录入时间" DESC NULLS LAST, "身份证号"
        """
        params = [start_time, end_time] + type_params
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return _attach_region_fields(rows)

    leixing = _normalize_leixing_list(leixing_list)
    params: List[Any] = [start_time, end_time]
    type_filter = ""
    if leixing:
        type_filter = """
              AND EXISTS (
                  SELECT 1
                  FROM "ywdata"."case_type_config" ctc
                  WHERE ctc.leixing = ANY(%s::text[])
                    AND COALESCE(x."ajxx_join_ajxx_ay", '') SIMILAR TO ctc."ay_pattern"
              )"""
        params.append(list(leixing))

    q = f"""
        WITH sx_minor AS MATERIALIZED (
            SELECT
                x."xyrxx_sfzh",
                x."xyrxx_xm",
                x."xyrxx_hjdxzqh",
                x."xyrxx_hjdxzqh_dm",
                x."xyrxx_xzdxz",
                x."xyrxx_lrsj",
                x."ajxx_join_ajxx_ajbh",
                x."ajxx_join_ajxx_lasj",
                x."ajxx_join_ajxx_ay",
                x."ajxx_join_ajxx_cbdw_bh_dm",
                TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD') AS birthday
            FROM "ywdata"."zq_zfba_xyrxx" x
            WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
              AND x."ajxx_join_ajxx_ajlx_dm" = '02'
              AND x."xyrxx_sfzh" IS NOT NULL
              AND LENGTH(x."xyrxx_sfzh") = 18
              AND x."xyrxx_sfzh" ~ '^\\d{{17}}[\\dXx]$'
              AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^\\d{{4}}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01])$'
              AND x."xyrxx_lrsj" BETWEEN %s AND %s{type_filter}
        ),
        sx_age16 AS MATERIALIZED (
            SELECT m.*
            FROM sx_minor m
            JOIN "ywdata"."zq_zfba_ajxx" aj
              ON m."ajxx_join_ajxx_ajbh" = aj."ajxx_ajbh"
            WHERE DATE_PART('year', AGE(aj."ajxx_fasj"::DATE, m.birthday)) < 16
        ),
        ws_pre AS MATERIALIZED (
            SELECT ajbh, xgry_xm, wsmc
            FROM "ywdata"."zq_zfba_wenshu"
            WHERE wsmc ~ '不予立案通知书|撤销案件决定书|不予行政处罚决定书|提请专门'
        ),
        ws_a AS MATERIALIZED (
            SELECT DISTINCT ajbh
            FROM ws_pre
            WHERE wsmc ~ '不予立案通知书|撤销案件决定书'
        ),
        ws_b AS MATERIALIZED (
            SELECT DISTINCT ajbh, xgry_xm
            FROM ws_pre
            WHERE wsmc ~ '不予行政处罚决定书'
        ),
        ws_sx AS MATERIALIZED (
            SELECT DISTINCT ajbh, xgry_xm
            FROM ws_pre
            WHERE wsmc ~ '提请专门'
        ),
        sx_labeled AS MATERIALIZED (
            SELECT
                m."xyrxx_sfzh",
                m."xyrxx_xm",
                m."xyrxx_hjdxzqh",
                m."xyrxx_hjdxzqh_dm",
                m."xyrxx_xzdxz",
                m."xyrxx_lrsj",
                m."ajxx_join_ajxx_ajbh",
                m."ajxx_join_ajxx_lasj",
                m."ajxx_join_ajxx_ay",
                m."ajxx_join_ajxx_cbdw_bh_dm",
                EXISTS (
                    SELECT 1
                    FROM ws_sx s
                    WHERE s.ajbh    = m."ajxx_join_ajxx_ajbh"
                      AND TRIM(s.xgry_xm) = m."xyrxx_xm"
                ) AS is_songxue
            FROM sx_age16 m
            WHERE EXISTS (
                      SELECT 1 FROM ws_a a
                      WHERE a.ajbh = m."ajxx_join_ajxx_ajbh"
                  )
               OR EXISTS (
                      SELECT 1 FROM ws_b b
                      WHERE b.ajbh    = m."ajxx_join_ajxx_ajbh"
                        AND TRIM(b.xgry_xm) = m."xyrxx_xm"
                  )
        ),
        sx_wsmc AS MATERIALIZED (
            SELECT
                l."xyrxx_sfzh",
                STRING_AGG(DISTINCT p.wsmc, ';' ORDER BY p.wsmc) AS ksws_mc
            FROM sx_labeled l
            JOIN ws_pre p ON (
                (p.wsmc ~ '不予立案通知书|撤销案件决定书'
                    AND p.ajbh = l."ajxx_join_ajxx_ajbh")
                OR (p.wsmc ~ '不予行政处罚决定书'
                    AND p.ajbh    = l."ajxx_join_ajxx_ajbh"
                    AND TRIM(p.xgry_xm) = l."xyrxx_xm")
                OR (p.wsmc ~ '提请专门'
                    AND p.ajbh    = l."ajxx_join_ajxx_ajbh"
                    AND TRIM(p.xgry_xm) = l."xyrxx_xm")
            )
            GROUP BY l."xyrxx_sfzh"
        )
        SELECT
            MAX("xyrxx_xm")  AS "姓名",
            "xyrxx_sfzh"     AS "身份证号",
            MAX("xyrxx_hjdxzqh")    AS "户籍区域",
            MAX("xyrxx_hjdxzqh_dm") AS "户籍区域代码",
            MAX("xyrxx_xzdxz")      AS "现住地",
            MAX("xyrxx_lrsj")       AS "录入时间",
            (array_agg("ajxx_join_ajxx_ay"          ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1] AS "案由",
            (array_agg("ajxx_join_ajxx_cbdw_bh_dm"  ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1] AS "办案部门编码",
            LEFT(COALESCE((array_agg("ajxx_join_ajxx_cbdw_bh_dm" ORDER BY "ajxx_join_ajxx_lasj" DESC NULLS LAST))[1], ''), 6) AS "地区代码",
            MAX(w.ksws_mc)   AS "开具文书名",
            CASE WHEN BOOL_OR(is_songxue) THEN '是' ELSE '否' END AS "是否送校"
        FROM sx_labeled
        LEFT JOIN sx_wsmc w USING ("xyrxx_sfzh")
        GROUP BY "xyrxx_sfzh"
        {having_condition}
        ORDER BY "是否送校" DESC, "录入时间" DESC NULLS LAST, "身份证号"
    """

    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _fetch_sx_songjiao_den_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    return _fetch_sx_songjiao_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        require_songjiao=False,
    )


def _fetch_sx_songjiao_num_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    return _fetch_sx_songjiao_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        require_songjiao=True,
    )


def _fetch_zmjz_ratio_rows(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    use_base_view = _relation_exists(
        conn,
        schema="ywdata",
        name="v_wcnr_zmjz_ratio_base",
        relkinds=("v", "m"),
    )

    if use_base_view:
        type_condition, type_params = _build_ay_similar_filter('src."ajxx_join_ajxx_ay"', leixing_list)
        q = f"""
            SELECT
                src.*
            FROM "ywdata"."v_wcnr_zmjz_ratio_base" src
            WHERE src."xyrxx_lrsj" BETWEEN %s AND %s
              {type_condition}
            ORDER BY src."xyrxx_sfzh"
        """
        params: List[Any] = [start_time, end_time] + type_params
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return _attach_region_fields(rows)
    else:
        type_condition, type_params = _build_ay_similar_filter('v."ajxx_join_ajxx_ay"', leixing_list)
        base_cte = f"""
        wf_validated AS MATERIALIZED (
            SELECT
                x."xyrxx_sfzh",
                x."xyrxx_xm",
                x."xyrxx_xb",
                x."xyrxx_lrsj",
                x."ajxx_join_ajxx_ajbh",
                x."ajxx_join_ajxx_lasj",
                x."ajxx_join_ajxx_ajlx",
                x."ajxx_join_ajxx_ajmc",
                x."ajxx_join_ajxx_ay",
                x."xyrxx_hjdxzqh",
                x."xyrxx_hjdxzqh_dm",
                x."xyrxx_hjdxz",
                x."xyrxx_xzdxz",
                x."ajxx_join_ajxx_cbdw_bh",
                x."ajxx_join_ajxx_cbdw_bh_dm",
                x."xyrxx_rybh",
                TO_DATE(SUBSTRING(x."xyrxx_sfzh", 7, 8), 'YYYYMMDD') AS birthday
            FROM "ywdata"."zq_zfba_xyrxx" x
            WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
              AND x."xyrxx_sfzh" IS NOT NULL
              AND LENGTH(x."xyrxx_sfzh") = 18
              AND x."xyrxx_sfzh" ~ '^\\d{{17}}[\\dXx]$'
              AND SUBSTRING(x."xyrxx_sfzh", 7, 8) ~ '^\\d{{4}}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01])$'
              AND SUBSTRING(x."xyrxx_sfzh", 7, 8) >= TO_CHAR(CURRENT_DATE - INTERVAL '20 years', 'YYYYMMDD')
        ),
        xz_keys AS MATERIALIZED (
            SELECT DISTINCT
                b."ajxx_ajbh" AS ajbh,
                b."byxzcfjds_rybh" AS rybh
            FROM "ywdata"."zq_zfba_byxzcfjds" b
            WHERE COALESCE(b."byxzcfjds_cbryj", '') !~ '违法事实不能成立'

            UNION

            SELECT DISTINCT
                x."ajxx_ajbh" AS ajbh,
                x."xzcfjds_rybh" AS rybh
            FROM "ywdata"."zq_zfba_xzcfjds" x
        ),
        wf_xz_filtered AS MATERIALIZED (
            SELECT v.*
            FROM wf_validated v
            WHERE v."ajxx_join_ajxx_ajlx" = '刑事'
              AND NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_byxzcfjds" bxf
                    WHERE bxf."ajxx_ajbh" = v."ajxx_join_ajxx_ajbh"
                      AND bxf."byxzcfjds_rybh" = v."xyrxx_rybh"
                      AND COALESCE(bxf."byxzcfjds_cbryj", '') ~ '违法事实不能成立'
                )
              AND NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_wenshu" ws
                    WHERE ws."ajbh" = v."ajxx_join_ajxx_ajbh"
                      AND TRIM(ws."xgry_xm") = v."xyrxx_xm"
                )

            UNION ALL

            SELECT v.*
            FROM wf_validated v
            INNER JOIN xz_keys k
                ON k.ajbh = v."ajxx_join_ajxx_ajbh"
               AND k.rybh = v."xyrxx_rybh"
            WHERE v."ajxx_join_ajxx_ajlx" = '行政'
        ),
        base_raw AS MATERIALIZED (
            SELECT
                v."xyrxx_sfzh",
                v."xyrxx_xm",
                v."xyrxx_xb",
                v."xyrxx_lrsj",
                v."ajxx_join_ajxx_ajbh",
                v."ajxx_join_ajxx_lasj",
                v."ajxx_join_ajxx_ajlx",
                v."ajxx_join_ajxx_ajmc",
                v."ajxx_join_ajxx_ay",
                v."xyrxx_hjdxzqh",
                v."xyrxx_hjdxzqh_dm",
                v."xyrxx_hjdxz",
                v."xyrxx_xzdxz",
                v."ajxx_join_ajxx_cbdw_bh",
                v."ajxx_join_ajxx_cbdw_bh_dm",
                v."xyrxx_rybh",
                v.birthday,
                aj."ajxx_fasj",
                DATE_PART('year', AGE(aj."ajxx_fasj"::DATE, v.birthday))::INTEGER AS fasj_age,
                DATE_PART('year', AGE(CURRENT_DATE, v.birthday))::INTEGER AS current_age,
                LEFT(COALESCE(v."ajxx_join_ajxx_cbdw_bh_dm", ''), 6) AS "地区代码"
            FROM wf_xz_filtered v
            INNER JOIN "ywdata"."zq_zfba_ajxx" aj
                ON aj."ajxx_ajbh" = v."ajxx_join_ajxx_ajbh"
            WHERE DATE_PART('year', AGE(CURRENT_DATE, v.birthday)) < 18
              AND v."xyrxx_lrsj" BETWEEN %s AND %s
              {type_condition}
        ),
        """

    q = f"""
        WITH
        {base_cte}
        base AS MATERIALIZED (
            SELECT DISTINCT ON (xyrxx_sfzh)
                *
            FROM base_raw
            WHERE NULLIF(BTRIM(COALESCE(xyrxx_sfzh, '')), '') IS NOT NULL
            ORDER BY xyrxx_sfzh, ajxx_join_ajxx_lasj DESC NULLS LAST, xyrxx_lrsj DESC NULLS LAST, ajxx_join_ajxx_ajbh DESC
        ),
        base_sfzh AS MATERIALIZED (
            SELECT DISTINCT xyrxx_sfzh
            FROM base
            WHERE NULLIF(BTRIM(COALESCE(xyrxx_sfzh, '')), '') IS NOT NULL
        ),
        admin_people AS MATERIALIZED (
            SELECT DISTINCT xyrxx_sfzh
            FROM base
            WHERE ajxx_join_ajxx_ajlx = '行政'
        ),
        jlbzx_cte AS MATERIALIZED (
            SELECT DISTINCT
                b.xyrxx_sfzh,
                '是' AS "是否拘留不执行",
                '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
                '否' AS "是否3次及以上违法犯罪",
                '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
            FROM base_raw b
            INNER JOIN "ywdata"."zq_zfba_xzcfjds" xz
                ON  xz."ajxx_ajbh" = b.ajxx_join_ajxx_ajbh
                AND xz."xzcfjds_rybh" = b.xyrxx_rybh
            WHERE b.ajxx_join_ajxx_ajlx = '行政'
              AND COALESCE(xz."xzcfjds_cfzl", '') ~ '拘留'
              AND COALESCE(xz."xzcfjds_zxqk_text", '') ~ '不执行|不送'
              AND NULLIF(TRIM(xz."xzcfjds_tj_jlts"), '') IS NOT NULL
              AND CAST(xz."xzcfjds_tj_jlts" AS INTEGER) > 4
        ),
        admin_history AS MATERIALIZED (
            SELECT
                x."xyrxx_sfzh",
                x."xyrxx_xm",
                x."ajxx_join_ajxx_ajbh",
                x."ajxx_join_ajxx_lasj",
                x."ajxx_join_ajxx_ay_dm"
            FROM "ywdata"."zq_zfba_xyrxx" x
            INNER JOIN admin_people p
                ON p.xyrxx_sfzh = x."xyrxx_sfzh"
            WHERE x."ajxx_join_ajxx_isdel_dm" = '0'
              AND x."ajxx_join_ajxx_ajlx" = '行政'
        ),
        admin_agg AS MATERIALIZED (
            SELECT
                xyrxx_sfzh,
                COUNT(*) AS admin_case_cnt,
                MIN(ajxx_join_ajxx_ay_dm) AS min_ay_dm,
                MAX(ajxx_join_ajxx_ay_dm) AS max_ay_dm
            FROM admin_history
            GROUP BY xyrxx_sfzh
        ),
        admin_first_case AS MATERIALIZED (
            SELECT DISTINCT ON (xyrxx_sfzh)
                xyrxx_sfzh,
                xyrxx_xm,
                ajxx_join_ajxx_ajbh,
                ajxx_join_ajxx_lasj
            FROM admin_history
            ORDER BY xyrxx_sfzh, ajxx_join_ajxx_lasj ASC NULLS LAST, ajxx_join_ajxx_ajbh ASC
        ),
        twice_same_ay_with_doc_cte AS MATERIALIZED (
            SELECT DISTINCT
                f.xyrxx_sfzh,
                '否' AS "是否拘留不执行",
                '是' AS "是否2次违法犯罪且第一次开具了矫治文书",
                '否' AS "是否3次及以上违法犯罪",
                '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
            FROM admin_agg a
            INNER JOIN admin_first_case f
                ON f.xyrxx_sfzh = a.xyrxx_sfzh
            WHERE a.admin_case_cnt = 2
              AND a.min_ay_dm IS NOT NULL
              AND a.min_ay_dm = a.max_ay_dm
              AND (
                    EXISTS (
                        SELECT 1
                        FROM "ywdata"."zq_zfba_xjs2" xj
                        WHERE xj."ajbh" = f.ajxx_join_ajxx_ajbh
                          AND TRIM(xj."xgry_xm") = f.xyrxx_xm
                    )
                 OR EXISTS (
                        SELECT 1
                        FROM "ywdata"."zq_zfba_zlwcnrzstdxwgftzs" zl
                        WHERE zl."zltzs_ajbh" = f.ajxx_join_ajxx_ajbh
                          AND zl."zltzs_ryxm" = f.xyrxx_xm
                    )
                )
        ),
        three_plus_cte AS MATERIALIZED (
            SELECT DISTINCT
                xyrxx_sfzh,
                '否' AS "是否拘留不执行",
                '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
                '是' AS "是否3次及以上违法犯罪",
                '否' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
            FROM admin_agg
            WHERE admin_case_cnt >= 3
        ),
        criminal_no_detain_no_stop_cte AS MATERIALIZED (
            SELECT DISTINCT
                b.xyrxx_sfzh,
                '否' AS "是否拘留不执行",
                '否' AS "是否2次违法犯罪且第一次开具了矫治文书",
                '否' AS "是否3次及以上违法犯罪",
                '是' AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
            FROM base_raw b
            WHERE b.ajxx_join_ajxx_ajlx = '刑事'
              AND NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_jlz" j
                    WHERE j."ajxx_ajbh" = b.ajxx_join_ajxx_ajbh
                      AND j."jlz_rybh" = b.xyrxx_rybh
                )
              AND NOT EXISTS (
                    SELECT 1
                    FROM "ywdata"."zq_zfba_wenshu" ws
                    WHERE ws."ajbh" = b.ajxx_join_ajxx_ajbh
                      AND TRIM(ws."xgry_xm") = b.xyrxx_xm
                      AND COALESCE(ws."wsmc", '') ~ '终止侦查决定书'
                )
        ),
        all_condition_rows AS MATERIALIZED (
            SELECT * FROM jlbzx_cte
            UNION ALL
            SELECT * FROM twice_same_ay_with_doc_cte
            UNION ALL
            SELECT * FROM three_plus_cte
            UNION ALL
            SELECT * FROM criminal_no_detain_no_stop_cte
        ),
        person_flags AS MATERIALIZED (
            SELECT
                xyrxx_sfzh,
                MAX("是否拘留不执行") AS "是否拘留不执行",
                MAX("是否2次违法犯罪且第一次开具了矫治文书") AS "是否2次违法犯罪且第一次开具了矫治文书",
                MAX("是否3次及以上违法犯罪") AS "是否3次及以上违法犯罪",
                MAX("是否犯罪且未刑拘且未开具《终止侦查决定书》") AS "是否犯罪且未刑拘且未开具《终止侦查决定书》"
            FROM all_condition_rows
            GROUP BY xyrxx_sfzh
        ),
        qualified_people AS MATERIALIZED (
            SELECT
                xyrxx_sfzh,
                '是' AS "是否符合专门(矫治)教育"
            FROM person_flags
        ),
        qualified_latest_case AS MATERIALIZED (
            SELECT
                b."xyrxx_sfzh",
                b."xyrxx_xm",
                b."ajxx_join_ajxx_ajbh"
            FROM base b
            INNER JOIN qualified_people q
                ON q.xyrxx_sfzh = b."xyrxx_sfzh"
        ),
        tqzmjy_people AS MATERIALIZED (
            SELECT DISTINCT
                q.xyrxx_sfzh,
                '是' AS "是否开具专门(矫治)教育申请书"
            FROM qualified_latest_case q
            INNER JOIN "ywdata"."zq_zfba_tqzmjy" t
                ON  t."ajbh" = q."ajxx_join_ajxx_ajbh"
                AND TRIM(t."xgry_xm") = q."xyrxx_xm"
        ),
        final_flags AS MATERIALIZED (
            SELECT
                s.xyrxx_sfzh,
                COALESCE(p."是否拘留不执行", '否') AS "是否拘留不执行",
                COALESCE(p."是否2次违法犯罪且第一次开具了矫治文书", '否') AS "是否2次违法犯罪且第一次开具了矫治文书",
                COALESCE(p."是否3次及以上违法犯罪", '否') AS "是否3次及以上违法犯罪",
                COALESCE(p."是否犯罪且未刑拘且未开具《终止侦查决定书》", '否') AS "是否犯罪且未刑拘且未开具《终止侦查决定书》",
                CASE WHEN q.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否' END AS "是否符合专门(矫治)教育",
                CASE WHEN tq.xyrxx_sfzh IS NOT NULL THEN '是' ELSE '否' END AS "是否开具专门(矫治)教育申请书"
            FROM base_sfzh s
            LEFT JOIN person_flags p
                ON p.xyrxx_sfzh = s.xyrxx_sfzh
            LEFT JOIN qualified_people q
                ON q.xyrxx_sfzh = s.xyrxx_sfzh
            LEFT JOIN tqzmjy_people tq
                ON tq.xyrxx_sfzh = s.xyrxx_sfzh
        )
        SELECT
            b.*,
            ff."是否拘留不执行",
            ff."是否2次违法犯罪且第一次开具了矫治文书",
            ff."是否3次及以上违法犯罪",
            ff."是否犯罪且未刑拘且未开具《终止侦查决定书》",
            ff."是否符合专门(矫治)教育",
            ff."是否开具专门(矫治)教育申请书"
        FROM base b
        LEFT JOIN final_flags ff
            ON ff.xyrxx_sfzh = b.xyrxx_sfzh
        ORDER BY b.xyrxx_sfzh
    """

    params: List[Any] = [start_time, end_time] + type_params
    with conn.cursor() as cur:
        cur.execute(q, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return _attach_region_fields(rows)


def _load_detail_rows(conn, *, start_time: str, end_time: str, leixing_list: Sequence[str], metric: str) -> List[Dict[str, Any]]:
    if metric in ("警情", "转案数", "行政", "刑事"):
        rows, _ = zfba_wcnr_jqaj_dao.fetch_detail_rows(
            conn,
            metric=metric,
            diqu="__ALL__",
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=[],
            limit=0,
        )
        return _attach_region_fields(rows)

    if metric == "警情(场所)":
        rows, _ = zfba_wcnr_jqaj_dao.fetch_detail_rows(
            conn,
            metric="警情",
            diqu="__ALL__",
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=[],
            limit=0,
        )
        rows = _attach_region_fields(rows)
        return _filter_rows_by_place_keywords(rows, field_names=("处警情况",))

    if metric == "案件(场所)":
        leixing = _normalize_leixing_for_query(conn, leixing_list)
        patterns = zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing)
        if leixing and not patterns:
            return []
        rows = zfba_wcnr_jqaj_dao.fetch_wcnr_ajxx_changsuo_base_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            patterns=patterns,
            diqu=None,
        )
        rows = _attach_region_fields(rows)
        return _filter_rows_by_place_keywords(rows, field_names=("简要案情",))

    if metric in ("案件数(被侵害)", "刑事(未成年人)"):
        m = "案件数(被侵害)" if metric == "案件数(被侵害)" else "刑事"
        rows, _ = zfba_wcnr_jqaj_dao.fetch_detail_rows(
            conn,
            metric=m,
            diqu="__ALL__",
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=[],
            limit=0,
        )
        return _attach_region_fields(rows)

    return []


def fetch_metric_detail_rows(
    conn,
    *,
    metric: str,
    part: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    """
    仅查询指定指标所需的最小数据，返回明细行列表。
    part 应已通过 _normalize_part 标准化（"value" | "numerator" | "denominator"）。
    相比 fetch_period_data(include_details=True) 全量查询，大幅减少无关查询。
    """
    leixing = _normalize_leixing_for_query(conn, leixing_list)

    def _patterns_and_empty() -> Tuple[List[str], bool]:
        pats = zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing)
        return pats, bool(leixing) and not pats

    # ── 1. 警情 ────────────────────────────────────────────────────────────
    if metric == "jq":
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="警情")
        return normalize_rows_for_output(rows)

    if metric == "jq_changsuo":
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="警情(场所)")
        return normalize_rows_for_output(rows)

    # ── 2. 转案率 ───────────────────────────────────────────────────────────
    if metric == "za_rate":
        m = "警情" if part == "denominator" else "转案数"
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric=m)
        return normalize_rows_for_output(rows)

    # ── 3. 行政 ────────────────────────────────────────────────────────────
    if metric == "xingzheng":
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="行政")
        return normalize_rows_for_output(rows)

    # ── 4. 刑事 / 刑事占比 ─────────────────────────────────────────────────
    if metric == "xingshi":
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="刑事")
        return normalize_rows_for_output(rows)

    if metric == "aj_changsuo":
        _, empty = _patterns_and_empty()
        if empty:
            return []
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="案件(场所)")
        return normalize_rows_for_output(rows)

    # 刑事占比：分子取 wcnr 刑事，分母取 jq_aj 总刑事
    if metric == "xingshi_ratio":
        if part == "denominator":
            _, empty = _patterns_and_empty()
            if empty:
                return []
            _rows, _ = zfba_jq_aj_dao.fetch_detail_rows(
                conn,
                metric="刑事",
                diqu="__ALL__",
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                za_types=[],
                limit=0,
            )
            return normalize_rows_for_output(_attach_region_fields(_rows))
        rows = _load_detail_rows(conn, start_time=start_time, end_time=end_time,
                                 leixing_list=leixing, metric="刑事")
        return normalize_rows_for_output(rows)

    # ── 5. 被侵害案件 / 场所被侵害案件 ─────────────────────────────────────
    if metric in ("bqh_case", "cs_bqh_case"):
        patterns, empty = _patterns_and_empty()
        if empty:
            return []
        bqh_base_rows = zfba_wcnr_jqaj_dao.fetch_wcnr_shr_ajxx_base_rows(
            conn, start_time=start_time, end_time=end_time, patterns=patterns, diqu=None
        )
        bqh_rows = _attach_region_fields(bqh_base_rows)
        if metric == "bqh_case":
            return normalize_rows_for_output(bqh_rows)
        cs_rows = _filter_changsuo_bqh_rows(bqh_rows)
        return normalize_rows_for_output(cs_rows)

    # ── 6. 违法犯罪人员（v_wcnr_wfry_jbxx，按录入时间/案由筛选）──────────
    if metric == "wfzf_people":
        rows = _fetch_wfzf_people_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
        )
        return normalize_rows_for_output(rows)

    # ── 6b. 涉刑人员送生占比 ───────────────────────────────────────────────
    if metric == "sx_songjiao_ratio":
        if part == "denominator":
            rows = _fetch_sx_songjiao_den_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
            )
        else:
            rows = _fetch_sx_songjiao_num_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
            )
        return normalize_rows_for_output(rows)

    # ── 6c. 严重不良占比 / 专门矫治占比 ────────────────────────────────────
    if metric == "yzbl_ratio":
        yzbl_rows = _fetch_yzbl_ratio_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
        )
        den_rows = [r for r in yzbl_rows if _is_yes(r.get("是否应采取矫治教育措施"))]
        rows = den_rows if part == "denominator" else [r for r in den_rows if _is_yes(r.get("是否开具矫治文书"))]
        return normalize_rows_for_output(rows)

    if metric == "zmjz_ratio":
        _, empty = _patterns_and_empty()
        if empty:
            return []
        zmjz_rows = _fetch_zmjz_ratio_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
        )
        if part == "denominator":
            rows = [r for r in zmjz_rows if _is_zmjz_ratio_den_row(r)]
        else:
            rows = [r for r in zmjz_rows if _is_zmjz_ratio_num_row(r)]
        return normalize_rows_for_output(rows)

    # ── 7. 专门教育学生结业后再犯数 ────────────────────────────────────────
    if metric == "zmy_reoff":
        _, empty = _patterns_and_empty()
        if empty:
            return []
        if part == "denominator":
            rows = _fetch_graduates(conn, start_time=start_time, end_time=end_time,
                                    leixing_list=leixing, jz_time_lt6=True)
        else:
            rows = _fetch_graduate_reoffend(conn, start_time=start_time, end_time=end_time,
                                            leixing_list=leixing, jz_time_lt6=True,
                                            xingshi_only=True, minor_only=True)
        return normalize_rows_for_output(_attach_region_fields(rows))

    # ── 8. 专门（矫治）教育学生结业后再犯数 ────────────────────────────────
    if metric == "zmjz_reoff":
        _, empty = _patterns_and_empty()
        if empty:
            return []
        if part == "denominator":
            rows = _fetch_graduates(conn, start_time=start_time, end_time=end_time,
                                    leixing_list=leixing, jz_time_lt6=False)
        else:
            rows = _fetch_graduate_reoffend(conn, start_time=start_time, end_time=end_time,
                                            leixing_list=leixing, jz_time_lt6=False,
                                            xingshi_only=False, minor_only=False)
        return normalize_rows_for_output(_attach_region_fields(rows))

    # ── 9. 纳管人员再犯占比 ────────────────────────────────────────────────
    if metric == "naguan_ratio":
        if part == "denominator":
            rows = _fetch_naguan_base(conn)
            return normalize_rows_for_output(_attach_region_fields(rows))
        _, empty = _patterns_and_empty()
        if empty:
            return []
        rows = _fetch_naguan_reoffend(conn, start_time=start_time, end_time=end_time,
                                      leixing_list=leixing)
        return normalize_rows_for_output(_attach_region_fields(rows))

    # ── 10. 责令加强监护数 ─────────────────────────────────────────────────
    if metric == "zljiaqjh":
        rows = _fetch_zljiaqjh_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
        )
        return normalize_rows_for_output(rows)

    return []


def fetch_period_data(
    conn,
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    include_details: bool,
    include_perf: bool = False,
) -> Dict[str, Any]:
    t_all = time.perf_counter()
    perf: Dict[str, float] = {}

    def _mark(name: str, t0: float) -> None:
        perf[name] = round((time.perf_counter() - t0) * 1000, 2)

    leixing = _normalize_leixing_for_query(conn, leixing_list)
    t = time.perf_counter()
    patterns = zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing)
    _mark("fetch_patterns_ms", t)
    typed_patterns_empty = bool(leixing) and not patterns

    counts: Dict[str, Dict[str, int]] = {}
    details: Dict[str, List[Dict[str, Any]]] = {}
    flags = {"addr_model_degraded": False}
    bqh_rows: List[Dict[str, Any]] = []
    cs_bqh_rows: List[Dict[str, Any]] = []
    jq_changsuo_rows: List[Dict[str, Any]] = []
    aj_changsuo_rows: List[Dict[str, Any]] = []

    t = time.perf_counter()
    jq_counts = zfba_wcnr_jqaj_dao.count_jq_by_diqu(
        conn, start_time=start_time, end_time=end_time, leixing_list=leixing
    )
    za_counts = zfba_wcnr_jqaj_dao.count_zhuanan_by_diqu(
        conn, start_time=start_time, end_time=end_time, leixing_list=leixing
    )
    _mark("count_jq_za_ms", t)
    counts["jq"] = _normalize_count_map(jq_counts)
    counts["zhuanan"] = _normalize_count_map(za_counts)

    t = time.perf_counter()
    jq_changsuo_rows = _load_detail_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        metric="警情(场所)",
    )
    aj_changsuo_rows = _load_detail_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        metric="案件(场所)",
    )
    counts["jq_changsuo"] = _count_rows_by_region(jq_changsuo_rows)
    counts["aj_changsuo"] = _count_rows_by_region(aj_changsuo_rows)
    _mark("place_detail_ms", t)

    if typed_patterns_empty:
        counts["xingzheng"] = _normalize_count_map({})
        counts["xingshi"] = _normalize_count_map({})
        counts["bqh_case"] = _normalize_count_map({})
        counts["cs_bqh_case"] = _normalize_count_map({})
        counts["wcnr_xingshi"] = _normalize_count_map({})
        counts["jqaj_xingshi"] = _normalize_count_map({})
    else:
        t = time.perf_counter()
        ajxx = zfba_wcnr_jqaj_dao.count_wcnr_ajxx_by_diqu_and_ajlx(
            conn,
            start_time=start_time,
            end_time=end_time,
            patterns=patterns,
        )
        counts["xingzheng"] = _normalize_count_map(ajxx.get("行政", {}))
        counts["xingshi"] = _normalize_count_map(ajxx.get("刑事", {}))
        counts["wcnr_xingshi"] = _normalize_count_map(ajxx.get("刑事", {}))
        jqaj_ajxx = zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(
            conn,
            start_time=start_time,
            end_time=end_time,
            patterns=patterns,
        )
        counts["jqaj_xingshi"] = _normalize_count_map(jqaj_ajxx.get("刑事", {}))
        _mark("count_ajxx_ms", t)

        if include_details:
            t = time.perf_counter()
            bqh_base_rows = zfba_wcnr_jqaj_dao.fetch_wcnr_shr_ajxx_base_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
                diqu=None,
            )
            bqh_rows = _attach_region_fields(bqh_base_rows)
            counts["bqh_case"] = _count_rows_by_region(bqh_rows)
            cs_bqh_rows = _filter_changsuo_bqh_rows(bqh_rows)
            counts["cs_bqh_case"] = _count_rows_by_region(cs_bqh_rows)
            _mark("bqh_and_changsuo_detail_ms", t)
        else:
            t = time.perf_counter()
            bqh_counts = zfba_wcnr_jqaj_dao.count_wcnr_shr_ajxx_by_diqu(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
            )
            counts["bqh_case"] = _normalize_count_map(bqh_counts)
            cs_base_rows = zfba_wcnr_jqaj_dao.fetch_wcnr_shr_ajxx_base_rows(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
                diqu=None,
            )
            cs_rows = _filter_changsuo_bqh_rows(_attach_region_fields(cs_base_rows))
            counts["cs_bqh_case"] = _count_rows_by_region(cs_rows)
            _mark("bqh_and_changsuo_summary_ms", t)

    wfzf_rows: List[Dict[str, Any]] = []
    sx_songjiao_den_rows: List[Dict[str, Any]] = []
    sx_songjiao_num_rows: List[Dict[str, Any]] = []
    yzbl_den_rows: List[Dict[str, Any]] = []
    yzbl_num_rows: List[Dict[str, Any]] = []
    zmjz_cover_num_rows: List[Dict[str, Any]] = []
    zmjz_cover_den_rows: List[Dict[str, Any]] = []
    if typed_patterns_empty:
        counts["zmjz_cover_num"] = _normalize_count_map({})
        counts["zmjz_cover_den"] = _normalize_count_map({})
    else:
        t = time.perf_counter()
        zmjz_ratio_rows = _fetch_zmjz_ratio_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
        )
        zmjz_cover_den_rows = [r for r in zmjz_ratio_rows if _is_zmjz_ratio_den_row(r)]
        zmjz_cover_num_rows = [r for r in zmjz_cover_den_rows if _is_zmjz_ratio_num_row(r)]
        counts["zmjz_cover_num"] = _count_rows_by_region(zmjz_cover_num_rows)
        counts["zmjz_cover_den"] = _count_rows_by_region(zmjz_cover_den_rows)
        _mark("zmjz_ratio_rows_ms", t)

    t = time.perf_counter()
    wfzf_rows = _fetch_wfzf_people_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
    )
    counts["wfzf_people"] = _count_rows_by_region(wfzf_rows)

    yzbl_den_rows = _fetch_yzbl_ratio_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
    )
    yzbl_den_rows = [r for r in yzbl_den_rows if _is_yes(r.get("是否应采取矫治教育措施"))]
    yzbl_num_rows = [r for r in yzbl_den_rows if _is_yes(r.get("是否开具矫治文书"))]
    counts["yzbl_num"] = _count_rows_by_region(yzbl_num_rows)
    counts["yzbl_den"] = _count_rows_by_region(yzbl_den_rows)

    sx_songjiao_den_rows = _fetch_sx_songjiao_den_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
    )
    sx_songjiao_num_rows = _fetch_sx_songjiao_num_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
    )
    counts["sx_songjiao_den"] = _count_rows_by_region(sx_songjiao_den_rows)
    counts["sx_songjiao_num"] = _count_rows_by_region(sx_songjiao_num_rows)
    _mark("wfzf_yzbl_sx_ms", t)

    zmy_den_rows: List[Dict[str, Any]] = []
    zmy_num_rows: List[Dict[str, Any]] = []
    zmjz_den_rows: List[Dict[str, Any]] = []
    zmjz_num_rows: List[Dict[str, Any]] = []
    if typed_patterns_empty:
        counts["zmy_den"] = _normalize_count_map({})
        counts["zmy_num"] = _normalize_count_map({})
        counts["zmjz_den"] = _normalize_count_map({})
        counts["zmjz_num"] = _normalize_count_map({})
    else:
        if include_details:
            t = time.perf_counter()
            zmy_den_rows = _fetch_graduates(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=True,
            )
            zmy_num_rows = _fetch_graduate_reoffend(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=True,
                xingshi_only=True,
                minor_only=True,
            )
            zmjz_den_rows = _fetch_graduates(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=False,
            )
            zmjz_num_rows = _fetch_graduate_reoffend(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=False,
                xingshi_only=False,
                minor_only=False,
            )
            counts["zmy_den"] = _count_rows_by_region(zmy_den_rows)
            counts["zmy_num"] = _count_distinct_by_region(zmy_num_rows, id_key="证件号码")
            counts["zmjz_den"] = _count_rows_by_region(zmjz_den_rows)
            counts["zmjz_num"] = _count_distinct_by_region(zmjz_num_rows, id_key="证件号码")
            _mark("graduate_detail_ms", t)
        else:
            t = time.perf_counter()
            counts["zmy_den"] = _count_graduates_by_region(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=True,
            )
            counts["zmy_num"] = _count_graduate_reoffend_by_region(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=True,
                xingshi_only=True,
                minor_only=True,
            )
            counts["zmjz_den"] = _count_graduates_by_region(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=False,
            )
            counts["zmjz_num"] = _count_graduate_reoffend_by_region(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
                jz_time_lt6=False,
                xingshi_only=False,
                minor_only=False,
            )
            _mark("graduate_summary_ms", t)

    naguan_den_rows: List[Dict[str, Any]] = []
    naguan_num_rows: List[Dict[str, Any]] = []
    if include_details:
        naguan_den_rows = _fetch_naguan_base(conn)
        counts["naguan_den"] = _count_rows_by_region(naguan_den_rows)
    else:
        counts["naguan_den"] = _count_naguan_base_by_region(conn)
    if typed_patterns_empty:
        counts["naguan_num"] = _normalize_count_map({})
    else:
        if include_details:
            t = time.perf_counter()
            naguan_num_rows = _fetch_naguan_reoffend(
                conn,
                start_time=start_time,
                end_time=end_time,
                leixing_list=leixing,
            )
            counts["naguan_num"] = _count_distinct_by_region(naguan_num_rows, id_key="证件号码")
            _mark("naguan_detail_ms", t)
        else:
            t = time.perf_counter()
            counts["naguan_num"] = _count_naguan_reoffend_by_region(
                conn,
                start_time=start_time,
                end_time=end_time,
                patterns=patterns,
            )
            _mark("naguan_summary_ms", t)

    # 责令加强监护数
    t = time.perf_counter()
    zljiaqjh_detail_rows = _fetch_zljiaqjh_detail_rows(
        conn,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
    )
    counts["zljiaqjh_num"] = _sum_field_by_region(
        zljiaqjh_detail_rows,
        value_key="已责令加强监护数",
    )
    counts["zljiaqjh_den"] = _sum_field_by_region(
        zljiaqjh_detail_rows,
        value_key="应责令加强监护数",
    )
    _mark("zljiaqjh_ms", t)

    if include_details:
        t = time.perf_counter()
        details["jq:value"] = _load_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            metric="警情",
        )
        details["jq_changsuo:value"] = jq_changsuo_rows
        details["za_rate:numerator"] = _load_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            metric="转案数",
        )
        details["za_rate:denominator"] = details["jq:value"]

        details["xingzheng:value"] = _load_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            metric="行政",
        )
        details["xingshi:value"] = _load_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            metric="刑事",
        )

        details["bqh_case:value"] = bqh_rows
        details["aj_changsuo:value"] = aj_changsuo_rows
        details["wfzf_people:value"] = wfzf_rows
        details["cs_bqh_case:value"] = cs_bqh_rows

        details["zmy_reoff:numerator"] = zmy_num_rows
        details["zmy_reoff:denominator"] = zmy_den_rows

        details["zmjz_reoff:numerator"] = zmjz_num_rows
        details["zmjz_reoff:denominator"] = zmjz_den_rows

        details["xingshi_ratio:numerator"] = _load_detail_rows(
            conn,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            metric="刑事",
        )
        _jqaj_xingshi_rows, _ = zfba_jq_aj_dao.fetch_detail_rows(
            conn,
            metric="刑事",
            diqu="__ALL__",
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            za_types=[],
            limit=0,
        )
        details["xingshi_ratio:denominator"] = _attach_region_fields(_jqaj_xingshi_rows)

        details["yzbl_ratio:numerator"] = yzbl_num_rows
        details["yzbl_ratio:denominator"] = yzbl_den_rows

        details["sx_songjiao_ratio:numerator"] = sx_songjiao_num_rows
        details["sx_songjiao_ratio:denominator"] = sx_songjiao_den_rows

        details["zmjz_ratio:numerator"] = zmjz_cover_num_rows
        details["zmjz_ratio:denominator"] = zmjz_cover_den_rows

        details["naguan_ratio:numerator"] = naguan_num_rows
        details["naguan_ratio:denominator"] = naguan_den_rows

        details["zljiaqjh:numerator"] = zljiaqjh_detail_rows
        details["zljiaqjh:denominator"] = zljiaqjh_detail_rows
        _mark("assemble_details_ms", t)

    perf["total_ms"] = round((time.perf_counter() - t_all) * 1000, 2)

    result = {
        "counts": counts,
        "details": details,
        "flags": flags,
    }
    if include_perf:
        result["perf"] = perf
    return result


def select_detail_rows(period_data: Dict[str, Any], *, metric: str, part: str) -> List[Dict[str, Any]]:
    metric_key = str(metric or "").strip()
    part_key = str(part or "").strip().lower() or "value"
    key = f"{metric_key}:{part_key}"
    details = period_data.get("details") or {}
    rows = details.get(key)
    if rows is None and part_key != "value":
        rows = details.get(f"{metric_key}:value")
    if rows is None:
        rows = []
    return normalize_rows_for_output(_attach_region_fields(rows))


def filter_rows_by_diqu(rows: Sequence[Dict[str, Any]], diqu: str) -> List[Dict[str, Any]]:
    target = str(diqu or "").strip()
    if not target or target in ("all", "__ALL__", "ALL", "全市"):
        return list(rows or [])

    code = _extract_region_code(target)
    if not code:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if _extract_region_code(row.get("地区代码") or row.get("地区")) == code:
            out.append(dict(row))
    return out
