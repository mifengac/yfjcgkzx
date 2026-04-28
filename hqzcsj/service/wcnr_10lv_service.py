from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import wcnr_10lv_dao, wcnr_case_list_dao
from hqzcsj.service.stats_common import (
    calc_percent_text,
    calc_ratio_text,
    fmt_dt,
    normalize_text_list,
    parse_dt,
    shift_year,
)


REGION_ORDER: List[Tuple[str, str]] = [
    ("445300", "市局"),
    ("445302", "云城"),
    ("445303", "云安"),
    ("445381", "罗定"),
    ("445321", "新兴"),
    ("445322", "郁南"),
    ("__ALL__", "全市"),
]

COUNT_METRICS: List[Dict[str, str]] = [
    {"label": "警情", "key": "jq", "unit": "起"},
    {"label": "警情(场所)", "key": "jq_changsuo", "unit": "起"},
    {"label": "行政", "key": "xingzheng", "unit": "起"},
    {"label": "刑事", "key": "xingshi", "unit": "起"},
    {"label": "案件(被侵害)", "key": "bqh_case", "unit": "起"},
    {"label": "违法犯罪人员", "key": "wfzf_people", "unit": "人"},
    {"label": "案件(场所)", "key": "aj_changsuo", "unit": "起"},
    {"label": "案件(场所被侵害)", "key": "cs_bqh_case", "unit": "起"},
]

RATE_METRICS: List[Dict[str, str]] = [
    {"key": "za_rate", "label": "转案率", "num_key": "zhuanan", "den_key": "jq"},
]

COMPOSITE_METRICS: List[Dict[str, str]] = [
    {
        "key": "zmy_reoff",
        "label": "专门教育学生结业后犯罪数",
        "rate_label": "专门教育学生结业后再犯率",
        "num_key": "zmy_num",
        "den_key": "zmy_den",
    },
    {
        "key": "zmjz_reoff",
        "label": "专门(矫治)教育学生结业后再犯数",
        "rate_label": "专门(矫治)教育学生结业后再犯率",
        "num_key": "zmjz_num",
        "den_key": "zmjz_den",
    },
    {
        "key": "xingshi_ratio",
        "label": "刑事占比",
        "rate_label": "刑事占比率",
        "num_key": "wcnr_xingshi",
        "den_key": "jqaj_xingshi",
    },
    {
        "key": "yzbl_ratio",
        "label": "严重不良未成年人矫治教育占比",
        "rate_label": "严重不良未成年人矫治教育占比率",
        "num_key": "yzbl_num",
        "den_key": "yzbl_den",
    },
    {
        "key": "sx_songjiao_ratio",
        "label": "涉刑人员送生占比",
        "rate_label": "涉刑人员送矫率",
        "num_key": "sx_songjiao_num",
        "den_key": "sx_songjiao_den",
    },
    {
        "key": "zmjz_ratio",
        "label": "专门(矫治)教育占比",
        "rate_label": "专门(矫治)教育占比率",
        "num_key": "zmjz_cover_num",
        "den_key": "zmjz_cover_den",
    },
    {
        "key": "naguan_ratio",
        "label": "纳管人员再犯占比",
        "rate_label": "纳管人员再犯率",
        "num_key": "naguan_num",
        "den_key": "naguan_den",
    },
    {
        "key": "zljiaqjh",
        "label": "责令加强监护数",
        "rate_label": "责令加强监护率",
        "num_key": "zljiaqjh_num",
        "den_key": "zljiaqjh_den",
    },
]
COUNT_METRIC_MAP: Dict[str, Dict[str, str]] = {item["key"]: item for item in COUNT_METRICS}
RATE_METRIC_MAP: Dict[str, Dict[str, str]] = {item["key"]: item for item in RATE_METRICS}
COMPOSITE_METRIC_MAP: Dict[str, Dict[str, str]] = {item["key"]: item for item in COMPOSITE_METRICS}
SUMMARY_LAYOUT: List[Tuple[str, str]] = [
    ("count", "jq"),
    ("rate", "za_rate"),
    ("count", "jq_changsuo"),
    ("count", "xingzheng"),
    ("count", "xingshi"),
    ("count", "bqh_case"),
    ("count", "wfzf_people"),
    ("count", "aj_changsuo"),
    ("composite", "zmy_reoff"),
    ("composite", "zmjz_reoff"),
    ("count", "cs_bqh_case"),
    ("composite", "xingshi_ratio"),
    ("composite", "yzbl_ratio"),
    ("composite", "sx_songjiao_ratio"),
    ("composite", "zmjz_ratio"),
    ("composite", "naguan_ratio"),
    ("composite", "zljiaqjh"),
]
DETAIL_METRIC_LABEL: Dict[str, str] = {
    **{item["key"]: item["label"] for item in COUNT_METRICS},
    **{item["key"]: item["label"] for item in RATE_METRICS},
    **{item["key"]: item["label"] for item in COMPOSITE_METRICS},
}
COMPOSITE_METRIC_KEYS = set(COMPOSITE_METRIC_MAP)
EXPORT_METRIC_ORDER: List[str] = [metric_key for _metric_kind, metric_key in SUMMARY_LAYOUT]
CAMPUS_BULLYING_EXPORT_COLUMNS: List[str] = [
    "警情编号",
    "报警时间",
    "管辖单位",
    "分局",
    "报警内容",
    "处警情况",
    "警情地址",
    "案件编号",
    "案件名称",
    "立案时间",
]


def default_time_range_for_page() -> Tuple[str, str, str, str]:
    now = datetime.now()
    today0 = datetime(now.year, now.month, now.day, 0, 0, 0)
    start = today0 - timedelta(days=7)
    hb_start = today0 - timedelta(days=14)
    hb_end = today0 - timedelta(days=7)
    return (
        start.strftime("%Y-%m-%d 00:00:00"),
        today0.strftime("%Y-%m-%d 00:00:00"),
        hb_start.strftime("%Y-%m-%d 00:00:00"),
        hb_end.strftime("%Y-%m-%d 00:00:00"),
    )


def _normalize_leixing_list(leixing_list: Sequence[str]) -> List[str]:
    return normalize_text_list(leixing_list)


def _first_non_empty(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        return value
    return ""


def _format_datetime_for_export(value: Any) -> Any:
    if isinstance(value, datetime):
        return fmt_dt(value)
    return value or ""


def build_campus_bullying_export_title(*, start_time: str, end_time: str) -> str:
    ranges = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=None,
        hb_end_time=None,
    )
    return f"{ranges['start_time']}至{ranges['end_time']}校园欺凌警情案件"


def _count(period_data: Dict[str, Any], key: str, code: str) -> int:
    counts = (period_data.get("counts") or {}).get(key) or {}
    try:
        return int(counts.get(code) or 0)
    except Exception:
        return 0


def _build_ranges(
    *,
    start_time: str,
    end_time: str,
    hb_start_time: str | None,
    hb_end_time: str | None,
) -> Dict[str, str]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    yoy_start = shift_year(start_dt, -1)
    yoy_end = shift_year(end_dt, -1)

    if hb_start_time and hb_end_time:
        hb_start_dt = parse_dt(hb_start_time)
        hb_end_dt = parse_dt(hb_end_time)
    else:
        hb_start_dt = start_dt - timedelta(days=7)
        hb_end_dt = end_dt - timedelta(days=7)

    if hb_end_dt < hb_start_dt:
        raise ValueError("环比结束时间不能早于环比开始时间")

    return {
        "start_time": fmt_dt(start_dt),
        "end_time": fmt_dt(end_dt),
        "yoy_start_time": fmt_dt(yoy_start),
        "yoy_end_time": fmt_dt(yoy_end),
        "hb_start_time": fmt_dt(hb_start_dt),
        "hb_end_time": fmt_dt(hb_end_dt),
    }


def build_summary(
    *,
    start_time: str,
    end_time: str,
    hb_start_time: str | None,
    hb_end_time: str | None,
    leixing_list: Sequence[str],
    include_hb: bool = True,
    include_perf: bool = False,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    t_all = time.perf_counter()
    perf: Dict[str, Any] = {}
    ranges = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=hb_start_time,
        hb_end_time=hb_end_time,
    )
    leixing = _normalize_leixing_list(leixing_list)

    conn = get_database_connection()
    try:
        t = time.perf_counter()
        current_data = wcnr_10lv_dao.fetch_period_data(
            conn,
            start_time=ranges["start_time"],
            end_time=ranges["end_time"],
            leixing_list=leixing,
            include_details=False,
            include_perf=include_perf,
        )
        perf["current_ms"] = round((time.perf_counter() - t) * 1000, 2)
        t = time.perf_counter()
        yoy_data = wcnr_10lv_dao.fetch_period_data(
            conn,
            start_time=ranges["yoy_start_time"],
            end_time=ranges["yoy_end_time"],
            leixing_list=leixing,
            include_details=False,
            include_perf=include_perf,
        )
        perf["yoy_ms"] = round((time.perf_counter() - t) * 1000, 2)
        hb_data: Dict[str, Any] = {"counts": {}, "flags": {}}
        if include_hb:
            t = time.perf_counter()
            hb_data = wcnr_10lv_dao.fetch_period_data(
                conn,
                start_time=ranges["hb_start_time"],
                end_time=ranges["hb_end_time"],
                leixing_list=leixing,
                include_details=False,
                include_perf=include_perf,
            )
            perf["hb_ms"] = round((time.perf_counter() - t) * 1000, 2)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    rows: List[Dict[str, Any]] = []

    def add_count_metric(row: Dict[str, Any], *, code: str, label: str, key: str, unit: str) -> None:
        cur = _count(current_data, key, code)
        yoy = _count(yoy_data, key, code)
        hb = _count(hb_data, key, code)
        row[label] = cur
        row[f"同比{label}"] = yoy
        row[f"环比{label}"] = hb
        row[f"同比{label}比例"] = calc_ratio_text(cur, yoy, unit)
        row[f"环比{label}比例"] = calc_ratio_text(cur, hb, unit)

    def add_composite_metric(
        row: Dict[str, Any],
        *,
        code: str,
        label: str,
        rate_label: str,
        num_key: str,
        den_key: str,
    ) -> None:
        cur_num = _count(current_data, num_key, code)
        cur_den = _count(current_data, den_key, code)
        yoy_num = _count(yoy_data, num_key, code)
        yoy_den = _count(yoy_data, den_key, code)
        hb_num = _count(hb_data, num_key, code)
        hb_den = _count(hb_data, den_key, code)

        row[label] = f"{cur_num}/{cur_den}"
        row[f"同比{label}"] = f"{yoy_num}/{yoy_den}"
        row[f"环比{label}"] = f"{hb_num}/{hb_den}"

        row[rate_label] = calc_percent_text(cur_num, cur_den)
        row[f"同比{rate_label}"] = calc_percent_text(yoy_num, yoy_den)
        row[f"环比{rate_label}"] = calc_percent_text(hb_num, hb_den)

    def add_rate_metric(
        row: Dict[str, Any],
        *,
        code: str,
        label: str,
        num_key: str,
        den_key: str,
    ) -> None:
        cur_num = _count(current_data, num_key, code)
        cur_den = _count(current_data, den_key, code)
        yoy_num = _count(yoy_data, num_key, code)
        yoy_den = _count(yoy_data, den_key, code)
        hb_num = _count(hb_data, num_key, code)
        hb_den = _count(hb_data, den_key, code)

        row[label] = calc_percent_text(cur_num, cur_den)
        row[f"同比{label}"] = calc_percent_text(yoy_num, yoy_den)
        row[f"环比{label}"] = calc_percent_text(hb_num, hb_den)

    for code, name in REGION_ORDER:
        row: Dict[str, Any] = {"地区": name, "地区代码": code}

        for metric_kind, metric_key in SUMMARY_LAYOUT:
            if metric_kind == "count":
                metric = COUNT_METRIC_MAP[metric_key]
                add_count_metric(
                    row,
                    code=code,
                    label=metric["label"],
                    key=metric["key"],
                    unit=metric["unit"],
                )
                continue
            if metric_kind == "rate":
                metric = RATE_METRIC_MAP[metric_key]
                add_rate_metric(
                    row,
                    code=code,
                    label=metric["label"],
                    num_key=metric["num_key"],
                    den_key=metric["den_key"],
                )
                continue

            metric = COMPOSITE_METRIC_MAP[metric_key]
            add_composite_metric(
                row,
                code=code,
                label=metric["label"],
                rate_label=metric["rate_label"],
                num_key=metric["num_key"],
                den_key=metric["den_key"],
            )

        rows.append(row)

    meta: Dict[str, Any] = {
        **ranges,
        "flags": {
            "addr_model_degraded_current": bool((current_data.get("flags") or {}).get("addr_model_degraded")),
            "addr_model_degraded_yoy": bool((yoy_data.get("flags") or {}).get("addr_model_degraded")),
            "addr_model_degraded_hb": bool((hb_data.get("flags") or {}).get("addr_model_degraded")),
        },
        "hb_loaded": bool(include_hb),
    }
    if include_perf:
        perf["total_ms"] = round((time.perf_counter() - t_all) * 1000, 2)
        meta["perf"] = {
            "service": perf,
            "current": current_data.get("perf") or {},
            "yoy": yoy_data.get("perf") or {},
            "hb": (hb_data.get("perf") or {}) if include_hb else {},
        }
        if perf["total_ms"] >= 5000:
            logging.warning("wcnr_10lv summary slow: %s", meta["perf"])
    return meta, rows


def get_display_columns(*, show_hb: bool, show_ratio: bool) -> List[str]:
    cols: List[str] = ["地区"]

    def add_count(label: str) -> None:
        cols.append(label)
        cols.append(f"同比{label}")
        if show_ratio:
            cols.append(f"同比{label}比例")
        if show_hb:
            cols.append(f"环比{label}")
            if show_ratio:
                cols.append(f"环比{label}比例")

    def add_rate(label: str) -> None:
        cols.append(label)
        cols.append(f"同比{label}")
        if show_hb:
            cols.append(f"环比{label}")

    def add_composite(label: str, rate_label: str) -> None:
        cols.append(label)
        cols.append(f"同比{label}")
        if show_ratio:
            cols.append(rate_label)
            cols.append(f"同比{rate_label}")
        if show_hb:
            cols.append(f"环比{label}")
            if show_ratio:
                cols.append(f"环比{rate_label}")

    for metric_kind, metric_key in SUMMARY_LAYOUT:
        if metric_kind == "count":
            add_count(COUNT_METRIC_MAP[metric_key]["label"])
        elif metric_kind == "rate":
            add_rate(RATE_METRIC_MAP[metric_key]["label"])
        else:
            metric = COMPOSITE_METRIC_MAP[metric_key]
            add_composite(metric["label"], metric["rate_label"])
    return cols


def trim_rows_for_display(
    *, rows: Sequence[Dict[str, Any]], show_hb: bool, show_ratio: bool
) -> List[Dict[str, Any]]:
    cols = get_display_columns(show_hb=show_hb, show_ratio=show_ratio)
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        item: Dict[str, Any] = {}
        for col in cols:
            item[col] = row.get(col)
        out.append(item)
    return out


def _normalize_part(metric: str, part: str | None) -> str:
    m = str(metric or "").strip()
    p = str(part or "").strip().lower()
    if m in COMPOSITE_METRIC_KEYS:
        if p in ("numerator", "denominator"):
            return p
        return "numerator"
    return "value"


def _period_range(meta: Dict[str, str], period: str) -> Tuple[str, str]:
    p = str(period or "").strip().lower()
    if p == "yoy":
        return meta["yoy_start_time"], meta["yoy_end_time"]
    if p == "hb":
        return meta["hb_start_time"], meta["hb_end_time"]
    return meta["start_time"], meta["end_time"]


def _period_label(period: str) -> str:
    p = str(period or "").strip().lower()
    if p == "yoy":
        return "同比"
    if p == "hb":
        return "环比"
    return "当前"


def metric_display_name(metric: str, part: str | None = None) -> str:
    m = str(metric or "").strip()
    base = DETAIL_METRIC_LABEL.get(m, m)
    p = _normalize_part(m, part)
    if m in COMPOSITE_METRIC_KEYS:
        if p == "denominator":
            return f"{base}(分母)"
        return f"{base}(分子)"
    return base


def fetch_detail(
    *,
    metric: str,
    part: str,
    period: str,
    diqu: str,
    start_time: str,
    end_time: str,
    hb_start_time: str | None,
    hb_end_time: str | None,
    leixing_list: Sequence[str],
) -> List[Dict[str, Any]]:
    metric_key = str(metric or "").strip()
    if not metric_key:
        raise ValueError("metric 不能为空")
    if metric_key not in DETAIL_METRIC_LABEL:
        raise ValueError(f"不支持的 metric: {metric_key}")

    part_key = _normalize_part(metric_key, part)
    meta = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=hb_start_time,
        hb_end_time=hb_end_time,
    )
    p_start, p_end = _period_range(meta, period)

    leixing = _normalize_leixing_list(leixing_list)
    conn = get_database_connection()
    try:
        rows = wcnr_10lv_dao.fetch_metric_detail_rows(
            conn,
            metric=metric_key,
            part=part_key,
            start_time=p_start,
            end_time=p_end,
            leixing_list=leixing,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    rows = wcnr_10lv_dao.filter_rows_by_diqu(rows, diqu)
    return rows


def build_detail_export_sheets(
    *,
    start_time: str,
    end_time: str,
    hb_start_time: str | None,
    hb_end_time: str | None,
    leixing_list: Sequence[str],
    show_hb: bool,
) -> List[Dict[str, Any]]:
    meta = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=hb_start_time,
        hb_end_time=hb_end_time,
    )
    leixing = _normalize_leixing_list(leixing_list)

    periods = ["current", "yoy"]
    if show_hb:
        periods.append("hb")

    period_range_map = {
        "current": (meta["start_time"], meta["end_time"]),
        "yoy": (meta["yoy_start_time"], meta["yoy_end_time"]),
        "hb": (meta["hb_start_time"], meta["hb_end_time"]),
    }

    conn = get_database_connection()
    try:
        period_data_map: Dict[str, Dict[str, Any]] = {}
        for p in periods:
            p_start, p_end = period_range_map[p]
            period_data_map[p] = wcnr_10lv_dao.fetch_period_data(
                conn,
                start_time=p_start,
                end_time=p_end,
                leixing_list=leixing,
                include_details=True,
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    sheets: List[Dict[str, Any]] = []
    for metric in EXPORT_METRIC_ORDER:
        parts = ["value"] if metric not in COMPOSITE_METRIC_KEYS else ["numerator", "denominator"]
        for part in parts:
            part_label = ""
            if metric in COMPOSITE_METRIC_KEYS:
                part_label = "分子" if part == "numerator" else "分母"
            for p in periods:
                period_data = period_data_map[p]
                rows = wcnr_10lv_dao.select_detail_rows(period_data, metric=metric, part=part)
                if part_label:
                    sheet_name = f"{DETAIL_METRIC_LABEL[metric]}-{part_label}-{_period_label(p)}"
                else:
                    sheet_name = f"{DETAIL_METRIC_LABEL[metric]}-{_period_label(p)}"
                sheets.append({"name": sheet_name, "rows": rows})

    return sheets


def build_campus_bullying_incident_case_export_rows(*, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    ranges = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=None,
        hb_end_time=None,
    )
    incident_rows = wcnr_case_list_dao.fetch_campus_bullying_case_rows(
        start_time=ranges["start_time"],
        end_time=ranges["end_time"],
    )
    incident_numbers = [
        str(_first_non_empty(row, "caseNo", "caseno") or "").strip()
        for row in incident_rows
    ]

    conn = get_database_connection()
    try:
        cases_by_incident = wcnr_case_list_dao.fetch_cases_by_incident_numbers(conn, incident_numbers)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    rows: List[Dict[str, Any]] = []
    for incident in incident_rows:
        case_no = str(_first_non_empty(incident, "caseNo", "caseno") or "").strip()
        matched_cases = cases_by_incident.get(case_no) or []
        if not matched_cases:
            matched_cases = [{}]
        for case in matched_cases:
            rows.append(
                {
                    "警情编号": case_no,
                    "报警时间": _first_non_empty(incident, "callTime", "calltime"),
                    "管辖单位": _first_non_empty(incident, "dutyDeptName", "dutydeptname"),
                    "分局": _first_non_empty(incident, "cmdName", "cmdname"),
                    "报警内容": _first_non_empty(
                        incident,
                        "caseContent",
                        "caseContents",
                        "casecontents",
                    ),
                    "处警情况": _first_non_empty(incident, "replies"),
                    "警情地址": _first_non_empty(incident, "occurAddress", "occuraddress"),
                    "案件编号": case.get("ajxx_ajbh") or "",
                    "案件名称": case.get("ajxx_ajmc") or "",
                    "立案时间": _format_datetime_for_export(case.get("ajxx_lasj")),
                }
            )

    return rows
