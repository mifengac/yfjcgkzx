from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import wcnr_10lv_dao


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
    {"label": "行政", "key": "xingzheng", "unit": "起"},
    {"label": "刑事", "key": "xingshi", "unit": "起"},
    {"label": "案件(被侵害)", "key": "bqh_case", "unit": "起"},
    {"label": "违法犯罪人员", "key": "wfzf_people", "unit": "人"},
    {"label": "案件(场所被侵害)", "key": "cs_bqh_case", "unit": "起"},
]

COMPOSITE_METRICS: List[Dict[str, str]] = [
    {
        "key": "zmy_reoff",
        "label": "专门教育学生结业后再犯数",
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
        "den_key": "xingshi",
    },
    {
        "key": "yzbl_ratio",
        "label": "严重不良未成年人矫治教育占比",
        "rate_label": "严重不良未成年人矫治教育占比率",
        "num_key": "yzbl_num",
        "den_key": "yzbl_den",
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
]

DETAIL_METRIC_LABEL: Dict[str, str] = {
    "jq": "警情",
    "za_rate": "转案率",
    "xingzheng": "行政",
    "xingshi": "刑事",
    "bqh_case": "案件(被侵害)",
    "wfzf_people": "违法犯罪人员",
    "zmy_reoff": "专门教育学生结业后再犯数",
    "zmjz_reoff": "专门(矫治)教育学生结业后再犯数",
    "cs_bqh_case": "案件(场所被侵害)",
    "xingshi_ratio": "刑事占比",
    "yzbl_ratio": "严重不良未成年人矫治教育占比",
    "zmjz_ratio": "专门(矫治)教育占比",
    "naguan_ratio": "纳管人员再犯占比",
}

COMPOSITE_METRIC_KEYS = {
    "za_rate",
    "zmy_reoff",
    "zmjz_reoff",
    "xingshi_ratio",
    "yzbl_ratio",
    "zmjz_ratio",
    "naguan_ratio",
}


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


def parse_dt(text: str) -> datetime:
    s = (text or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    raise ValueError(f"时间格式错误: {text}（期望 YYYY-MM-DD HH:MM:SS）")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def shift_year(dt: datetime, years: int = -1) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except Exception:
        if dt.month == 2 and dt.day == 29:
            return dt.replace(year=dt.year + years, day=28)
        raise


def _to_num(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _fmt_num(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else f"{v:.2f}".rstrip("0").rstrip(".")


def calc_percent_text(num: Any, den: Any) -> str:
    d = _to_num(den)
    if d <= 0:
        return "0.00%"
    n = _to_num(num)
    return f"{(n / d) * 100:.2f}%"


def calc_ratio_text(current_value: Any, compare_value: Any, unit: str) -> str:
    cur = _to_num(current_value)
    cmp = _to_num(compare_value)
    if cur == cmp:
        return "持平"
    if cur == 0 and cmp != 0:
        return f"下降{_fmt_num(cmp)}{unit}"
    if cur != 0 and cmp == 0:
        return f"上升{_fmt_num(cur)}{unit}"
    ratio = ((cur - cmp) / cmp) * 100
    return f"{ratio:.2f}%"


def _normalize_leixing_list(leixing_list: Sequence[str]) -> List[str]:
    return [str(x).strip() for x in (leixing_list or []) if str(x).strip()]


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
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    ranges = _build_ranges(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=hb_start_time,
        hb_end_time=hb_end_time,
    )
    leixing = _normalize_leixing_list(leixing_list)

    conn = get_database_connection()
    try:
        current_data = wcnr_10lv_dao.fetch_period_data(
            conn,
            start_time=ranges["start_time"],
            end_time=ranges["end_time"],
            leixing_list=leixing,
            include_details=False,
        )
        yoy_data = wcnr_10lv_dao.fetch_period_data(
            conn,
            start_time=ranges["yoy_start_time"],
            end_time=ranges["yoy_end_time"],
            leixing_list=leixing,
            include_details=False,
        )
        hb_data: Dict[str, Any] = {"counts": {}, "flags": {}}
        if include_hb:
            hb_data = wcnr_10lv_dao.fetch_period_data(
                conn,
                start_time=ranges["hb_start_time"],
                end_time=ranges["hb_end_time"],
                leixing_list=leixing,
                include_details=False,
            )
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

    for code, name in REGION_ORDER:
        row: Dict[str, Any] = {"地区": name, "地区代码": code}

        add_count_metric(row, code=code, label="警情", key="jq", unit="起")

        cur_za = _count(current_data, "zhuanan", code)
        cur_jq = _count(current_data, "jq", code)
        yoy_za = _count(yoy_data, "zhuanan", code)
        yoy_jq = _count(yoy_data, "jq", code)
        hb_za = _count(hb_data, "zhuanan", code)
        hb_jq = _count(hb_data, "jq", code)
        row["转案率"] = calc_percent_text(cur_za, cur_jq)
        row["同比转案率"] = calc_percent_text(yoy_za, yoy_jq)
        row["环比转案率"] = calc_percent_text(hb_za, hb_jq)

        add_count_metric(row, code=code, label="行政", key="xingzheng", unit="起")
        add_count_metric(row, code=code, label="刑事", key="xingshi", unit="起")
        add_count_metric(row, code=code, label="案件(被侵害)", key="bqh_case", unit="起")
        add_count_metric(row, code=code, label="违法犯罪人员", key="wfzf_people", unit="人")

        add_composite_metric(
            row,
            code=code,
            label="专门教育学生结业后再犯数",
            rate_label="专门教育学生结业后再犯率",
            num_key="zmy_num",
            den_key="zmy_den",
        )
        add_composite_metric(
            row,
            code=code,
            label="专门(矫治)教育学生结业后再犯数",
            rate_label="专门(矫治)教育学生结业后再犯率",
            num_key="zmjz_num",
            den_key="zmjz_den",
        )

        add_count_metric(row, code=code, label="案件(场所被侵害)", key="cs_bqh_case", unit="起")

        add_composite_metric(
            row,
            code=code,
            label="刑事占比",
            rate_label="刑事占比率",
            num_key="wcnr_xingshi",
            den_key="xingshi",
        )
        add_composite_metric(
            row,
            code=code,
            label="严重不良未成年人矫治教育占比",
            rate_label="严重不良未成年人矫治教育占比率",
            num_key="yzbl_num",
            den_key="yzbl_den",
        )
        add_composite_metric(
            row,
            code=code,
            label="专门(矫治)教育占比",
            rate_label="专门(矫治)教育占比率",
            num_key="zmjz_cover_num",
            den_key="zmjz_cover_den",
        )
        add_composite_metric(
            row,
            code=code,
            label="纳管人员再犯占比",
            rate_label="纳管人员再犯率",
            num_key="naguan_num",
            den_key="naguan_den",
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

    add_count("警情")
    add_rate("转案率")
    add_count("行政")
    add_count("刑事")
    add_count("案件(被侵害)")
    add_count("违法犯罪人员")
    add_composite("专门教育学生结业后再犯数", "专门教育学生结业后再犯率")
    add_composite("专门(矫治)教育学生结业后再犯数", "专门(矫治)教育学生结业后再犯率")
    add_count("案件(场所被侵害)")
    add_composite("刑事占比", "刑事占比率")
    add_composite("严重不良未成年人矫治教育占比", "严重不良未成年人矫治教育占比率")
    add_composite("专门(矫治)教育占比", "专门(矫治)教育占比率")
    add_composite("纳管人员再犯占比", "纳管人员再犯率")
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
        period_data = wcnr_10lv_dao.fetch_period_data(
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

    rows = wcnr_10lv_dao.select_detail_rows(period_data, metric=metric_key, part=part_key)
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

    export_order = [
        "jq",
        "za_rate",
        "xingzheng",
        "xingshi",
        "bqh_case",
        "wfzf_people",
        "zmy_reoff",
        "zmjz_reoff",
        "cs_bqh_case",
        "xingshi_ratio",
        "yzbl_ratio",
        "zmjz_ratio",
        "naguan_ratio",
    ]

    sheets: List[Dict[str, Any]] = []
    for metric in export_order:
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
