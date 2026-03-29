from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from jingqing_anjian_fenxi.dao import jingqing_anjian_fenxi_dao


GROUP_MODE_COUNTY = "county"
GROUP_MODE_STATION = "station"

SUMMARY_COLUMNS = [
    "分局",
    "当前分组名称",
    "及时立案平均小时",
    "及时研判抓人平均小时",
    "及时破案平均小时",
    "及时结案平均小时",
]

METRIC_LABELS: Dict[str, str] = {
    "timely_filing": "及时立案平均小时",
    "timely_arrest": "及时研判抓人平均小时",
    "timely_solve": "及时破案平均小时",
    "timely_close": "及时结案平均小时",
}


@dataclass(frozen=True)
class SummaryMeta:
    start_time: str
    end_time: str
    group_mode: str
    group_mode_label: str


def _normalize_list(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def parse_dt(text: str) -> datetime:
    content = str(text or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(content, fmt)
        except Exception:
            pass
    raise ValueError(f"时间格式错误: {text}，期望 YYYY-MM-DD HH:MM:SS")


def fmt_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def default_time_range_for_page() -> Tuple[str, str]:
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = today0 - timedelta(days=7)
    return fmt_dt(start), fmt_dt(today0)


def normalize_group_mode(group_mode: str) -> str:
    mode = str(group_mode or GROUP_MODE_COUNTY).strip().lower()
    if mode == GROUP_MODE_STATION:
        return GROUP_MODE_STATION
    return GROUP_MODE_COUNTY


def get_group_mode_label(group_mode: str) -> str:
    return "派出所" if normalize_group_mode(group_mode) == GROUP_MODE_STATION else "县市区"


def resolve_metric_label(metric: str) -> str:
    metric_key = str(metric or "").strip()
    label = METRIC_LABELS.get(metric_key)
    if not label:
        raise ValueError(f"未知 metric: {metric_key}")
    return label


def _format_avg(sum_hours: Any, row_count: Any) -> str:
    try:
        count = int(row_count or 0)
    except Exception:
        count = 0
    if count <= 0:
        return ""
    try:
        avg = float(sum_hours or 0) / count
    except Exception:
        return ""
    return f"{avg:.2f}".rstrip("0").rstrip(".")


def fetch_fenju_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return jingqing_anjian_fenxi_dao.fetch_fenju_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_leixing_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return jingqing_anjian_fenxi_dao.fetch_leixing_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def build_summary(
    *,
    start_time: str,
    end_time: str,
    group_mode: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    normalized_group_mode = normalize_group_mode(group_mode)
    meta = SummaryMeta(
        start_time=fmt_dt(start_dt),
        end_time=fmt_dt(end_dt),
        group_mode=normalized_group_mode,
        group_mode_label=get_group_mode_label(normalized_group_mode),
    )

    leixing = _normalize_list(leixing_list)
    ssfjdm = _normalize_list(ssfjdm_list)

    conn = get_database_connection()
    try:
        group_rows = jingqing_anjian_fenxi_dao.fetch_group_rows(
            conn,
            group_mode=normalized_group_mode,
            ssfjdm_list=ssfjdm,
        )
        filing_map = jingqing_anjian_fenxi_dao.fetch_timely_filing_summary(
            conn,
            group_mode=normalized_group_mode,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        arrest_map = jingqing_anjian_fenxi_dao.fetch_timely_arrest_summary(
            conn,
            group_mode=normalized_group_mode,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        solve_map = jingqing_anjian_fenxi_dao.fetch_timely_solve_summary(
            conn,
            group_mode=normalized_group_mode,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        close_map = jingqing_anjian_fenxi_dao.fetch_timely_close_summary(
            conn,
            group_mode=normalized_group_mode,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    metrics = [
        ("timely_filing", "及时立案平均小时", filing_map),
        ("timely_arrest", "及时研判抓人平均小时", arrest_map),
        ("timely_solve", "及时破案平均小时", solve_map),
        ("timely_close", "及时结案平均小时", close_map),
    ]

    total_stats: Dict[str, Dict[str, float]] = {
        metric_key: {"sum_hours": 0.0, "row_count": 0.0}
        for metric_key, _label, _metric_map in metrics
    }

    rows: List[Dict[str, Any]] = []
    for group_row in group_rows:
        group_code = str(group_row.get("group_code") or "").strip()
        row: Dict[str, Any] = {
            "分局": group_row.get("fenju_name") or "",
            "当前分组名称": group_row.get("group_name") or "",
            "group_code": group_code,
            "fenju_code": group_row.get("fenju_code") or "",
            "group_mode": normalized_group_mode,
        }
        for metric_key, label, metric_map in metrics:
            stat = metric_map.get(group_code) or {}
            sum_hours = stat.get("sum_hours")
            row_count = stat.get("row_count")
            row[label] = _format_avg(sum_hours, row_count)
            try:
                total_stats[metric_key]["sum_hours"] += float(sum_hours or 0)
            except Exception:
                pass
            try:
                total_stats[metric_key]["row_count"] += float(row_count or 0)
            except Exception:
                pass
        rows.append(row)

    total_row: Dict[str, Any] = {
        "分局": "全市",
        "当前分组名称": "全市",
        "group_code": "__ALL__",
        "fenju_code": "__ALL__",
        "group_mode": normalized_group_mode,
    }
    for metric_key, label, _metric_map in metrics:
        total_row[label] = _format_avg(
            total_stats[metric_key]["sum_hours"],
            total_stats[metric_key]["row_count"],
        )
    rows.append(total_row)
    return meta, rows


def fetch_detail(
    *,
    metric: str,
    group_code: str,
    group_mode: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
    limit: int,
) -> Tuple[List[Dict[str, Any]], bool]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")
    metric_key = str(metric or "").strip()
    if metric_key not in METRIC_LABELS:
        raise ValueError(f"未知 metric: {metric_key}")

    conn = get_database_connection()
    try:
        return jingqing_anjian_fenxi_dao.fetch_detail_rows(
            conn,
            metric=metric_key,
            group_code=group_code,
            group_mode=normalize_group_mode(group_mode),
            start_time=fmt_dt(start_dt),
            end_time=fmt_dt(end_dt),
            leixing_list=_normalize_list(leixing_list),
            ssfjdm_list=_normalize_list(ssfjdm_list),
            limit=limit,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
