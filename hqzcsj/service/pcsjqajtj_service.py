from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import pcsjqajtj_dao
from hqzcsj.service.stats_common import (
    calc_percent_text,
    calc_ratio_text,
    default_recent_time_window,
    fmt_dt,
    infer_hb_range,
    normalize_text_list,
    parse_dt,
    shift_year,
)


@dataclass(frozen=True)
class SummaryMeta:
    start_time: str
    end_time: str
    yoy_start_time: str
    yoy_end_time: str
    hb_start_time: str
    hb_end_time: str


RATIO_METRIC_UNIT: Dict[str, str] = {
    "警情": "起",
    "行政": "起",
    "刑事": "起",
    "高质量": "起",
    "治拘": "人次",
    "刑拘": "人次",
    "逮捕": "人次",
    "起诉": "人次",
}


def _normalize_list(values: Sequence[str]) -> List[str]:
    return normalize_text_list(values)


def default_time_range_for_page() -> Tuple[str, str]:
    return default_recent_time_window(days=7)


def fetch_leixing_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return pcsjqajtj_dao.fetch_leixing_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_fenju_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return pcsjqajtj_dao.fetch_fenju_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def build_summary(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
    hb_start_time: Optional[str] = None,
    hb_end_time: Optional[str] = None,
) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    yoy_start_dt = shift_year(start_dt, -1)
    yoy_end_dt = shift_year(end_dt, -1)
    if hb_start_time and hb_end_time:
        hb_start_dt = parse_dt(hb_start_time)
        hb_end_dt = parse_dt(hb_end_time)
    else:
        hb_start_dt, hb_end_dt = infer_hb_range(start_dt, end_dt)
    if hb_end_dt < hb_start_dt:
        raise ValueError("环比结束时间不能早于环比开始时间")

    meta = SummaryMeta(
        start_time=fmt_dt(start_dt),
        end_time=fmt_dt(end_dt),
        yoy_start_time=fmt_dt(yoy_start_dt),
        yoy_end_time=fmt_dt(yoy_end_dt),
        hb_start_time=fmt_dt(hb_start_dt),
        hb_end_time=fmt_dt(hb_end_dt),
    )

    leixing = _normalize_list(leixing_list)
    ssfjdm = _normalize_list(ssfjdm_list)

    conn = get_database_connection()
    try:
        current_rows = pcsjqajtj_dao.fetch_summary_rows(
            conn,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        yoy_rows = pcsjqajtj_dao.fetch_summary_rows(
            conn,
            start_time=meta.yoy_start_time,
            end_time=meta.yoy_end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        hb_rows = pcsjqajtj_dao.fetch_summary_rows(
            conn,
            start_time=meta.hb_start_time,
            end_time=meta.hb_end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    yoy_map: Dict[str, Dict[str, Any]] = {
        str(row.get("派出所代码") or ""): row for row in yoy_rows
    }
    hb_map: Dict[str, Dict[str, Any]] = {
        str(row.get("派出所代码") or ""): row for row in hb_rows
    }

    out: List[Dict[str, Any]] = []
    for row in current_rows:
        pcsdm = str(row.get("派出所代码") or "")
        yoy_row = yoy_map.get(pcsdm) or {}
        hb_row = hb_map.get(pcsdm) or {}

        current_jq = int(row.get("警情") or 0)
        current_za = int(row.get("转案") or 0)
        current_xz = int(row.get("行政") or 0)
        current_xs = int(row.get("刑事") or 0)
        current_bj = int(row.get("办结") or 0)
        current_pa = int(row.get("破案") or 0)
        current_gzl = int(row.get("高质量") or 0)
        current_zhiju = int(row.get("治拘") or 0)
        current_xingju = int(row.get("刑拘") or 0)
        current_daibu = int(row.get("逮捕") or 0)
        current_qisu = int(row.get("起诉") or 0)

        yoy_jq = int(yoy_row.get("警情") or 0)
        yoy_za = int(yoy_row.get("转案") or 0)
        yoy_xz = int(yoy_row.get("行政") or 0)
        yoy_xs = int(yoy_row.get("刑事") or 0)
        yoy_bj = int(yoy_row.get("办结") or 0)
        yoy_pa = int(yoy_row.get("破案") or 0)
        yoy_gzl = int(yoy_row.get("高质量") or 0)
        yoy_zhiju = int(yoy_row.get("治拘") or 0)
        yoy_xingju = int(yoy_row.get("刑拘") or 0)
        yoy_daibu = int(yoy_row.get("逮捕") or 0)
        yoy_qisu = int(yoy_row.get("起诉") or 0)

        hb_jq = int(hb_row.get("警情") or 0)
        hb_za = int(hb_row.get("转案") or 0)
        hb_xz = int(hb_row.get("行政") or 0)
        hb_xs = int(hb_row.get("刑事") or 0)
        hb_bj = int(hb_row.get("办结") or 0)
        hb_pa = int(hb_row.get("破案") or 0)
        hb_gzl = int(hb_row.get("高质量") or 0)
        hb_zhiju = int(hb_row.get("治拘") or 0)
        hb_xingju = int(hb_row.get("刑拘") or 0)
        hb_daibu = int(hb_row.get("逮捕") or 0)
        hb_qisu = int(hb_row.get("起诉") or 0)

        item: Dict[str, Any] = {
            "所属分局": row.get("所属分局") or "",
            "所属分局代码": row.get("所属分局代码") or "",
            "派出所名称": row.get("派出所名称") or "",
            "派出所代码": pcsdm,
            "警情": current_jq,
            "同比警情": yoy_jq,
            "环比警情": hb_jq,
            "转案": current_za,
            "同比转案": yoy_za,
            "环比转案": hb_za,
            "转案率": calc_percent_text(current_za, current_jq),
            "同比转案率": calc_percent_text(yoy_za, yoy_jq),
            "环比转案率": calc_percent_text(hb_za, hb_jq),
            "行政": current_xz,
            "同比行政": yoy_xz,
            "环比行政": hb_xz,
            "刑事": current_xs,
            "同比刑事": yoy_xs,
            "环比刑事": hb_xs,
            "办结": current_bj,
            "同比办结": yoy_bj,
            "环比办结": hb_bj,
            "办结率": calc_percent_text(current_bj, current_xz),
            "环比办结率": calc_percent_text(hb_bj, hb_xz),
            "破案": current_pa,
            "同比破案": yoy_pa,
            "环比破案": hb_pa,
            "破案率": calc_percent_text(current_pa, current_xs),
            "环比破案率": calc_percent_text(hb_pa, hb_xs),
            "高质量": current_gzl,
            "同比高质量": yoy_gzl,
            "环比高质量": hb_gzl,
            "治拘": current_zhiju,
            "同比治拘": yoy_zhiju,
            "环比治拘": hb_zhiju,
            "刑拘": current_xingju,
            "同比刑拘": yoy_xingju,
            "环比刑拘": hb_xingju,
            "逮捕": current_daibu,
            "同比逮捕": yoy_daibu,
            "环比逮捕": hb_daibu,
            "起诉": current_qisu,
            "同比起诉": yoy_qisu,
            "环比起诉": hb_qisu,
        }

        item["同比警情比例"] = calc_ratio_text(current_jq, yoy_jq, RATIO_METRIC_UNIT["警情"])
        item["同比行政比例"] = calc_ratio_text(current_xz, yoy_xz, RATIO_METRIC_UNIT["行政"])
        item["同比刑事比例"] = calc_ratio_text(current_xs, yoy_xs, RATIO_METRIC_UNIT["刑事"])
        item["同比高质量比例"] = calc_ratio_text(current_gzl, yoy_gzl, RATIO_METRIC_UNIT["高质量"])
        item["同比治拘比例"] = calc_ratio_text(current_zhiju, yoy_zhiju, RATIO_METRIC_UNIT["治拘"])
        item["同比刑拘比例"] = calc_ratio_text(current_xingju, yoy_xingju, RATIO_METRIC_UNIT["刑拘"])
        item["同比逮捕比例"] = calc_ratio_text(current_daibu, yoy_daibu, RATIO_METRIC_UNIT["逮捕"])
        item["同比起诉比例"] = calc_ratio_text(current_qisu, yoy_qisu, RATIO_METRIC_UNIT["起诉"])
        item["环比警情比例"] = calc_ratio_text(current_jq, hb_jq, RATIO_METRIC_UNIT["警情"])
        item["环比行政比例"] = calc_ratio_text(current_xz, hb_xz, RATIO_METRIC_UNIT["行政"])
        item["环比刑事比例"] = calc_ratio_text(current_xs, hb_xs, RATIO_METRIC_UNIT["刑事"])
        item["环比高质量比例"] = calc_ratio_text(current_gzl, hb_gzl, RATIO_METRIC_UNIT["高质量"])
        item["环比治拘比例"] = calc_ratio_text(current_zhiju, hb_zhiju, RATIO_METRIC_UNIT["治拘"])
        item["环比刑拘比例"] = calc_ratio_text(current_xingju, hb_xingju, RATIO_METRIC_UNIT["刑拘"])
        item["环比逮捕比例"] = calc_ratio_text(current_daibu, hb_daibu, RATIO_METRIC_UNIT["逮捕"])
        item["环比起诉比例"] = calc_ratio_text(current_qisu, hb_qisu, RATIO_METRIC_UNIT["起诉"])

        out.append(item)

    return meta, out


def fetch_detail(
    *,
    metric: str,
    pcsdm: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = get_database_connection()
    try:
        return pcsjqajtj_dao.fetch_detail_rows(
            conn,
            metric=metric,
            pcsdm=pcsdm,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            limit=limit,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
