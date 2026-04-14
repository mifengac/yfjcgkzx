from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_wcnr_jqaj_dao
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


REGION_ORDER: List[Tuple[str, str]] = [
    ("445302", "云城"),
    ("445303", "云安"),
    ("445381", "罗定"),
    ("445321", "新兴"),
    ("445322", "郁南"),
    ("445300", "市局"),
]

RATIO_RULES: List[Tuple[str, str, str]] = [
    ("警情", "同比警情", "起"),
    ("案件数(被侵害)", "同比案件数(被侵害)", "起"),
    ("场所案件(被侵害)", "同比场所案件(被侵害)", "起"),
    ("行政", "同比行政", "起"),
    ("刑事", "同比刑事", "起"),
    ("场所案件", "同比场所案件", "起"),
    ("治安处罚", "同比治安处罚", "人次"),
    ("治安处罚(不执行)", "同比治安处罚(不执行)", "人次"),
    ("刑拘", "同比刑拘", "人次"),
    ("矫治文书(行政)", "同比矫治文书(行政)", "人次"),
    ("矫治文书(刑事)", "同比矫治文书(刑事)", "人次"),
    ("加强监督教育(行政)", "同比加强监督教育(行政)", "人次"),
    ("加强监督教育(刑事)", "同比加强监督教育(刑事)", "人次"),
    ("送校", "同比送校", "人次"),
]
RATIO_HB_RULES: List[Tuple[str, str, str]] = [
    (cur_col, f"环比{cur_col}", unit) for cur_col, _yoy_col, unit in RATIO_RULES
]
RATIO_RULE_BY_COMPARE: Dict[str, Tuple[str, str]] = {
    compare_col: (cur_col, unit) for cur_col, compare_col, unit in (RATIO_RULES + RATIO_HB_RULES)
}


@dataclass(frozen=True)
class SummaryMeta:
    start_time: str
    end_time: str
    yoy_start_time: str
    yoy_end_time: str
    hb_start_time: str
    hb_end_time: str


def default_time_range_for_page() -> Tuple[str, str]:
    return default_recent_time_window(days=7)


def append_ratio_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        new_row: Dict[str, Any] = {}
        for key, value in row.items():
            new_row[key] = value
            rule = RATIO_RULE_BY_COMPARE.get(key)
            if rule:
                current_col, unit = rule
                ratio_col = f"{key}比例"
                new_row[ratio_col] = calc_ratio_text(row.get(current_col), row.get(key), unit)
        new_row["转案率"] = calc_percent_text(row.get("转案数"), row.get("警情"))
        new_row["同比转案率"] = calc_percent_text(row.get("同比转案数"), row.get("同比警情"))
        new_row["环比转案率"] = calc_percent_text(row.get("环比转案数"), row.get("环比警情"))
        out.append(new_row)
    return out


def _build_wfry_stats_bundle(rows: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """基于“矫治情况统计”同口径明细，按地区聚合未成年人统计相关指标。"""
    out: Dict[str, Dict[str, int]] = {
        "矫治文书(行政)": {},
        "矫治文书(刑事)": {},
        "加强监督教育(行政)": {},
        "加强监督教育(刑事)": {},
        "符合送校": {},
        "送校": {},
    }

    def _inc(metric: str, code: str) -> None:
        bucket = out.get(metric)
        if bucket is None:
            return
        bucket[code] = int(bucket.get(code) or 0) + 1

    for row in rows:
        diqu_code = str(row.get("地区") or "").strip()
        if not diqu_code:
            continue

        ajlx = str(row.get("案件类型") or "").strip()
        if str(row.get("是否开具矫治文书") or "").strip() == "是":
            if ajlx == "行政":
                _inc("矫治文书(行政)", diqu_code)
            elif ajlx == "刑事":
                _inc("矫治文书(刑事)", diqu_code)

        if str(row.get("是否开具家庭教育指导书") or "").strip() == "是":
            if ajlx == "行政":
                _inc("加强监督教育(行政)", diqu_code)
            elif ajlx == "刑事":
                _inc("加强监督教育(刑事)", diqu_code)

        if str(row.get("是否符合送生") or "").strip() == "是":
            _inc("符合送校", diqu_code)
        if str(row.get("是否送校") or "").strip() == "是":
            _inc("送校", diqu_code)

    return out


def build_summary(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    za_types: Sequence[str],
    hb_start_time: Optional[str] = None,
    hb_end_time: Optional[str] = None,
) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
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
        hb_start_dt, hb_end_dt = infer_hb_range(start_dt, end_dt)
    if hb_end_dt < hb_start_dt:
        raise ValueError("环比结束时间不能早于环比开始时间")

    meta = SummaryMeta(
        start_time=fmt_dt(start_dt),
        end_time=fmt_dt(end_dt),
        yoy_start_time=fmt_dt(yoy_start),
        yoy_end_time=fmt_dt(yoy_end),
        hb_start_time=fmt_dt(hb_start_dt),
        hb_end_time=fmt_dt(hb_end_dt),
    )

    leixing_list = normalize_text_list(leixing_list)
    za_types = normalize_text_list(za_types)

    def _call(stage: str, fn):
        try:
            return fn()
        except Exception as exc:
            raise RuntimeError(f"{stage}查询失败: {exc}") from exc

    conn = get_database_connection()
    try:
        patterns = _call("类型映射(案由)", lambda: zfba_wcnr_jqaj_dao.fetch_ay_patterns(conn, leixing_list=leixing_list))
        subclasses = _call(
            "类型映射(警情子类)",
            lambda: (zfba_wcnr_jqaj_dao.fetch_newcharasubclass_list(conn, leixing_list=leixing_list) if leixing_list else []),
        )
        typed_patterns_empty = bool(leixing_list) and not patterns
        typed_subclass_empty = bool(leixing_list) and not subclasses

        def _collect_period_stats(stage_prefix: str, period_start: str, period_end: str) -> Dict[str, Dict[str, int]]:
            jq = (
                {}
                if typed_subclass_empty
                else _call(
                    f"{stage_prefix}-警情",
                    lambda: zfba_wcnr_jqaj_dao.count_jq_by_diqu(conn, start_time=period_start, end_time=period_end, leixing_list=leixing_list),
                )
            )
            zhuanan = (
                {}
                if typed_subclass_empty
                else _call(
                    f"{stage_prefix}-转案数",
                    lambda: zfba_wcnr_jqaj_dao.count_zhuanan_by_diqu(
                        conn, start_time=period_start, end_time=period_end, leixing_list=leixing_list
                    ),
                )
            )
            ajxx = (
                {"行政": {}, "刑事": {}}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-案件(行政/刑事)",
                    lambda: zfba_wcnr_jqaj_dao.count_wcnr_ajxx_by_diqu_and_ajlx(conn, start_time=period_start, end_time=period_end, patterns=patterns),
                )
            )
            changsuo = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-场所案件",
                    lambda: zfba_wcnr_jqaj_dao.count_changsuo_ajxx_by_diqu(
                        conn, start_time=period_start, end_time=period_end, patterns=patterns
                    ),
                )
            )
            wfry_rows = (
                []
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-v_wcnr_wfry_base",
                    lambda: zfba_wcnr_jqaj_dao.fetch_wcnr_jzqk_rows(
                        conn, start_time=period_start, end_time=period_end, leixing_list=leixing_list
                    ),
                )
            )
            wfry_stats = _build_wfry_stats_bundle(wfry_rows)
            xz = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-治安处罚",
                    lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                        conn, start_time=period_start, end_time=period_end, patterns=patterns, za_types=za_types, not_execute_only=False
                    ),
                )
            )
            xz_noexec = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-治安处罚(不执行)",
                    lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                        conn, start_time=period_start, end_time=period_end, patterns=patterns, za_types=za_types, not_execute_only=True
                    ),
                )
            )
            jlz = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-刑拘",
                    lambda: zfba_wcnr_jqaj_dao.count_wcnr_jlz_by_diqu(conn, start_time=period_start, end_time=period_end, patterns=patterns),
                )
            )
            shr_ajxx = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-案件数(被侵害)",
                    lambda: zfba_wcnr_jqaj_dao.count_wcnr_shr_ajxx_by_diqu(conn, start_time=period_start, end_time=period_end, patterns=patterns),
                )
            )
            shr_changsuo = (
                {}
                if typed_patterns_empty
                else _call(
                    f"{stage_prefix}-场所案件(被侵害)",
                    lambda: zfba_wcnr_jqaj_dao.count_changsuo_shr_ajxx_by_diqu(
                        conn, start_time=period_start, end_time=period_end, patterns=patterns
                    ),
                )
            )

            return {
                "警情": jq,
                "转案数": zhuanan,
                "案件数(被侵害)": shr_ajxx,
                "场所案件(被侵害)": shr_changsuo,
                "行政": ajxx.get("行政", {}),
                "刑事": ajxx.get("刑事", {}),
                "场所案件": changsuo,
                "治安处罚": xz,
                "治安处罚(不执行)": xz_noexec,
                "刑拘": jlz,
                "矫治文书(行政)": wfry_stats.get("矫治文书(行政)", {}),
                "矫治文书(刑事)": wfry_stats.get("矫治文书(刑事)", {}),
                "加强监督教育(行政)": wfry_stats.get("加强监督教育(行政)", {}),
                "加强监督教育(刑事)": wfry_stats.get("加强监督教育(刑事)", {}),
                "符合送校": wfry_stats.get("符合送校", {}),
                "送校": wfry_stats.get("送校", {}),
            }

        now_stats = _collect_period_stats("当前", meta.start_time, meta.end_time)
        yoy_stats = _collect_period_stats("同比", meta.yoy_start_time, meta.yoy_end_time)
        hb_stats = _collect_period_stats("环比", meta.hb_start_time, meta.hb_end_time)

        def g(m: Dict[str, int], code: str) -> int:
            return int(m.get(code) or 0)

        ratio_metrics = [
            "警情",
            "转案数",
            "案件数(被侵害)",
            "场所案件(被侵害)",
            "行政",
            "刑事",
            "场所案件",
            "治安处罚",
            "治安处罚(不执行)",
            "刑拘",
            "矫治文书(行政)",
            "矫治文书(刑事)",
            "加强监督教育(行政)",
            "加强监督教育(刑事)",
        ]

        rows: List[Dict[str, Any]] = []
        for code, name in REGION_ORDER:
            row: Dict[str, Any] = {"地区": name, "地区代码": code}
            for metric in ratio_metrics:
                row[metric] = g(now_stats.get(metric, {}), code)
                row[f"同比{metric}"] = g(yoy_stats.get(metric, {}), code)
                row[f"环比{metric}"] = g(hb_stats.get(metric, {}), code)
            row["符合送校"] = g(now_stats.get("符合送校", {}), code)
            row["送校"] = g(now_stats.get("送校", {}), code)
            row["同比送校"] = g(yoy_stats.get("送校", {}), code)
            row["环比送校"] = g(hb_stats.get("送校", {}), code)
            rows.append(row)

        if rows:
            total: Dict[str, Any] = {"地区": "全市", "地区代码": "__ALL__"}
            for k in rows[0].keys():
                if k in ("地区", "地区代码"):
                    continue
                total[k] = sum(int(r.get(k) or 0) for r in rows)
            rows.append(total)

        return meta, rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_detail(
    *,
    metric: str,
    diqu: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    za_types: Sequence[str],
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = get_database_connection()
    try:
        return zfba_wcnr_jqaj_dao.fetch_detail_rows(
            conn,
            metric=metric,
            diqu=diqu,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=za_types,
            limit=limit,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
