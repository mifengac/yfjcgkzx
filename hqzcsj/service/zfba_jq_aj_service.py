from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_jq_aj_dao
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
    ("案件数", "同比案件数", "起"),
    ("行政", "同比行政", "起"),
    ("刑事", "同比刑事", "起"),
    ("治安处罚", "同比治安处罚", "人次"),
    ("刑拘", "同比刑拘", "人次"),
    ("逮捕", "同比逮捕", "人次"),
    ("起诉", "同比起诉", "人次"),
    ("移送案件", "同比移送案件", "起"),
    ("办结", "同比办结", "起"),
    ("破案", "同比破案", "起"),
    ("高质量", "同比高质量", "起"),
]
RATIO_HB_RULES: List[Tuple[str, str, str]] = [(cur_col, f"环比{cur_col}", unit) for cur_col, _yoy_col, unit in RATIO_RULES]
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
    """默认：前7天00:00:00 到 当天00:00:00。"""
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
        # 转案率口径：转案数/警情，同比转案率：同比转案数/同比警情
        new_row["转案率"] = calc_percent_text(row.get("转案数"), row.get("警情"))
        new_row["同比转案率"] = calc_percent_text(row.get("同比转案数"), row.get("同比警情"))
        out.append(new_row)
    return out


def _collect_stats_bundle(conn, *, start_time: str, end_time: str, leixing_list: Sequence[str], patterns: Sequence[str], za_types: Sequence[str], _call, stage_prefix: str) -> Dict[str, Any]:
    jq = _call(
        f"{stage_prefix}-警情",
        lambda: zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=start_time, end_time=end_time, leixing_list=leixing_list),
    )
    zhuanan = _call(
        f"{stage_prefix}-转案数",
        lambda: zfba_jq_aj_dao.count_zhuanan_by_diqu(conn, start_time=start_time, end_time=end_time, leixing_list=leixing_list),
    )
    ajxx_all = _call(
        f"{stage_prefix}-案件数",
        lambda: zfba_jq_aj_dao.count_ajxx_all_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    ajxx = _call(
        f"{stage_prefix}-案件(行政/刑事)",
        lambda: zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    zhiju = _call(
        f"{stage_prefix}-治安处罚",
        lambda: zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(
            conn, start_time=start_time, end_time=end_time, patterns=patterns, za_types=za_types
        ),
    )
    xingju = _call(
        f"{stage_prefix}-刑拘",
        lambda: zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    daibu = _call(
        f"{stage_prefix}-逮捕",
        lambda: zfba_jq_aj_dao.count_dbz_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    qisu = _call(
        f"{stage_prefix}-起诉",
        lambda: zfba_jq_aj_dao.count_qsryxx_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    yisong = _call(
        f"{stage_prefix}-移送案件",
        lambda: zfba_jq_aj_dao.count_ysajtzs_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    banjie = _call(
        f"{stage_prefix}-办结",
        lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns, ajlx="行政"),
    )
    poan = _call(
        f"{stage_prefix}-破案",
        lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns, ajlx="刑事"),
    )
    gaozhiliang = _call(
        f"{stage_prefix}-高质量",
        lambda: zfba_jq_aj_dao.count_gaozhiliang_by_diqu(conn, start_time=start_time, end_time=end_time, patterns=patterns),
    )
    return {
        "警情": jq,
        "转案数": zhuanan,
        "案件数": ajxx_all,
        "行政": ajxx.get("行政", {}),
        "刑事": ajxx.get("刑事", {}),
        "治安处罚": zhiju,
        "刑拘": xingju,
        "逮捕": daibu,
        "起诉": qisu,
        "移送案件": yisong,
        "办结": banjie,
        "破案": poan,
        "高质量": gaozhiliang,
    }


def build_summary(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    za_types: Sequence[str] = None,
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

    def _call(stage: str, fn):
        try:
            return fn()
        except Exception as exc:
            raise RuntimeError(f"{stage}查询失败: {exc}") from exc

    conn = get_database_connection()
    try:
        leixing_list = normalize_text_list(leixing_list)
        za_types = normalize_text_list(za_types)
        patterns = _call("类型映射(案由)", lambda: zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing_list))

        now_stats = _collect_stats_bundle(
            conn,
            start_time=meta.start_time,
            end_time=meta.end_time,
            leixing_list=leixing_list,
            patterns=patterns,
            za_types=za_types,
            _call=_call,
            stage_prefix="当前",
        )
        yoy_stats = _collect_stats_bundle(
            conn,
            start_time=meta.yoy_start_time,
            end_time=meta.yoy_end_time,
            leixing_list=leixing_list,
            patterns=patterns,
            za_types=za_types,
            _call=_call,
            stage_prefix="同比",
        )
        hb_stats = _collect_stats_bundle(
            conn,
            start_time=meta.hb_start_time,
            end_time=meta.hb_end_time,
            leixing_list=leixing_list,
            patterns=patterns,
            za_types=za_types,
            _call=_call,
            stage_prefix="环比",
        )

        def g(m: Dict[str, int], code: str) -> int:
            return int(m.get(code) or 0)

        metric_order = ["警情", "转案数", "案件数", "行政", "刑事", "治安处罚", "刑拘", "逮捕", "起诉", "移送案件", "办结", "破案", "高质量"]
        rows: List[Dict[str, Any]] = []
        for code, name in REGION_ORDER:
            row: Dict[str, Any] = {"地区": name, "地区代码": code}
            for metric in metric_order:
                row[metric] = g(now_stats.get(metric, {}), code)
                row[f"同比{metric}"] = g(yoy_stats.get(metric, {}), code)
                row[f"环比{metric}"] = g(hb_stats.get(metric, {}), code)
            rows.append(row)

        # 全市合计（6个地区 + 市局）
        total = {"地区": "全市", "地区代码": "__ALL__"}
        if rows:
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
    za_types: Sequence[str] = None,
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = get_database_connection()
    try:
        return zfba_jq_aj_dao.fetch_detail_rows(
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

