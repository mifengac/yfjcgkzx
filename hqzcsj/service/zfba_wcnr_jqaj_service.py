from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_wcnr_jqaj_dao


REGION_ORDER: List[Tuple[str, str]] = [
    ("445302", "云城"),
    ("445303", "云安"),
    ("445381", "罗定"),
    ("445321", "新兴"),
    ("445322", "郁南"),
    ("445300", "市局"),
]


@dataclass(frozen=True)
class SummaryMeta:
    start_time: str
    end_time: str
    yoy_start_time: str
    yoy_end_time: str


def default_time_range_for_page() -> Tuple[str, str]:
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today0 - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    end = today0.strftime("%Y-%m-%d 00:00:00")
    return start, end


def parse_dt(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    raise ValueError(f"时间格式错误: {s}（期望 YYYY-MM-DD HH:MM:SS）")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def shift_year(dt: datetime, years: int = -1) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except Exception:
        if dt.month == 2 and dt.day == 29:
            return dt.replace(year=dt.year + years, day=28)
        raise


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
    *, start_time: str, end_time: str, leixing_list: Sequence[str], za_types: Sequence[str]
) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    yoy_start = shift_year(start_dt, -1)
    yoy_end = shift_year(end_dt, -1)
    meta = SummaryMeta(
        start_time=fmt_dt(start_dt),
        end_time=fmt_dt(end_dt),
        yoy_start_time=fmt_dt(yoy_start),
        yoy_end_time=fmt_dt(yoy_end),
    )

    leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
    za_types = [str(x).strip() for x in (za_types or []) if str(x).strip()]

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

        # 当前
        jq_now = (
            {}
            if typed_subclass_empty
            else _call(
                "当前-警情",
                lambda: zfba_wcnr_jqaj_dao.count_jq_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, leixing_list=leixing_list),
            )
        )
        zhuanan_now = (
            {}
            if typed_subclass_empty
            else _call(
                "当前-转案数",
                lambda: zfba_wcnr_jqaj_dao.count_zhuanan_by_diqu(
                    conn, start_time=meta.start_time, end_time=meta.end_time, leixing_list=leixing_list
                ),
            )
        )
        ajxx_now = (
            {"行政": {}, "刑事": {}}
            if typed_patterns_empty
            else _call(
                "当前-案件(行政/刑事)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_ajxx_by_diqu_and_ajlx(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
            )
        )
        changsuo_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-场所案件",
                lambda: zfba_wcnr_jqaj_dao.count_changsuo_ajxx_by_diqu(
                    conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns
                ),
            )
        )
        wfry_rows_now = (
            []
            if typed_patterns_empty
            else _call(
                "当前-v_wcnr_wfry_base",
                lambda: zfba_wcnr_jqaj_dao.fetch_wcnr_jzqk_rows(
                    conn, start_time=meta.start_time, end_time=meta.end_time, leixing_list=leixing_list
                ),
            )
        )
        wfry_stats_now = _build_wfry_stats_bundle(wfry_rows_now)
        xz_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-治安处罚",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                    conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns, za_types=za_types, not_execute_only=False
                ),
            )
        )
        xz_noexec_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-治安处罚(不执行)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                    conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns, za_types=za_types, not_execute_only=True
                ),
            )
        )
        jlz_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-刑拘",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_jlz_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
            )
        )
        # 新增：案件数(被侵害)
        shr_ajxx_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-案件数(被侵害)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_shr_ajxx_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
            )
        )
        shr_changsuo_now = (
            {}
            if typed_patterns_empty
            else _call(
                "当前-场所案件(被侵害)",
                lambda: zfba_wcnr_jqaj_dao.count_changsuo_shr_ajxx_by_diqu(
                    conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns
                ),
            )
        )
        # 同比
        jq_yoy = (
            {}
            if typed_subclass_empty
            else _call(
                "同比-警情",
                lambda: zfba_wcnr_jqaj_dao.count_jq_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, leixing_list=leixing_list),
            )
        )
        zhuanan_yoy = (
            {}
            if typed_subclass_empty
            else _call(
                "同比-转案数",
                lambda: zfba_wcnr_jqaj_dao.count_zhuanan_by_diqu(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, leixing_list=leixing_list
                ),
            )
        )
        ajxx_yoy = (
            {"行政": {}, "刑事": {}}
            if typed_patterns_empty
            else _call(
                "同比-案件(行政/刑事)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_ajxx_by_diqu_and_ajlx(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
            )
        )
        changsuo_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-场所案件",
                lambda: zfba_wcnr_jqaj_dao.count_changsuo_ajxx_by_diqu(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns
                ),
            )
        )
        wfry_rows_yoy = (
            []
            if typed_patterns_empty
            else _call(
                "同比-v_wcnr_wfry_base",
                lambda: zfba_wcnr_jqaj_dao.fetch_wcnr_jzqk_rows(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, leixing_list=leixing_list
                ),
            )
        )
        wfry_stats_yoy = _build_wfry_stats_bundle(wfry_rows_yoy)
        xz_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-治安处罚",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns, za_types=za_types, not_execute_only=False
                ),
            )
        )
        xz_noexec_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-治安处罚(不执行)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_xzcfjds_by_diqu(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns, za_types=za_types, not_execute_only=True
                ),
            )
        )
        jlz_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-刑拘",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_jlz_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
            )
        )
        # 新增：案件数(被侵害)同比
        shr_ajxx_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-案件数(被侵害)",
                lambda: zfba_wcnr_jqaj_dao.count_wcnr_shr_ajxx_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
            )
        )
        shr_changsuo_yoy = (
            {}
            if typed_patterns_empty
            else _call(
                "同比-场所案件(被侵害)",
                lambda: zfba_wcnr_jqaj_dao.count_changsuo_shr_ajxx_by_diqu(
                    conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns
                ),
            )
        )

        def g(m: Dict[str, int], code: str) -> int:
            return int(m.get(code) or 0)

        def g2(m: Dict[str, Dict[str, int]], key: str, code: str) -> int:
            return int((m.get(key) or {}).get(code) or 0)

        def gm(m: Dict[str, Dict[str, int]], metric: str, code: str) -> int:
            return int((m.get(metric) or {}).get(code) or 0)

        rows: List[Dict[str, Any]] = []
        for code, name in REGION_ORDER:
            rows.append(
                {
                    "地区": name,
                    "地区代码": code,
                    "警情": g(jq_now, code),
                    "同比警情": g(jq_yoy, code),
                    "转案数": g(zhuanan_now, code),
                    "同比转案数": g(zhuanan_yoy, code),
                    "案件数(被侵害)": g(shr_ajxx_now, code),
                    "同比案件数(被侵害)": g(shr_ajxx_yoy, code),
                    "场所案件(被侵害)": g(shr_changsuo_now, code),
                    "同比场所案件(被侵害)": g(shr_changsuo_yoy, code),
                    "行政": g2(ajxx_now, "行政", code),
                    "同比行政": g2(ajxx_yoy, "行政", code),
                    "刑事": g2(ajxx_now, "刑事", code),
                    "同比刑事": g2(ajxx_yoy, "刑事", code),
                    "场所案件": g(changsuo_now, code),
                    "同比场所案件": g(changsuo_yoy, code),
                    "治安处罚": g(xz_now, code),
                    "同比治安处罚": g(xz_yoy, code),
                    "治安处罚(不执行)": g(xz_noexec_now, code),
                    "同比治安处罚(不执行)": g(xz_noexec_yoy, code),
                    "刑拘": g(jlz_now, code),
                    "同比刑拘": g(jlz_yoy, code),
                    "矫治文书(行政)": gm(wfry_stats_now, "矫治文书(行政)", code),
                    "同比矫治文书(行政)": gm(wfry_stats_yoy, "矫治文书(行政)", code),
                    "矫治文书(刑事)": gm(wfry_stats_now, "矫治文书(刑事)", code),
                    "同比矫治文书(刑事)": gm(wfry_stats_yoy, "矫治文书(刑事)", code),
                    "加强监督教育(行政)": gm(wfry_stats_now, "加强监督教育(行政)", code),
                    "同比加强监督教育(行政)": gm(wfry_stats_yoy, "加强监督教育(行政)", code),
                    "加强监督教育(刑事)": gm(wfry_stats_now, "加强监督教育(刑事)", code),
                    "同比加强监督教育(刑事)": gm(wfry_stats_yoy, "加强监督教育(刑事)", code),
                    "符合送校": gm(wfry_stats_now, "符合送校", code),
                    "送校": gm(wfry_stats_now, "送校", code),
                    "同比送校": gm(wfry_stats_yoy, "送校", code),
                }
            )

        # 全市：按 6 个地区（含市局 445300）合计，放在最后一行
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
