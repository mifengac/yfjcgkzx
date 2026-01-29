from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_jq_aj_dao


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
    """
    默认：前7天00:00:00 到 当天00:00:00
    """
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today0 - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    end = today0.strftime("%Y-%m-%d 00:00:00")
    return start, end


def parse_dt(s: str) -> datetime:
    return datetime.strptime((s or "").strip(), "%Y-%m-%d %H:%M:%S")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def shift_year(dt: datetime, years: int = -1) -> datetime:
    """
    上一年同周期：优先 replace(year-1)，若遇到 2/29 则降为 2/28。
    """
    try:
        return dt.replace(year=dt.year + years)
    except Exception:
        if dt.month == 2 and dt.day == 29:
            return dt.replace(year=dt.year + years, day=28)
        raise


def build_summary(*, start_time: str, end_time: str, leixing_list: Sequence[str]) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
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

    conn = get_database_connection()
    try:
        patterns = zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing_list)

        # 当前
        jq_now = zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, leixing_list=leixing_list)
        ajxx_now = zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)
        zhiju_now = zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)
        xingju_now = zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)
        daibu_now = zfba_jq_aj_dao.count_dbz_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)
        qisu_now = zfba_jq_aj_dao.count_qsryxx_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)
        yisong_now = zfba_jq_aj_dao.count_ysajtzs_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns)

        # 同比（上一年同周期）
        jq_yoy = zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, leixing_list=leixing_list)
        ajxx_yoy = zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)
        zhiju_yoy = zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)
        xingju_yoy = zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)
        daibu_yoy = zfba_jq_aj_dao.count_dbz_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)
        qisu_yoy = zfba_jq_aj_dao.count_qsryxx_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)
        yisong_yoy = zfba_jq_aj_dao.count_ysajtzs_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns)

        def g(m: Dict[str, int], code: str) -> int:
            return int(m.get(code) or 0)

        rows: List[Dict[str, Any]] = []
        for code, name in REGION_ORDER:
            rows.append(
                {
                    "地区": name,
                    "地区代码": code,
                    "警情": g(jq_now, code),
                    "同比警情": g(jq_yoy, code),
                    "行政": g(ajxx_now.get("行政", {}), code),
                    "同比行政": g(ajxx_yoy.get("行政", {}), code),
                    "刑事": g(ajxx_now.get("刑事", {}), code),
                    "同比刑事": g(ajxx_yoy.get("刑事", {}), code),
                    "治拘": g(zhiju_now, code),
                    "同比治拘": g(zhiju_yoy, code),
                    "刑拘": g(xingju_now, code),
                    "同比刑拘": g(xingju_yoy, code),
                    "逮捕": g(daibu_now, code),
                    "同比逮捕": g(daibu_yoy, code),
                    "起诉": g(qisu_now, code),
                    "同比起诉": g(qisu_yoy, code),
                    "移送案件": g(yisong_now, code),
                    "同比移送案件": g(yisong_yoy, code),
                }
            )

        # 全市合计（6个地区 + 市局）
        total = {"地区": "全市", "地区代码": "__ALL__"}
        for k in [
            "警情",
            "同比警情",
            "行政",
            "同比行政",
            "刑事",
            "同比刑事",
            "治拘",
            "同比治拘",
            "刑拘",
            "同比刑拘",
            "逮捕",
            "同比逮捕",
            "起诉",
            "同比起诉",
            "移送案件",
            "同比移送案件",
        ]:
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
            limit=limit,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

