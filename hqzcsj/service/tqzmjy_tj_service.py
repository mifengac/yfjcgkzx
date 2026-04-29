from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import tqzmjy_tj_dao
from hqzcsj.service.stats_common import default_recent_time_window, fmt_dt, normalize_text_list, parse_dt


DISPLAY_COLUMNS = [
    "案件编号",
    "案件名称",
    "审批时间",
    "文书名称",
    "姓名",
    "身份证号",
    "案件类型",
    "地区",
    "承办单位",
    "案由",
    "户籍地址",
    "现住地",
]


def default_time_range_for_page() -> Tuple[str, str]:
    return default_recent_time_window(days=7)


def fetch_leixing_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return tqzmjy_tj_dao.fetch_leixing_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_fenju_options() -> List[Dict[str, str]]:
    conn = get_database_connection()
    try:
        return tqzmjy_tj_dao.fetch_fenju_list(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _format_cell_datetime(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return fmt_dt(value)
    text = str(value).strip()
    if not text:
        return ""
    return text


def query_rows(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    ssfjdm_list: Sequence[str],
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("审批结束时间不能早于审批开始时间")

    meta = {
        "start_time": fmt_dt(start_dt),
        "end_time": fmt_dt(end_dt),
    }
    leixing = normalize_text_list(leixing_list)
    ssfjdm = normalize_text_list(ssfjdm_list)

    conn = get_database_connection()
    try:
        raw_rows = tqzmjy_tj_dao.fetch_rows(
            conn,
            start_time=meta["start_time"],
            end_time=meta["end_time"],
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    rows: List[Dict[str, Any]] = []
    for raw_row in raw_rows:
        item: Dict[str, Any] = {}
        for column in DISPLAY_COLUMNS:
            value = raw_row.get(column)
            if column == "审批时间":
                item[column] = _format_cell_datetime(value)
            else:
                item[column] = "" if value is None else value
        rows.append(item)
    return meta, rows