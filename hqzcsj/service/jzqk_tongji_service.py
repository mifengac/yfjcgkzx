from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import jzqk_tongji_dao


def default_time_range_for_page() -> Tuple[str, str]:
    """默认时间范围：7天前到当天 00:00:00"""
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today0 - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    end = today0.strftime("%Y-%m-%d 00:00:00")
    return start, end


def parse_dt(s: str) -> datetime:
    """解析时间字符串"""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    raise ValueError(f"时间格式错误: {s}（期望 YYYY-MM-DD HH:MM:SS）")


def fmt_dt(dt: datetime) -> str:
    """格式化时间"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_summary(
    *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """
    构建统计汇总数据

    返回: (meta, rows)
    - meta: {"start_time": "...", "end_time": "..."}
    - rows: 按分局分组的统计列表 + 最后一行全市合计
    """
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    meta = {
        "start_time": fmt_dt(start_dt),
        "end_time": fmt_dt(end_dt),
    }

    conn = get_database_connection()
    try:
        # 获取详细数据
        detail_rows = jzqk_tongji_dao.fetch_jzqk_data(
            conn, start_time=meta["start_time"], end_time=meta["end_time"], leixing_list=leixing_list
        )

        # 按分局分组统计
        summary_rows = jzqk_tongji_dao.calculate_summary_by_fenju(detail_rows)

        return meta, summary_rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_detail(
    *, start_time: str, end_time: str, leixing_list: Sequence[str], limit: int = 0
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    获取明细数据

    参数:
    - start_time: 开始时间
    - end_time: 结束时间
    - leixing_list: 类型列表
    - limit: 限制数量，0表示不限制

    返回: (rows, truncated)
    - rows: 详细数据列表
    - truncated: 是否被截断
    """
    conn = get_database_connection()
    try:
        rows = jzqk_tongji_dao.fetch_jzqk_data(
            conn, start_time=start_time, end_time=end_time, leixing_list=leixing_list
        )

        truncated = False
        if limit and limit > 0 and len(rows) > limit:
            rows = rows[:limit]
            truncated = True

        return rows, truncated
    finally:
        try:
            conn.close()
        except Exception:
            pass
