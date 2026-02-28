"""
矛盾纠纷重复报警统计：Service 层

- 默认时间范围：结束时间=今日 00:00:00，开始时间=今日向前 7 天 00:00:00
- 分局选项（前端固定）映射为正则关键词
- 提供 summary / detail 查询及 csv / xlsx 导出
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, time
from io import BytesIO, StringIO
from typing import Any, Dict, List, Optional, Tuple

from flask import Response, send_file
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from mdjfxsyj.dao.mdjfxsyj_cfbj_dao import fetch_cfbj_detail, fetch_cfbj_summary


# --------------------------------------------------------------------------
# 分局名称 → 正则关键词映射
# --------------------------------------------------------------------------
FENJU_KEYWORD_MAP: Dict[str, str] = {
    "云城": "云城",
    "云安": "云安",
    "罗定": "罗定",
    "新兴": "新兴",
    "郁南": "郁南",
    "市局": "云浮市局",
}


def _fenju_list_to_patterns(fenju_list: Optional[List[str]]) -> Optional[List[str]]:
    """将前端传入的分局简称列表转换为正则模式列表。"""
    if not fenju_list:
        return None
    patterns = []
    for fj in fenju_list:
        kw = FENJU_KEYWORD_MAP.get(fj.strip())
        if kw:
            patterns.append(kw)
    return patterns or None


# --------------------------------------------------------------------------
# 默认时间范围
# --------------------------------------------------------------------------

def _default_range() -> Tuple[str, str]:
    today = datetime.now().date()
    end_dt = datetime.combine(today, time(0, 0, 0))
    start_dt = end_dt - timedelta(days=7)
    return start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt_str(val: str) -> str:
    """校验并规范化时间字符串，返回 'YYYY-MM-DD HH:MM:SS'。"""
    val = (val or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"时间格式错误：{val!r}，请使用 YYYY-MM-DD HH:MM:SS")


def _normalize_range(start_time: Optional[str], end_time: Optional[str]) -> Tuple[str, str]:
    if start_time and end_time:
        s = _parse_dt_str(start_time)
        e = _parse_dt_str(end_time)
    else:
        s, e = _default_range()
    if s > e:
        raise ValueError("开始时间不能晚于结束时间")
    return s, e


# --------------------------------------------------------------------------
# 查询接口
# --------------------------------------------------------------------------

def get_cfbj_summary(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    fenju_list: Optional[List[str]],
    min_cs: Optional[int],
) -> Tuple[List[Dict[str, Any]], str, str]:
    s, e = _normalize_range(start_time, end_time)
    patterns = _fenju_list_to_patterns(fenju_list)
    conn = get_database_connection()
    try:
        rows = fetch_cfbj_summary(conn, start_time=s, end_time=e, fenju_patterns=patterns, min_cs=min_cs)
    finally:
        conn.close()
    return rows, s, e


def get_cfbj_detail(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    fenju_list: Optional[List[str]],
    min_cs: Optional[int],
    fenju_exact: Optional[str] = None,
    detail_type: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], str, str]:
    s, e = _normalize_range(start_time, end_time)
    patterns = _fenju_list_to_patterns(fenju_list)
    conn = get_database_connection()
    try:
        rows = fetch_cfbj_detail(
            conn,
            start_time=s,
            end_time=e,
            fenju_patterns=patterns,
            min_cs=min_cs,
            fenju_exact=fenju_exact,
            detail_type=detail_type,
        )
    finally:
        conn.close()
    return rows, s, e


# --------------------------------------------------------------------------
# 导出工具
# --------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def export_cfbj_to_csv(
    rows: List[Dict[str, Any]],
    *,
    filename: str,
) -> Response:
    buf = StringIO()
    if rows:
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    encoded = buf.getvalue().encode("utf-8-sig")

    resp = Response(encoded, mimetype="text/csv; charset=utf-8-sig")
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


def export_cfbj_to_xlsx(
    rows: List[Dict[str, Any]],
    *,
    filename: str,
) -> Response:
    wb = Workbook()
    ws = wb.active
    ws.title = filename[:31]  # Excel 工作表名最长 31 字符

    if rows:
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row.get(h) for h in headers])

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def make_export_response(
    rows: List[Dict[str, Any]],
    *,
    fmt: str,
    prefix: str,
) -> Response:
    """
    prefix: 如 '矛盾纠纷重复报警统计' 或 '{分局}_矛盾纠纷重复报警总数'
    fmt:    'csv' | 'xlsx'
    """
    ts = _ts()
    if fmt == "xlsx":
        return export_cfbj_to_xlsx(rows, filename=f"{prefix}{ts}.xlsx")
    return export_cfbj_to_csv(rows, filename=f"{prefix}{ts}.csv")
