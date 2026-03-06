from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from weichengnianren.dao.wcnr_fzxxlxxshf_dao import query_fzxxlxxshf_all, query_fzxxlxxshf_page

BRANCH_OPTIONS: List[Dict[str, str]] = [
    {"value": "云城分局", "label": "云城分局"},
    {"value": "云安分局", "label": "云安分局"},
    {"value": "罗定市公安局", "label": "罗定市公安局"},
    {"value": "新兴县公安局", "label": "新兴县公安局"},
    {"value": "郁南县公安局", "label": "郁南县公安局"},
]

ALLOWED_PAGE_SIZES = {20, 50, 100, 200}


def default_time_range() -> Tuple[str, str]:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=7)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def normalize_datetime_text(value: str) -> str:
    text = (value or "").strip().replace("T", " ")
    if not text:
        raise ValueError("回访系统登记时间不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"时间格式错误: {text}，应为 YYYY-MM-DD HH:MM:SS")


def _normalize_branches(branches: Iterable[str] | None) -> List[str]:
    raw = [x.strip() for x in (branches or []) if x and x.strip()]
    valid_values = {item["value"] for item in BRANCH_OPTIONS}
    return [value for value in raw if value in valid_values]


def _normalize_page(page: Any) -> int:
    try:
        return max(int(page), 1)
    except (TypeError, ValueError):
        return 1


def _normalize_page_size(page_size: Any) -> int:
    try:
        size = int(page_size)
    except (TypeError, ValueError):
        return 20
    return size if size in ALLOWED_PAGE_SIZES else 20


def defaults_payload() -> Dict[str, Any]:
    start_time, end_time = default_time_range()
    return {
        "success": True,
        "start_time": start_time,
        "end_time": end_time,
        "branches": [],
        "branch_options": BRANCH_OPTIONS,
        "page": 1,
        "page_size": 20,
    }


def query_fzxxlxxshf_records(
    *,
    start_time: str,
    end_time: str,
    branches: Iterable[str] | None = None,
    page: Any = 1,
    page_size: Any = 20,
) -> Dict[str, Any]:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    start_dt = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_text, "%Y-%m-%d %H:%M:%S")
    if start_dt > end_dt:
        raise ValueError("开始时间不能大于结束时间")

    current_page = _normalize_page(page)
    current_page_size = _normalize_page_size(page_size)
    branch_list = _normalize_branches(branches)

    conn = get_database_connection()
    try:
        records, total = query_fzxxlxxshf_page(
            conn,
            start_time=start_text,
            end_time=end_text,
            branches=branch_list,
            page=current_page,
            page_size=current_page_size,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    total_pages = (total + current_page_size - 1) // current_page_size if total > 0 else 1
    return {
        "success": True,
        "records": records,
        "count": len(records),
        "total": total,
        "page": current_page,
        "page_size": current_page_size,
        "total_pages": total_pages,
        "filters": {
            "start_time": start_text,
            "end_time": end_text,
            "branches": branch_list,
        },
        "branch_options": BRANCH_OPTIONS,
    }


def _sanitize_filename_text(text: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "-", (text or "").strip())


def _to_csv_bytes(records: List[Dict[str, Any]]) -> bytes:
    buf = io.StringIO()
    if records:
        cols = list(records[0].keys())
        writer = csv.writer(buf)
        writer.writerow(cols)
        for row in records:
            writer.writerow([row.get(c, "") for c in cols])
    return ("\ufeff" + buf.getvalue()).encode("utf-8")


def _to_xlsx_bytes(records: List[Dict[str, Any]]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "方正学校离校学生回访"
    if records:
        cols = list(records[0].keys())
        ws.append(cols)
        for row in records:
            ws.append([row.get(c, "") for c in cols])
    else:
        ws.append(["无数据"])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def export_fzxxlxxshf_records(
    *,
    fmt: str,
    start_time: str,
    end_time: str,
    branches: Iterable[str] | None = None,
) -> Tuple[bytes, str, str]:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    start_dt = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_text, "%Y-%m-%d %H:%M:%S")
    if start_dt > end_dt:
        raise ValueError("开始时间不能大于结束时间")

    branch_list = _normalize_branches(branches)
    conn = get_database_connection()
    try:
        records = query_fzxxlxxshf_all(
            conn,
            start_time=start_text,
            end_time=end_text,
            branches=branch_list,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    fmt_text = (fmt or "xlsx").lower().strip()
    if fmt_text not in ("xlsx", "csv"):
        raise ValueError("导出格式仅支持 xlsx/csv")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = (
        f"{_sanitize_filename_text(start_text)}_to_{_sanitize_filename_text(end_text)}"
        f"_方正学校离校学生回访_{timestamp}.{fmt_text}"
    )
    if fmt_text == "csv":
        return _to_csv_bytes(records), "text/csv; charset=utf-8", filename
    return (
        _to_xlsx_bytes(records),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename,
    )
