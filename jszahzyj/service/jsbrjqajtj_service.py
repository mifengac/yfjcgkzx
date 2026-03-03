from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

from openpyxl import Workbook

from jszahzyj.dao.jsbrjqajtj_dao import (
    normalize_datetime_text,
    query_branch_options,
    query_jsbrjqajtj,
)


def default_time_range() -> Tuple[str, str]:
    now = datetime.now()
    end_dt = datetime(now.year, now.month, now.day, 0, 0, 0)
    start_dt = end_dt - timedelta(days=7)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _normalize_branch_options(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()
    for row in raw_rows or []:
        value = str((row.get("value") or "")).strip()
        label = str((row.get("label") or "")).strip()
        if not value or not label or value in seen:
            continue
        seen.add(value)
        out.append({"value": value, "label": label})
    return out


def query_jsbrjqajtj_records(
    *,
    start_time: str,
    end_time: str,
    branches: Iterable[str] | None = None,
) -> Dict[str, Any]:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    branch_list = [x.strip() for x in (branches or []) if x and x.strip()]
    records = query_jsbrjqajtj(
        start_time=start_text,
        end_time=end_text,
        branches=branch_list,
    )
    branch_options = _normalize_branch_options(query_branch_options())
    return {
        "success": True,
        "records": records,
        "count": len(records),
        "filters": {
            "start_time": start_text,
            "end_time": end_text,
            "branches": branch_list,
        },
        "branch_options": branch_options,
    }


def defaults_payload() -> Dict[str, Any]:
    start_time, end_time = default_time_range()
    return {
        "success": True,
        "start_time": start_time,
        "end_time": end_time,
        "branches": [],
        "branch_options": _normalize_branch_options(query_branch_options()),
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
    ws.title = "精神病人警情案件统计"
    if records:
        cols = list(records[0].keys())
        ws.append(cols)
        for row in records:
            ws.append([row.get(c, "") for c in cols])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def export_jsbrjqajtj_records(
    *,
    fmt: str,
    start_time: str,
    end_time: str,
    branches: Iterable[str] | None = None,
) -> Tuple[bytes, str, str]:
    payload = query_jsbrjqajtj_records(
        start_time=start_time,
        end_time=end_time,
        branches=branches,
    )
    records = payload["records"]
    start_text = payload["filters"]["start_time"]
    end_text = payload["filters"]["end_time"]

    fmt_text = (fmt or "xlsx").lower().strip()
    if fmt_text not in ("xlsx", "csv"):
        raise ValueError("导出格式仅支持 xlsx/csv")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = (
        f"{_sanitize_filename_text(start_text)}至{_sanitize_filename_text(end_text)}"
        f"_涉精神病人警情_{timestamp}.{fmt_text}"
    )
    if fmt_text == "csv":
        return _to_csv_bytes(records), "text/csv; charset=utf-8", filename
    return (
        _to_xlsx_bytes(records),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename,
    )

