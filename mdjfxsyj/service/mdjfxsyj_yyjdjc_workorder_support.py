from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Mapping, Sequence, Tuple


WORKORDER_COLUMNS: Tuple[str, ...] = (
    "\u5de5\u5355\u7f16\u53f7",
    "\u59d3\u540d",
    "\u7535\u8bdd",
    "\u5de5\u5355\u6807\u9898",
    "\u95ee\u9898\u63cf\u8ff0",
    "\u8bc9\u6c42\u8981\u70b9",
    "\u767b\u8bb0\u65f6\u95f4",
    "\u5730\u5740",
    "\u6d89\u4e8b\u5355\u4f4d",
    "\u5de5\u5355\u72b6\u6001",
    "\u547d\u4e2d\u5173\u952e\u8bcd",
)


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def format_workorder_query_range(start_dt: datetime, end_dt: datetime) -> Tuple[str, str]:
    return start_dt.strftime("%Y%m%d%H%M%S"), end_dt.strftime("%Y%m%d%H%M%S")


def _extract_caseaddr_text(value: Any) -> str:
    text = _stringify_value(value).strip()
    if not text:
        return ""

    try:
        parsed = json.loads(text)
    except Exception:
        return text

    if isinstance(parsed, dict):
        address = _stringify_value(parsed.get("address")).strip()
        if address:
            return address
    return text


def _format_registertime(value: Any) -> str:
    text = _stringify_value(value).strip()
    if len(text) == 14 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]} {text[8:10]}:{text[10:12]}:{text[12:14]}"
    return text


def build_workorder_display_row(raw_row: Mapping[str, Any], matched_keywords: Sequence[str]) -> Dict[str, str]:
    return {
        WORKORDER_COLUMNS[0]: _stringify_value(raw_row.get("orderid")),
        WORKORDER_COLUMNS[1]: _stringify_value(raw_row.get("name")),
        WORKORDER_COLUMNS[2]: _stringify_value(raw_row.get("mobile")),
        WORKORDER_COLUMNS[3]: _stringify_value(raw_row.get("ordertitle")),
        WORKORDER_COLUMNS[4]: _stringify_value(raw_row.get("ordercont")),
        WORKORDER_COLUMNS[5]: _stringify_value(raw_row.get("keyword")),
        WORKORDER_COLUMNS[6]: _format_registertime(raw_row.get("registertime")),
        WORKORDER_COLUMNS[7]: _extract_caseaddr_text(raw_row.get("caseaddr")),
        WORKORDER_COLUMNS[8]: _stringify_value(raw_row.get("objectname")),
        WORKORDER_COLUMNS[9]: _stringify_value(raw_row.get("orderstatuscd")),
        WORKORDER_COLUMNS[10]: "\u3001".join(_stringify_value(keyword) for keyword in matched_keywords if keyword),
    }
