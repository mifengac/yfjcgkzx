from __future__ import annotations

import io
from datetime import datetime
from math import ceil
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from openpyxl import Workbook

from jszahzyj.service.common import sanitize_filename_text


DEFAULT_DETAIL_PAGE_SIZE = 20
DETAIL_PAGE_SIZE_VALUE_ALL = "all"
DETAIL_PAGE_SIZE_CHOICES = (
    ("20", "20条", 20),
    ("50", "50条", 50),
    ("100", "100条", 100),
    (DETAIL_PAGE_SIZE_VALUE_ALL, "全部", None),
)


def normalize_page_number(value: Any, *, default: int = 1) -> int:
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


def normalize_page_size_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    valid_values = {item[0] for item in DETAIL_PAGE_SIZE_CHOICES}
    return text if text in valid_values else str(DEFAULT_DETAIL_PAGE_SIZE)


def build_page_size_options(selected_value: str) -> List[Dict[str, Any]]:
    normalized_value = normalize_page_size_value(selected_value)
    return [
        {
            "value": value,
            "label": label,
            "selected": value == normalized_value,
        }
        for value, label, _limit in DETAIL_PAGE_SIZE_CHOICES
    ]


def paginate_records(
    records: Sequence[Dict[str, Any]] | None,
    *,
    page: Any,
    page_size: Any,
) -> Dict[str, Any]:
    all_records = list(records or [])
    total_count = len(all_records)
    selected_page_size = normalize_page_size_value(page_size)
    page_size_label = next(
        (label for value, label, _limit in DETAIL_PAGE_SIZE_CHOICES if value == selected_page_size),
        str(DEFAULT_DETAIL_PAGE_SIZE),
    )
    current_page = normalize_page_number(page)
    limit = next(
        (item_limit for value, _label, item_limit in DETAIL_PAGE_SIZE_CHOICES if value == selected_page_size),
        DEFAULT_DETAIL_PAGE_SIZE,
    )

    if not total_count:
        return {
            "records": [],
            "page": 1,
            "page_size": limit,
            "page_size_value": selected_page_size,
            "page_size_label": page_size_label,
            "page_record_count": 0,
            "total_count": 0,
            "total_pages": 1,
            "page_numbers": [1],
            "has_previous": False,
            "has_next": False,
            "previous_page": 1,
            "next_page": 1,
            "page_size_options": build_page_size_options(selected_page_size),
        }

    if limit is None:
        page_records = all_records
        total_pages = 1
        current_page = 1
    else:
        total_pages = max(1, ceil(total_count / limit))
        current_page = min(current_page, total_pages)
        start = (current_page - 1) * limit
        end = start + limit
        page_records = all_records[start:end]

    return {
        "records": page_records,
        "page": current_page,
        "page_size": limit,
        "page_size_value": selected_page_size,
        "page_size_label": page_size_label,
        "page_record_count": len(page_records),
        "total_count": total_count,
        "total_pages": total_pages,
        "page_numbers": build_page_numbers(current_page, total_pages),
        "has_previous": current_page > 1,
        "has_next": current_page < total_pages,
        "previous_page": current_page - 1 if current_page > 1 else 1,
        "next_page": current_page + 1 if current_page < total_pages else total_pages,
        "page_size_options": build_page_size_options(selected_page_size),
    }


def build_page_numbers(current_page: int, total_pages: int, *, radius: int = 2) -> List[int]:
    if total_pages <= 1:
        return [1]
    start = max(1, current_page - radius)
    end = min(total_pages, current_page + radius)
    return list(range(start, end + 1))


def build_filter_segment(values: Iterable[str] | None, fallback: str) -> str:
    items = [str(item).strip() for item in (values or []) if str(item).strip()]
    if not items:
        return fallback
    return "_".join(sanitize_filename_text(item) for item in items)


def build_relation_scope_label(
    relation_types: Iterable[str],
    relation_type_configs: Mapping[str, Mapping[str, str]],
) -> str:
    selected = [item for item in relation_types if item in relation_type_configs]
    if not selected or len(selected) == len(relation_type_configs):
        return "全部关联"
    return "_".join(
        sanitize_filename_text(relation_type_configs[item].get("short_label") or item)
        for item in selected
    )


def build_detail_export_filename(
    *,
    branch_name: str,
    managed_only: bool,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    relation_types: Iterable[str],
    relation_type_configs: Mapping[str, Mapping[str, str]],
) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    branch_scope = sanitize_filename_text(branch_name.strip() or "汇总")
    managed_scope = "已列管" if managed_only else "全部人员"
    return (
        f"精神患者主题库详情_"
        f"{branch_scope}_"
        f"{managed_scope}_"
        f"{build_filter_segment(person_types, '全部类型')}_"
        f"{build_filter_segment(risk_labels, '全部风险')}_"
        f"{build_relation_scope_label(relation_types, relation_type_configs)}_"
        f"{timestamp}.xlsx"
    )


def build_summary_export_filename(
    *,
    managed_only: bool,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> str:
    scope = "已列管" if managed_only else "全部人员"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        f"{sanitize_filename_text(scope)}_"
        f"{build_filter_segment(person_types, '全部类型')}_"
        f"{build_filter_segment(risk_labels, '全部风险')}_"
        f"精神障碍患者_{timestamp}.xlsx"
    )


def build_filter_summary_lines(
    *,
    branch_name: str,
    managed_only: bool,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    relation_labels: Iterable[str],
) -> List[str]:
    person_type_text = "、".join(item for item in (person_types or []) if item) or "全部类型"
    risk_text = "、".join(item for item in (risk_labels or []) if item) or "全部风险"
    relation_text = "、".join(item for item in (relation_labels or []) if item) or "全部关联"
    managed_text = "已列管" if managed_only else "全部人员"
    export_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        f"筛选条件：分局={branch_name.strip() or '汇总'}；列管范围={managed_text}；人员类型={person_type_text}；人员风险={risk_text}；关联类型={relation_text}",
        f"导出时间：{export_time}",
    ]


def build_multi_sheet_xlsx_bytes(
    *,
    detail_records: Sequence[Dict[str, Any]],
    relation_sheets: Sequence[Dict[str, Any]],
    summary_lines: Sequence[str] | None = None,
) -> bytes:
    workbook = Workbook()
    patient_sheet = workbook.active
    patient_sheet.title = "患者明细"
    _write_sheet(patient_sheet, detail_records, summary_lines=summary_lines)

    for sheet in relation_sheets:
        worksheet = workbook.create_sheet(title=str(sheet.get("title") or "Sheet"))
        _write_sheet(worksheet, sheet.get("records") or [])

    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


def _write_sheet(worksheet, records: Sequence[Dict[str, Any]], *, summary_lines: Sequence[str] | None = None) -> None:
    for line in summary_lines or []:
        worksheet.append([line])
    if summary_lines:
        worksheet.append([])

    rows = list(records or [])
    if not rows:
        worksheet.append(["暂无数据"])
        return

    headers = list(rows[0].keys())
    worksheet.append(headers)
    for row in rows:
        worksheet.append([row.get(header, "") for header in headers])
