from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from jszahzyj.dao import jszahz_topic_relation_dao
from jszahzyj.service.jszahz_topic_detail_support import (
    build_detail_export_filename,
    build_filter_summary_lines,
    build_multi_sheet_xlsx_bytes,
    paginate_records,
)
from jszahzyj.service.jszahz_topic_relation_service import (
    RELATION_TYPES,
    get_relation_columns,
    list_relation_type_options,
    normalize_relation_types,
)


def build_detail_page_payload(
    *,
    query_detail_payload_func: Callable[..., Dict[str, Any]],
    branch_code: Optional[str],
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
    relation_types: Iterable[str] | None,
    page: Any,
    page_size: Any,
) -> Dict[str, Any]:
    payload = query_detail_payload_func(
        branch_code=branch_code,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
        include_relation_counts=False,
    )
    selected_relation_types = normalize_relation_types(relation_types)
    pagination = paginate_records(payload["records"], page=page, page_size=page_size)
    payload["filters"]["relation_types"] = selected_relation_types
    payload.update(
        {
            "records": pagination["records"],
            "count": pagination["total_count"],
            "page": pagination["page"],
            "page_size": pagination["page_size"],
            "page_size_value": pagination["page_size_value"],
            "page_size_label": pagination["page_size_label"],
            "page_record_count": pagination["page_record_count"],
            "total_pages": pagination["total_pages"],
            "page_numbers": pagination["page_numbers"],
            "has_previous": pagination["has_previous"],
            "has_next": pagination["has_next"],
            "previous_page": pagination["previous_page"],
            "next_page": pagination["next_page"],
            "page_size_options": pagination["page_size_options"],
            "selected_relation_types": selected_relation_types,
            "relation_type_options": list_relation_type_options(selected_relation_types),
            "visible_relation_columns": get_relation_columns(selected_relation_types),
        }
    )
    return payload


def build_detail_export_xlsx(
    *,
    query_detail_payload_func: Callable[..., Dict[str, Any]],
    branch_code: Optional[str],
    branch_name: Optional[str] = None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
    relation_types: Iterable[str] | None = None,
) -> Tuple[bytes, str]:
    payload = query_detail_payload_func(
        branch_code=branch_code,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
        include_relation_counts=False,
    )
    selected_relation_types = normalize_relation_types(relation_types)
    resolved_branch_name = _resolve_branch_name(branch_code, branch_name, payload["records"])
    relation_labels = [RELATION_TYPES[item]["column"] for item in selected_relation_types]
    relation_sheets = _build_relation_sheet_records(payload["records"], selected_relation_types)
    summary_lines = build_filter_summary_lines(
        branch_name=resolved_branch_name,
        managed_only=payload["filters"]["managed_only"],
        person_types=payload["filters"]["person_types"],
        risk_labels=payload["filters"]["risk_labels"],
        relation_labels=relation_labels,
    )
    return (
        build_multi_sheet_xlsx_bytes(
            detail_records=payload["records"],
            relation_sheets=relation_sheets,
            summary_lines=summary_lines,
        ),
        build_detail_export_filename(
            branch_name=resolved_branch_name,
            managed_only=payload["filters"]["managed_only"],
            person_types=payload["filters"]["person_types"],
            risk_labels=payload["filters"]["risk_labels"],
            relation_types=selected_relation_types,
            relation_type_configs=RELATION_TYPES,
        ),
    )


def _resolve_branch_name(branch_code: Optional[str], branch_name: Optional[str], detail_records: List[Dict[str, Any]]) -> str:
    text = str(branch_name or "").strip()
    if text:
        return text
    if not branch_code or branch_code == "__ALL__":
        return "汇总"
    branch_values = {
        str(item.get("分局") or "").strip()
        for item in (detail_records or [])
        if str(item.get("分局") or "").strip()
    }
    if len(branch_values) == 1:
        return next(iter(branch_values))
    return "指定分局"


def _build_relation_sheet_records(
    detail_records: List[Dict[str, Any]],
    relation_types: List[str],
) -> List[Dict[str, Any]]:
    person_index: Dict[str, Dict[str, Any]] = {}
    ordered_zjhms: List[str] = []
    for item in detail_records or []:
        zjhm = str(item.get("身份证号") or "").strip().upper()
        if not zjhm or zjhm in person_index:
            continue
        person_index[zjhm] = item
        ordered_zjhms.append(zjhm)

    sheet_definitions: List[Dict[str, Any]] = []
    for relation_type in relation_types:
        grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
        raw_rows = jszahz_topic_relation_dao.query_relation_detail_rows_batch(relation_type, ordered_zjhms)
        for row in raw_rows:
            zjhm = str(row.get("身份证号") or "").strip().upper()
            if not zjhm or zjhm not in person_index:
                continue
            grouped_rows.setdefault(zjhm, []).append(
                {key: value for key, value in row.items() if key != "身份证号"}
            )

        relation_records: List[Dict[str, Any]] = []
        for zjhm in ordered_zjhms:
            person = person_index[zjhm]
            for row in grouped_rows.get(zjhm, []):
                relation_record = {
                    "患者姓名": person.get("姓名") or "",
                    "患者身份证号": zjhm,
                    "患者分局": person.get("分局") or "",
                    "患者列管单位": person.get("列管单位") or "",
                    "患者人员风险": person.get("人员风险") or "",
                    "患者人员类型": person.get("人员类型") or "",
                }
                relation_record.update(row)
                relation_records.append(relation_record)

        sheet_definitions.append(
            {
                "title": RELATION_TYPES[relation_type]["sheet"],
                "records": relation_records,
            }
        )

    return sheet_definitions
