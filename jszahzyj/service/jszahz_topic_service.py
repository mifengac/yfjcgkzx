from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from jszahzyj.dao import jszahz_topic_dao
from jszahzyj.jszahz_topic_constants import PERSON_TYPE_OPTIONS, RISK_OPTIONS
from jszahzyj.service.common import normalize_text_list
from jszahzyj.service.jszahz_topic_base_excel_parser import parse_base_person_workbook
from jszahzyj.service.jszahz_topic_detail_page_service import (
    build_detail_export_xlsx,
    build_detail_page_payload,
)
from jszahzyj.service.jszahz_topic_detail_support import build_summary_export_filename
from jszahzyj.service.jszahz_topic_excel_parser import parse_person_type_workbook
from jszahzyj.service.jszahz_topic_merge import (
    build_detail_records,
    build_person_records,
    build_summary_records,
    filter_person_records,
)
from jszahzyj.service.jszahz_topic_payloads import records_to_xlsx_bytes
from jszahzyj.service.jszahz_topic_relation_service import (
    attach_relation_counts,
    initialize_relation_placeholders,
)


logger = logging.getLogger(__name__)

UPLOAD_API_VERSION = "jszahz-upload-20260414-v2"

def default_time_range() -> Tuple[str, str]:
    now = datetime.now()
    end_dt = datetime(now.year, now.month, now.day, 0, 0, 0)
    start_dt = datetime(2025, 1, 1, 0, 0, 0)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    )

def _serialize_batch(batch: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not batch:
        return None
    data = dict(batch)
    for key in ("created_at", "activated_at"):
        value = data.get(key)
        if isinstance(value, datetime):
            data[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    return data

def _serialize_active_batches(active_batches: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[Dict[str, Any]]]:
    batches = active_batches or jszahz_topic_dao.get_active_batches()
    return {
        "base_batch": _serialize_batch(batches.get("base_batch")),
        "tag_batch": _serialize_batch(batches.get("tag_batch")),
    }

def _normalize_bool(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in ("0", "false", "off", "no"):
        return False
    if text in ("1", "true", "on", "yes"):
        return True
    return default

def _normalize_filters(
    *,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
) -> Dict[str, Any]:
    return {
        "branch_codes": normalize_text_list(branch_codes),
        "person_types": normalize_text_list(person_types),
        "risk_labels": normalize_text_list(risk_labels),
        "managed_only": _normalize_bool(managed_only, default=True),
    }

def _progress(title: str, detail: str = "") -> Dict[str, Any]:
    return {
        "progress": True,
        "title": title,
        "detail": detail,
        "api_version": UPLOAD_API_VERSION,
    }

def _build_import_result_payload(
    *,
    source_kind: str,
    batch_id: int,
    imported_row_count: int,
    generated_tag_count: int,
    matched_person_count: int,
    active_batches: Dict[str, Optional[Dict[str, Any]]],
    tagged_person_count: Optional[int] = None,
    deduplicated_person_count: Optional[int] = None,
) -> Dict[str, Any]:
    payload = {
        "success": True,
        "api_version": UPLOAD_API_VERSION,
        "source_kind": source_kind,
        "batch_id": batch_id,
        "imported_row_count": imported_row_count,
        "generated_tag_count": generated_tag_count,
        "matched_person_count": matched_person_count,
        "active_batches": _serialize_active_batches(active_batches),
    }
    if tagged_person_count is not None:
        payload["tagged_person_count"] = tagged_person_count
    if deduplicated_person_count is not None:
        payload["deduplicated_person_count"] = deduplicated_person_count
    return payload

def _active_batch_ids() -> Tuple[Optional[int], Optional[int], Dict[str, Optional[Dict[str, Any]]]]:
    active_batches = jszahz_topic_dao.get_active_batches()
    base_batch = active_batches.get("base_batch")
    tag_batch = active_batches.get("tag_batch")
    base_batch_id = int(base_batch["id"]) if base_batch and base_batch.get("id") is not None else None
    tag_batch_id = int(tag_batch["id"]) if tag_batch and tag_batch.get("id") is not None else None
    return base_batch_id, tag_batch_id, active_batches

def _load_person_records(*, managed_only: bool) -> Tuple[List[Dict[str, Any]], Dict[str, Optional[Dict[str, Any]]]]:
    base_batch_id, tag_batch_id, active_batches = _active_batch_ids()
    records = build_person_records(
        base_rows=jszahz_topic_dao.query_base_person_rows(base_batch_id),
        live_rows=jszahz_topic_dao.query_live_person_rows(managed_only),
        tag_rows=jszahz_topic_dao.query_tag_rows(tag_batch_id),
    )
    return records, active_batches

def defaults_payload() -> Dict[str, Any]:
    return {
        "success": True,
        "managed_only": True,
        "branch_options": jszahz_topic_dao.query_branch_options(),
        "person_type_options": [{"value": item, "label": item} for item in PERSON_TYPE_OPTIONS],
        "risk_options": [{"value": item, "label": item} for item in RISK_OPTIONS],
        "active_batches": _serialize_active_batches(),
    }

def import_jszahz_base_excel_stream(*, file_bytes: bytes, filename: str, created_by: str):
    logger.warning(
        "JSZAHZ_UPLOAD_TRACE version=%s source_kind=base filename=%s created_by=%s",
        UPLOAD_API_VERSION,
        filename or "base.xlsx",
        created_by or "",
    )
    yield _progress("正在解析基础数据 Excel...")
    parsed = parse_base_person_workbook(io.BytesIO(file_bytes))
    yield _progress(
        "基础数据解析完成",
        f"共读取 {parsed.imported_row_count} 行，去重后 {parsed.deduplicated_person_count} 人",
    )

    batch_id = jszahz_topic_dao.create_pending_batch(
        source_file_name=filename or "base.xlsx",
        sheet_name="基础数据",
        created_by=created_by or "",
        source_kind=jszahz_topic_dao.SOURCE_KIND_BASE,
    )

    try:
        yield _progress("正在保存基础数据...", f"待写入 {parsed.deduplicated_person_count} 名去重人员")
        matched_person_count = jszahz_topic_dao.save_base_batch_data_and_activate(
            batch_id=batch_id,
            imported_row_count=parsed.imported_row_count,
            deduplicated_person_count=parsed.deduplicated_person_count,
            base_rows=parsed.rows,
        )
    except Exception as exc:
        jszahz_topic_dao.mark_batch_failed(
            batch_id=batch_id,
            imported_row_count=parsed.imported_row_count,
            generated_tag_count=0,
            error_message=str(exc),
        )
        logger.exception("jszahz base import failed: batch_id=%s file=%s", batch_id, filename)
        raise

    active_batches = jszahz_topic_dao.get_active_batches()
    yield _build_import_result_payload(
        source_kind=jszahz_topic_dao.SOURCE_KIND_BASE,
        batch_id=batch_id,
        imported_row_count=parsed.imported_row_count,
        generated_tag_count=0,
        matched_person_count=matched_person_count,
        deduplicated_person_count=parsed.deduplicated_person_count,
        active_batches=active_batches,
    )

def import_jszahz_tag_excel_stream(*, file_bytes: bytes, filename: str, created_by: str):
    logger.warning(
        "JSZAHZ_UPLOAD_TRACE version=%s source_kind=tag filename=%s created_by=%s",
        UPLOAD_API_VERSION,
        filename or "tags.xlsx",
        created_by or "",
    )
    yield _progress("正在解析标签 Excel...")
    parsed = parse_person_type_workbook(io.BytesIO(file_bytes))
    yield _progress(
        "标签数据解析完成",
        f"共读取 {parsed.imported_row_count} 行，生成 {parsed.generated_tag_count} 条标签，标记 {parsed.tagged_person_count} 人",
    )

    batch_id = jszahz_topic_dao.create_pending_batch(
        source_file_name=filename or "tags.xlsx",
        sheet_name="汇总",
        created_by=created_by or "",
        source_kind=jszahz_topic_dao.SOURCE_KIND_TAG,
    )

    try:
        yield _progress("正在保存标签数据...", f"待写入 {parsed.generated_tag_count} 条人员标签")
        matched_person_count = jszahz_topic_dao.save_tag_batch_data_and_activate(
            batch_id=batch_id,
            imported_row_count=parsed.imported_row_count,
            generated_tag_count=parsed.generated_tag_count,
            tagged_person_count=parsed.tagged_person_count,
            person_type_rows=parsed.rows,
        )
    except Exception as exc:
        jszahz_topic_dao.mark_batch_failed(
            batch_id=batch_id,
            imported_row_count=parsed.imported_row_count,
            generated_tag_count=parsed.generated_tag_count,
            error_message=str(exc),
        )
        logger.exception("jszahz tag import failed: batch_id=%s file=%s", batch_id, filename)
        raise

    active_batches = jszahz_topic_dao.get_active_batches()
    yield _build_import_result_payload(
        source_kind=jszahz_topic_dao.SOURCE_KIND_TAG,
        batch_id=batch_id,
        imported_row_count=parsed.imported_row_count,
        generated_tag_count=parsed.generated_tag_count,
        tagged_person_count=parsed.tagged_person_count,
        matched_person_count=matched_person_count,
        active_batches=active_batches,
    )

def import_jszahz_topic_excel_stream(*, file_bytes: bytes, filename: str, created_by: str):
    return import_jszahz_tag_excel_stream(
        file_bytes=file_bytes,
        filename=filename,
        created_by=created_by,
    )

def query_summary_payload(
    *,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
) -> Dict[str, Any]:
    filters = _normalize_filters(
        branch_codes=branch_codes,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
    )
    records, active_batches = _load_person_records(managed_only=filters["managed_only"])
    filtered = filter_person_records(
        records,
        branch_codes=filters["branch_codes"],
        person_types=filters["person_types"],
        risk_labels=filters["risk_labels"],
    )
    summary_rows = build_summary_records(filtered)
    total_count = sum(int(row.get("去重患者数") or 0) for row in summary_rows)
    if summary_rows:
        summary_rows = list(summary_rows)
        summary_rows.append(
            {
                "分局代码": "__ALL__",
                "分局名称": "汇总",
                "去重患者数": total_count,
            }
        )

    return {
        "success": True,
        "records": summary_rows,
        "count": total_count,
        "message": "" if summary_rows else "当前筛选条件下暂无数据",
        "filters": filters,
        "active_batches": _serialize_active_batches(active_batches),
    }

def query_detail_payload(
    *,
    branch_code: Optional[str],
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
    include_relation_counts: bool = True,
) -> Dict[str, Any]:
    filters = _normalize_filters(
        branch_codes=[branch_code] if branch_code and branch_code != "__ALL__" else [],
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
    )
    records, active_batches = _load_person_records(managed_only=filters["managed_only"])
    filtered = filter_person_records(
        records,
        branch_codes=filters["branch_codes"],
        person_types=filters["person_types"],
        risk_labels=filters["risk_labels"],
    )
    detail_records = build_detail_records(filtered)
    detail_records = (
        attach_relation_counts(detail_records)
        if include_relation_counts
        else initialize_relation_placeholders(detail_records)
    )

    return {
        "success": True,
        "records": detail_records,
        "count": len(detail_records),
        "message": "" if detail_records else "当前筛选条件下暂无明细数据",
        "filters": filters,
        "active_batches": _serialize_active_batches(active_batches),
    }

def query_detail_page_payload(
    *,
    branch_code: Optional[str],
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
    relation_types: Iterable[str] | None,
    page: Any,
    page_size: Any,
) -> Dict[str, Any]:
    return build_detail_page_payload(
        query_detail_payload_func=query_detail_payload,
        branch_code=branch_code,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
        relation_types=relation_types,
        page=page,
        page_size=page_size,
    )

def export_summary_xlsx(
    *,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
) -> Tuple[bytes, str]:
    payload = query_summary_payload(
        branch_codes=branch_codes,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
    )
    return (
        records_to_xlsx_bytes(payload["records"], "精神患者主题库汇总"),
        build_summary_export_filename(
            managed_only=payload["filters"]["managed_only"],
            person_types=payload["filters"]["person_types"],
            risk_labels=payload["filters"]["risk_labels"],
        ),
    )


def export_detail_xlsx(
    *,
    branch_code: Optional[str],
    branch_name: Optional[str] = None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    managed_only: Any,
    relation_types: Iterable[str] | None = None,
) -> Tuple[bytes, str]:
    return build_detail_export_xlsx(
        query_detail_payload_func=query_detail_payload,
        branch_code=branch_code,
        branch_name=branch_name,
        person_types=person_types,
        risk_labels=risk_labels,
        managed_only=managed_only,
        relation_types=relation_types,
    )
