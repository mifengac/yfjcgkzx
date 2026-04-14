from __future__ import annotations

import logging
import os
from typing import Any, BinaryIO, Dict, Iterable, Optional, Tuple

from gonggong.config.database import get_database_connection
from jszahzyj.dao import jszahz_topic_dao
from jszahzyj.service import jszahz_topic_import_engine, jszahz_topic_payloads
from jszahzyj.service.jszahz_topic_excel_parser import (
    PERSON_TYPE_OPTIONS,
    PERSON_TYPE_RULES,
    RISK_OPTIONS,
    ParsedImportResult,
    parse_person_type_workbook,
)
from jszahzyj.service.jszahz_topic_relation_service import (
    attach_relation_counts,
    initialize_relation_placeholders,
)


logger = logging.getLogger(__name__)


UPLOAD_API_VERSION = "jszahz-upload-20260414-v1"
LOADED_JSZAHZ_TOPIC_DAO_FILE = os.path.abspath(getattr(jszahz_topic_dao, "__file__", ""))


def default_time_range() -> Tuple[str, str]:
    return jszahz_topic_payloads.default_time_range()


def _log_import_start(*, mode: str, filename: str, created_by: str) -> None:
    logger.warning(
        "JSZAHZ_UPLOAD_TRACE version=%s mode=%s filename=%s created_by=%s dao_file=%s",
        UPLOAD_API_VERSION,
        mode,
        filename or "upload.xlsx",
        created_by or "",
        LOADED_JSZAHZ_TOPIC_DAO_FILE,
    )


def _mark_batch_failed(
    *,
    batch_id: int,
    parsed: Optional[ParsedImportResult],
    error_message: str,
) -> None:
    jszahz_topic_dao.mark_batch_failed(
        batch_id=batch_id,
        imported_row_count=parsed.imported_row_count if parsed else 0,
        generated_tag_count=parsed.generated_tag_count if parsed else 0,
        error_message=error_message,
    )


def _serialize_batch(batch: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return jszahz_topic_payloads.serialize_batch(batch)


def _build_import_payload(
    *,
    batch_id: int,
    parsed: ParsedImportResult,
    matched_person_count: int,
    active_batch: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return jszahz_topic_payloads.build_import_result_payload(
        api_version=UPLOAD_API_VERSION,
        batch_id=batch_id,
        parsed=parsed,
        matched_person_count=matched_person_count,
        active_batch=active_batch,
        serialize_batch_func=_serialize_batch,
    )


def defaults_payload() -> Dict[str, Any]:
    return jszahz_topic_payloads.build_defaults_payload(
        query_branch_options=jszahz_topic_dao.query_branch_options,
        get_active_batch=jszahz_topic_dao.get_active_batch,
        person_type_options=PERSON_TYPE_OPTIONS,
        risk_options=RISK_OPTIONS,
        serialize_batch_func=_serialize_batch,
    )


def import_jszahz_topic_excel(*, file_obj: BinaryIO, filename: str, created_by: str) -> Dict[str, Any]:
    return jszahz_topic_import_engine.run_import(
        file_obj=file_obj,
        filename=filename,
        created_by=created_by,
        parse_workbook=parse_person_type_workbook,
        create_pending_batch=jszahz_topic_dao.create_pending_batch,
        save_batch_data_and_activate=jszahz_topic_dao.save_batch_data_and_activate,
        mark_batch_failed_func=_mark_batch_failed,
        get_active_batch=jszahz_topic_dao.get_active_batch,
        build_import_result_payload_func=_build_import_payload,
        log_import_start=_log_import_start,
        logger=logger,
    )


def _progress(title: str, detail: str = "") -> Dict[str, Any]:
    return jszahz_topic_payloads.build_progress_payload(
        api_version=UPLOAD_API_VERSION,
        title=title,
        detail=detail,
    )


def import_jszahz_topic_excel_stream(
    *, file_bytes: bytes, filename: str, created_by: str
):
    return jszahz_topic_import_engine.run_stream_import(
        file_bytes=file_bytes,
        filename=filename,
        created_by=created_by,
        parse_workbook=parse_person_type_workbook,
        create_pending_batch=jszahz_topic_dao.create_pending_batch,
        get_database_connection=get_database_connection,
        insert_person_types_on_cursor=jszahz_topic_dao.insert_person_types_on_cursor,
        rebuild_snapshot_on_cursor=jszahz_topic_dao.rebuild_snapshot_on_cursor,
        update_snapshot_tags_on_cursor=jszahz_topic_dao.update_snapshot_tags_on_cursor,
        activate_batch_on_cursor=jszahz_topic_dao.activate_batch_on_cursor,
        mark_batch_failed_func=_mark_batch_failed,
        get_active_batch=jszahz_topic_dao.get_active_batch,
        build_progress_payload_func=_progress,
        build_import_result_payload_func=_build_import_payload,
        log_import_start=_log_import_start,
        logger=logger,
    )


def _normalize_filters(
    *,
    start_time: str,
    end_time: str,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> Dict[str, Any]:
    return jszahz_topic_payloads.normalize_filters(
        normalize_datetime_text=jszahz_topic_dao.normalize_datetime_text,
        start_time=start_time,
        end_time=end_time,
        branch_codes=branch_codes,
        person_types=person_types,
        risk_labels=risk_labels,
    )


def query_summary_payload(
    *,
    start_time: str,
    end_time: str,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> Dict[str, Any]:
    return jszahz_topic_payloads.build_summary_payload(
        get_active_batch=jszahz_topic_dao.get_active_batch,
        query_summary_rows=jszahz_topic_dao.query_summary_rows,
        normalize_datetime_text=jszahz_topic_dao.normalize_datetime_text,
        start_time=start_time,
        end_time=end_time,
        branch_codes=branch_codes,
        person_types=person_types,
        risk_labels=risk_labels,
        serialize_batch_func=_serialize_batch,
    )


def query_detail_payload(
    *,
    branch_code: Optional[str],
    start_time: str,
    end_time: str,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
    include_relation_counts: bool = True,
) -> Dict[str, Any]:
    return jszahz_topic_payloads.build_detail_payload(
        get_active_batch=jszahz_topic_dao.get_active_batch,
        query_detail_rows=jszahz_topic_dao.query_detail_rows,
        normalize_datetime_text=jszahz_topic_dao.normalize_datetime_text,
        attach_relation_counts_func=attach_relation_counts,
        initialize_relation_placeholders_func=initialize_relation_placeholders,
        branch_code=branch_code,
        start_time=start_time,
        end_time=end_time,
        person_types=person_types,
        risk_labels=risk_labels,
        include_relation_counts=include_relation_counts,
        serialize_batch_func=_serialize_batch,
    )


def export_summary_xlsx(
    *,
    start_time: str,
    end_time: str,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> Tuple[bytes, str]:
    return jszahz_topic_payloads.export_summary_xlsx(
        build_summary_payload_func=query_summary_payload,
        start_time=start_time,
        end_time=end_time,
        branch_codes=branch_codes,
        person_types=person_types,
        risk_labels=risk_labels,
    )


def export_detail_xlsx(
    *,
    branch_code: Optional[str],
    start_time: str,
    end_time: str,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> Tuple[bytes, str]:
    return jszahz_topic_payloads.export_detail_xlsx(
        build_detail_payload_func=query_detail_payload,
        branch_code=branch_code,
        start_time=start_time,
        end_time=end_time,
        person_types=person_types,
        risk_labels=risk_labels,
    )
