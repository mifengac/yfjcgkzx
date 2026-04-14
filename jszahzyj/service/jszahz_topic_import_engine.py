from __future__ import annotations

import io
from datetime import datetime
from typing import Any, BinaryIO, Callable, Dict, Optional


def run_import(
    *,
    file_obj: BinaryIO,
    filename: str,
    created_by: str,
    parse_workbook: Callable[[BinaryIO], Any],
    create_pending_batch: Callable[..., int],
    save_batch_data_and_activate: Callable[..., int],
    mark_batch_failed_func: Callable[..., None],
    get_active_batch: Callable[[], Optional[Dict[str, Any]]],
    build_import_result_payload_func: Callable[..., Dict[str, Any]],
    log_import_start: Callable[..., None],
    logger,
) -> Dict[str, Any]:
    log_import_start(mode="sync", filename=filename, created_by=created_by)
    started_at = datetime.now()
    batch_id = create_pending_batch(
        source_file_name=filename or "upload.xlsx",
        sheet_name="汇总",
        created_by=created_by or "",
    )

    try:
        parsed = parse_workbook(file_obj)
        logger.info(
            "jszahz topic import parsed: batch_id=%s file=%s imported_rows=%s generated_tags=%s",
            batch_id,
            filename,
            parsed.imported_row_count,
            parsed.generated_tag_count,
        )
        matched_person_count = save_batch_data_and_activate(
            batch_id=batch_id,
            imported_row_count=parsed.imported_row_count,
            person_type_rows=parsed.rows,
            zjhm_list=parsed.all_zjhms,
        )
    except Exception as exc:
        parsed = locals().get("parsed")
        mark_batch_failed_func(batch_id=batch_id, parsed=parsed, error_message=str(exc))
        logger.exception("jszahz topic import failed: batch_id=%s file=%s", batch_id, filename)
        raise

    active_batch = get_active_batch()
    elapsed_seconds = (datetime.now() - started_at).total_seconds()
    logger.info(
        "jszahz topic import completed: batch_id=%s file=%s matched_person_count=%s elapsed_seconds=%.3f",
        batch_id,
        filename,
        matched_person_count,
        elapsed_seconds,
    )
    return build_import_result_payload_func(
        batch_id=batch_id,
        parsed=parsed,
        matched_person_count=matched_person_count,
        active_batch=active_batch,
    )


def run_stream_import(
    *,
    file_bytes: bytes,
    filename: str,
    created_by: str,
    parse_workbook: Callable[[BinaryIO], Any],
    create_pending_batch: Callable[..., int],
    get_database_connection: Callable[[], Any],
    insert_person_types_on_cursor: Callable[..., None],
    rebuild_snapshot_on_cursor: Callable[..., int],
    update_snapshot_tags_on_cursor: Callable[..., int],
    activate_batch_on_cursor: Callable[..., None],
    mark_batch_failed_func: Callable[..., None],
    get_active_batch: Callable[[], Optional[Dict[str, Any]]],
    build_progress_payload_func: Callable[..., Dict[str, Any]],
    build_import_result_payload_func: Callable[..., Dict[str, Any]],
    log_import_start: Callable[..., None],
    logger,
):
    log_import_start(mode="stream", filename=filename, created_by=created_by)
    started_at = datetime.now()

    yield build_progress_payload_func(title="正在解析 Excel 文件...")
    parsed = parse_workbook(io.BytesIO(file_bytes))
    yield build_progress_payload_func(
        title="Excel 解析完成",
        detail=f"共读取 {parsed.imported_row_count} 行，生成 {parsed.generated_tag_count} 条标签",
    )

    batch_id = create_pending_batch(
        source_file_name=filename or "upload.xlsx",
        sheet_name="汇总",
        created_by=created_by or "",
    )
    logger.info(
        "jszahz topic stream import parsed: batch_id=%s file=%s imported_rows=%s",
        batch_id,
        filename,
        parsed.imported_row_count,
    )

    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("SET LOCAL lock_timeout = '5000ms'")
                cur.execute("SET LOCAL statement_timeout = '120000ms'")
            except Exception:
                conn.rollback()

            yield build_progress_payload_func(
                title=f"正在保存 {parsed.generated_tag_count} 条人员标签...",
            )
            insert_person_types_on_cursor(cur, batch_id, parsed.rows)

            yield build_progress_payload_func(
                title="正在合并 Excel 与主表患者数据",
                detail=f"共 {len(parsed.all_zjhms)} 名去重患者，正在查询主表...",
            )
            patient_count = rebuild_snapshot_on_cursor(cur, batch_id, parsed.all_zjhms)
            yield build_progress_payload_func(
                title=f"已合并 {patient_count} 名患者数据",
                detail="正在匹配人员类型标签...",
            )

            matched = update_snapshot_tags_on_cursor(cur, batch_id)
            yield build_progress_payload_func(
                title=f"标签匹配完成，共匹配 {matched} 名患者",
                detail="正在切换生效批次...",
            )

            activate_batch_on_cursor(
                cur,
                batch_id,
                parsed.imported_row_count,
                parsed.generated_tag_count,
                matched,
            )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        mark_batch_failed_func(batch_id=batch_id, parsed=parsed, error_message=str(exc))
        logger.exception("jszahz topic stream import failed: batch_id=%s", batch_id)
        raise
    finally:
        conn.close()

    active_batch = get_active_batch()
    elapsed = (datetime.now() - started_at).total_seconds()
    logger.info(
        "jszahz topic stream import completed: batch_id=%s matched=%s elapsed=%.3f",
        batch_id,
        matched,
        elapsed,
    )
    yield build_import_result_payload_func(
        batch_id=batch_id,
        parsed=parsed,
        matched_person_count=matched,
        active_batch=active_batch,
    )