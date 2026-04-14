from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg2.extras import execute_values

from gonggong.config.database import execute_query, get_database_connection
from jszahzyj.jszahz_topic_constants import TOPIC_BRANCH_CODES


SOURCE_KIND_BASE = "base"
SOURCE_KIND_TAG = "tag"


def _apply_import_timeouts(cur, conn) -> None:
    try:
        cur.execute("SET LOCAL lock_timeout = '5000ms'")
        cur.execute("SET LOCAL statement_timeout = '120000ms'")
    except Exception:
        conn.rollback()


def normalize_datetime_text(value: str) -> str:
    text = (value or "").strip().replace("T", " ")
    if not text:
        raise ValueError("时间不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"时间格式错误: {text}，应为 YYYY-MM-DD HH:MM:SS")


def query_branch_options() -> List[Dict[str, Any]]:
    sql = """
    SELECT DISTINCT ON (d.ssfjdm)
        d.ssfjdm AS value,
        d.ssfj AS label
    FROM "stdata"."b_dic_zzjgdm" d
    WHERE d.ssfjdm = ANY(%s)
      AND d.ssfj IS NOT NULL
    ORDER BY d.ssfjdm, d.ssfj NULLS LAST
    """
    return execute_query(sql, (list(TOPIC_BRANCH_CODES),))


def get_active_batch(source_kind: str) -> Optional[Dict[str, Any]]:
    sql = """
    SELECT
        id,
        source_kind,
        source_file_name,
        sheet_name,
        import_status,
        is_active,
        imported_row_count,
        matched_person_count,
        generated_tag_count,
        created_by,
        error_message,
        created_at,
        activated_at
    FROM "jcgkzx_monitor"."jszahz_topic_batch"
    WHERE source_kind = %s
      AND is_active = TRUE
      AND import_status = 'success'
    ORDER BY activated_at DESC NULLS LAST, created_at DESC, id DESC
    LIMIT 1
    """
    rows = execute_query(sql, (source_kind,))
    return rows[0] if rows else None


def get_active_batches() -> Dict[str, Optional[Dict[str, Any]]]:
    return {
        "base_batch": get_active_batch(SOURCE_KIND_BASE),
        "tag_batch": get_active_batch(SOURCE_KIND_TAG),
    }


def create_pending_batch(
    *,
    source_file_name: str,
    sheet_name: str,
    created_by: str,
    source_kind: str,
) -> int:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "jcgkzx_monitor"."jszahz_topic_batch" (
                    source_kind,
                    source_file_name,
                    sheet_name,
                    import_status,
                    is_active,
                    imported_row_count,
                    matched_person_count,
                    generated_tag_count,
                    created_by,
                    created_at
                )
                VALUES (%s, %s, %s, 'pending', FALSE, 0, 0, 0, %s, CURRENT_TIMESTAMP)
                RETURNING id
                """,
                (source_kind, source_file_name, sheet_name, created_by),
            )
            batch_id = int(cur.fetchone()[0])
        conn.commit()
        return batch_id
    finally:
        conn.close()


def mark_batch_failed(
    *,
    batch_id: int,
    imported_row_count: int,
    generated_tag_count: int,
    error_message: str,
) -> None:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
                SET import_status = 'failed',
                    is_active = FALSE,
                    imported_row_count = %s,
                    generated_tag_count = %s,
                    matched_person_count = 0,
                    error_message = %s
                WHERE id = %s
                """,
                (imported_row_count, generated_tag_count, str(error_message or "导入失败")[:1000], batch_id),
            )
        conn.commit()
    finally:
        conn.close()


def query_base_person_rows(batch_id: Optional[int]) -> List[Dict[str, Any]]:
    if not batch_id:
        return []

    sql = """
    SELECT
        p.zjhm AS zjhm,
        COALESCE(p.xm, '') AS xm,
        p.ssfjdm AS ssfjdm,
        COALESCE(d.ssfj, p.source_sheet_name) AS ssfj,
        COALESCE(p.source_sheet_name, '') AS source_sheet_name
    FROM "jcgkzx_monitor"."jszahz_topic_base_person" p
    LEFT JOIN (
        SELECT DISTINCT ON (d.ssfjdm)
            d.ssfjdm,
            d.ssfj
        FROM "stdata"."b_dic_zzjgdm" d
        WHERE d.ssfjdm IS NOT NULL
        ORDER BY d.ssfjdm, d.ssfj NULLS LAST
    ) d
        ON d.ssfjdm = p.ssfjdm
    WHERE p.batch_id = %s
    ORDER BY p.id
    """
    return execute_query(sql, (batch_id,))


def query_live_person_rows(managed_only: bool) -> List[Dict[str, Any]]:
    sql = """
    WITH live_people AS (
        SELECT
            UPPER(BTRIM(p.zjhm)) AS zjhm,
            COALESCE(BTRIM(p.xm), '') AS xm,
            p.lgsj AS lgsj,
            p.xgsj AS xgsj,
            p.djsj AS djsj,
            p.systemid AS systemid,
            COALESCE(BTRIM(p.lgdw), '') AS lgdw,
            COALESCE(BTRIM(p.sflg), '') AS sflg,
            CASE
                WHEN p.fxdj = '00' THEN '0级患者'
                WHEN p.fxdj = '01' THEN '1级患者'
                WHEN p.fxdj = '02' THEN '2级患者'
                WHEN p.fxdj = '03' THEN '3级患者'
                WHEN p.fxdj = '04' THEN '4级患者'
                WHEN p.fxdj = '05' THEN '5级患者'
                ELSE '无数据'
            END AS fxdj_label,
            COALESCE(d1.ssfjdm, d2.ssfjdm) AS ssfjdm,
            COALESCE(d1.ssfj, d2.ssfj) AS ssfj
        FROM "stdata"."b_per_jszahzryxxwh" p
        LEFT JOIN (
            SELECT DISTINCT ON (d.sspcsdm)
                d.sspcsdm,
                d.ssfjdm,
                d.ssfj
            FROM "stdata"."b_dic_zzjgdm" d
            WHERE d.sspcsdm IS NOT NULL
            ORDER BY d.sspcsdm, d.ssfjdm NULLS LAST, d.ssfj NULLS LAST
        ) d1
            ON p.lgdw = d1.sspcsdm
        LEFT JOIN (
            SELECT DISTINCT ON (d.ssfjdm)
                d.ssfjdm,
                d.ssfj
            FROM "stdata"."b_dic_zzjgdm" d
            WHERE d.ssfjdm IS NOT NULL
            ORDER BY d.ssfjdm, d.ssfj NULLS LAST
        ) d2
            ON (substring(COALESCE(p.lgdw, ''), 1, 6) || '000000') = d2.ssfjdm
        WHERE p.deleteflag = '0'
          AND p.zjhm IS NOT NULL
          AND BTRIM(p.zjhm) <> ''
          AND (NOT %s OR p.sflg = '1')
    )
    SELECT DISTINCT ON (lp.zjhm)
        lp.zjhm,
        lp.xm,
        lp.lgsj,
        lp.xgsj,
        lp.djsj,
        lp.systemid,
        lp.lgdw,
        lp.sflg,
        lp.fxdj_label,
        lp.ssfjdm,
        lp.ssfj
    FROM live_people lp
    ORDER BY lp.zjhm,
             lp.lgsj DESC NULLS LAST,
             lp.xgsj DESC NULLS LAST,
             lp.djsj DESC NULLS LAST,
             lp.systemid DESC
    """
    return execute_query(sql, (managed_only,))


def query_tag_rows(batch_id: Optional[int]) -> List[Dict[str, Any]]:
    if not batch_id:
        return []

    sql = """
    SELECT
        UPPER(BTRIM(zjhm)) AS zjhm,
        person_type,
        source_row_no
    FROM "jcgkzx_monitor"."jszahz_topic_person_type"
    WHERE batch_id = %s
    ORDER BY UPPER(BTRIM(zjhm)), source_row_no, id
    """
    return execute_query(sql, (batch_id,))


def replace_base_people_on_cursor(cur, batch_id: int, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    execute_values(
        cur,
        """
        INSERT INTO "jcgkzx_monitor"."jszahz_topic_base_person" (
            batch_id, zjhm, xm, ssfjdm, source_sheet_name, source_row_no, source_seq_no
        )
        VALUES %s
        ON CONFLICT (batch_id, zjhm) DO UPDATE
        SET xm = EXCLUDED.xm,
            ssfjdm = EXCLUDED.ssfjdm,
            source_sheet_name = EXCLUDED.source_sheet_name,
            source_row_no = EXCLUDED.source_row_no,
            source_seq_no = EXCLUDED.source_seq_no
        """,
        [
            (
                batch_id,
                str(item["zjhm"]),
                str(item.get("xm") or ""),
                str(item.get("ssfjdm") or ""),
                str(item.get("source_sheet_name") or ""),
                int(item.get("source_row_no") or 0),
                str(item.get("source_seq_no") or ""),
            )
            for item in rows
        ],
        page_size=500,
    )


def insert_person_types_on_cursor(cur, batch_id: int, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    execute_values(
        cur,
        """
        INSERT INTO "jcgkzx_monitor"."jszahz_topic_person_type" (
            batch_id, zjhm, person_type, source_row_no
        )
        VALUES %s
        ON CONFLICT (batch_id, zjhm, person_type) DO NOTHING
        """,
        [
            (
                batch_id,
                str(item["zjhm"]),
                str(item["person_type"]),
                int(item["source_row_no"]),
            )
            for item in rows
        ],
        page_size=500,
    )


def activate_batch_on_cursor(
    cur,
    *,
    batch_id: int,
    source_kind: str,
    imported_row_count: int,
    generated_tag_count: int,
    matched_person_count: int,
) -> None:
    cur.execute(
        """
        UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
        SET is_active = FALSE
        WHERE source_kind = %s
          AND is_active = TRUE
          AND import_status = 'success'
          AND id <> %s
        """,
        (source_kind, batch_id),
    )
    cur.execute(
        """
        UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
        SET import_status = 'success',
            is_active = TRUE,
            imported_row_count = %s,
            generated_tag_count = %s,
            matched_person_count = %s,
            error_message = NULL,
            activated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (imported_row_count, generated_tag_count, matched_person_count, batch_id),
    )


def save_base_batch_data_and_activate(
    *,
    batch_id: int,
    imported_row_count: int,
    deduplicated_person_count: int,
    base_rows: List[Dict[str, Any]],
) -> int:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            _apply_import_timeouts(cur, conn)
            replace_base_people_on_cursor(cur, batch_id, base_rows)
            activate_batch_on_cursor(
                cur,
                batch_id=batch_id,
                source_kind=SOURCE_KIND_BASE,
                imported_row_count=imported_row_count,
                generated_tag_count=0,
                matched_person_count=deduplicated_person_count,
            )
        conn.commit()
        return deduplicated_person_count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_tag_batch_data_and_activate(
    *,
    batch_id: int,
    imported_row_count: int,
    generated_tag_count: int,
    tagged_person_count: int,
    person_type_rows: List[Dict[str, Any]],
) -> int:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            _apply_import_timeouts(cur, conn)
            insert_person_types_on_cursor(cur, batch_id, person_type_rows)
            activate_batch_on_cursor(
                cur,
                batch_id=batch_id,
                source_kind=SOURCE_KIND_TAG,
                imported_row_count=imported_row_count,
                generated_tag_count=generated_tag_count,
                matched_person_count=tagged_person_count,
            )
        conn.commit()
        return tagged_person_count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
