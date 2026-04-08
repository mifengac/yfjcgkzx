from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from gonggong.config.database import execute_query, get_database_connection


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
    SELECT DISTINCT
        ssfjdm AS value,
        ssfj AS label
    FROM "stdata"."b_dic_zzjgdm"
    WHERE ssfjdm IS NOT NULL
      AND ssfj IS NOT NULL
    ORDER BY ssfj
    """
    return execute_query(sql)


def get_active_batch() -> Optional[Dict[str, Any]]:
    sql = """
    SELECT
        id,
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
    WHERE is_active = TRUE
      AND import_status = 'success'
    ORDER BY activated_at DESC NULLS LAST, created_at DESC, id DESC
    LIMIT 1
    """
    rows = execute_query(sql)
    return rows[0] if rows else None


def create_pending_batch(
    *,
    source_file_name: str,
    sheet_name: str,
    created_by: str,
) -> int:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO "jcgkzx_monitor"."jszahz_topic_batch" (
                    source_file_name,
                    sheet_name,
                    import_status,
                    is_active,
                    imported_row_count,
                    matched_person_count,
                    generated_tag_count,
                    created_by,
                    error_message,
                    created_at
                )
                VALUES (%s, %s, 'pending', FALSE, 0, 0, 0, %s, '', CURRENT_TIMESTAMP)
                RETURNING id
                """,
                (source_file_name, sheet_name, created_by),
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
                (imported_row_count, generated_tag_count, str(error_message or "")[:1000], batch_id),
            )
        conn.commit()
    finally:
        conn.close()


def save_batch_data_and_activate(
    *,
    batch_id: int,
    imported_row_count: int,
    person_type_rows: Iterable[Dict[str, Any]],
) -> int:
    rows = list(person_type_rows)
    generated_tag_count = len(rows)
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            if rows:
                cur.executemany(
                    """
                    INSERT INTO "jcgkzx_monitor"."jszahz_topic_person_type" (
                        batch_id,
                        zjhm,
                        person_type,
                        source_row_no
                    )
                    VALUES (%s, %s, %s, %s)
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
                )

            cur.execute(
                'DELETE FROM "jcgkzx_monitor"."jszahz_topic_snapshot" WHERE batch_id = %s',
                (batch_id,),
            )
            cur.execute(
                """
                WITH person_types AS (
                    SELECT
                        batch_id,
                        zjhm,
                        string_agg(
                            person_type,
                            ',' ORDER BY
                                CASE person_type
                                    WHEN '不规律服药' THEN 1
                                    WHEN '弱监护' THEN 2
                                    WHEN '无监护' THEN 3
                                    WHEN '既往有严重自杀或伤人行为' THEN 4
                                    ELSE 99
                                END,
                                person_type
                        ) AS person_types_text
                    FROM "jcgkzx_monitor"."jszahz_topic_person_type"
                    WHERE batch_id = %s
                    GROUP BY batch_id, zjhm
                ),
                risk_source AS (
                    SELECT DISTINCT ON (p.zjhm)
                        p.zjhm,
                        COALESCE(p.xm, '') AS xm,
                        p.lgsj,
                        COALESCE(p.lgdw, '') AS lgdw,
                        COALESCE(p.fxdj, '') AS fxdj,
                        CASE
                            WHEN p.fxdj = '00' THEN '0级患者'
                            WHEN p.fxdj = '01' THEN '1级患者'
                            WHEN p.fxdj = '02' THEN '2级患者'
                            WHEN p.fxdj = '03' THEN '3级患者'
                            WHEN p.fxdj = '04' THEN '4级患者'
                            WHEN p.fxdj = '05' THEN '5级患者'
                            ELSE '无数据'
                        END AS fxdj_label,
                        COALESCE(d1.ssfjdm, d2.ssfjdm, '') AS ssfjdm,
                        COALESCE(d1.ssfj, d2.ssfj, '') AS ssfj
                    FROM "stdata"."b_per_jszahzryxxwh" p
                    LEFT JOIN "stdata"."b_dic_zzjgdm" d1
                        ON p.lgdw = d1.sspcsdm
                    LEFT JOIN "stdata"."b_dic_zzjgdm" d2
                        ON (substring(COALESCE(p.lgdw, ''), 1, 6) || '000000') = d2.ssfjdm
                    WHERE p.sflg = '1'
                      AND p.deleteflag = '0'
                    ORDER BY p.zjhm, p.lgsj DESC NULLS LAST, p.xgsj DESC NULLS LAST, p.djsj DESC NULLS LAST, p.systemid DESC
                )
                INSERT INTO "jcgkzx_monitor"."jszahz_topic_snapshot" (
                    batch_id,
                    zjhm,
                    xm,
                    lgsj,
                    lgdw,
                    fxdj,
                    fxdj_label,
                    person_types_text,
                    ssfjdm,
                    ssfj,
                    created_at
                )
                SELECT
                    %s,
                    r.zjhm,
                    r.xm,
                    r.lgsj,
                    r.lgdw,
                    r.fxdj,
                    r.fxdj_label,
                    p.person_types_text,
                    r.ssfjdm,
                    r.ssfj,
                    CURRENT_TIMESTAMP
                FROM person_types p
                JOIN risk_source r
                  ON r.zjhm = p.zjhm
                """,
                (batch_id, batch_id),
            )
            matched_person_count = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

            cur.execute(
                """
                UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
                SET is_active = FALSE
                WHERE is_active = TRUE
                  AND import_status = 'success'
                  AND id <> %s
                """,
                (batch_id,),
            )
            cur.execute(
                """
                UPDATE "jcgkzx_monitor"."jszahz_topic_batch"
                SET import_status = 'success',
                    is_active = TRUE,
                    imported_row_count = %s,
                    generated_tag_count = %s,
                    matched_person_count = %s,
                    error_message = '',
                    activated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (imported_row_count, generated_tag_count, matched_person_count, batch_id),
            )
        conn.commit()
        return matched_person_count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_summary_rows(
    *,
    batch_id: int,
    start_time: str,
    end_time: str,
    branch_codes: Iterable[str] | None = None,
    person_types: Iterable[str] | None = None,
    risk_labels: Iterable[str] | None = None,
) -> List[Dict[str, Any]]:
    branch_list = [x.strip() for x in (branch_codes or []) if x and x.strip()]
    person_type_list = [x.strip() for x in (person_types or []) if x and x.strip()]
    risk_list = [x.strip() for x in (risk_labels or []) if x and x.strip()]

    sql = """
    SELECT
        COALESCE(NULLIF(s.ssfjdm, ''), '__UNMATCHED__') AS "分局代码",
        COALESCE(NULLIF(s.ssfj, ''), '未匹配分局') AS "分局名称",
        COUNT(DISTINCT s.zjhm) AS "去重患者数"
    FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
    WHERE s.batch_id = %s
      AND s.lgsj >= %s
      AND s.lgsj <= %s
    """
    params: List[Any] = [batch_id, start_time, end_time]

    if branch_list:
        sql += ' AND COALESCE(NULLIF(s.ssfjdm, \'\'), \'__UNMATCHED__\') = ANY(%s)'
        params.append(branch_list)
    if risk_list:
        sql += ' AND s.fxdj_label = ANY(%s)'
        params.append(risk_list)
    if person_type_list:
        sql += """
          AND EXISTS (
              SELECT 1
              FROM "jcgkzx_monitor"."jszahz_topic_person_type" pt
              WHERE pt.batch_id = s.batch_id
                AND pt.zjhm = s.zjhm
                AND pt.person_type = ANY(%s)
          )
        """
        params.append(person_type_list)

    sql += """
    GROUP BY COALESCE(NULLIF(s.ssfjdm, ''), '__UNMATCHED__'), COALESCE(NULLIF(s.ssfj, ''), '未匹配分局')
    ORDER BY COUNT(DISTINCT s.zjhm) DESC, COALESCE(NULLIF(s.ssfj, ''), '未匹配分局')
    """
    return execute_query(sql, tuple(params))


def query_detail_rows(
    *,
    batch_id: int,
    start_time: str,
    end_time: str,
    branch_code: Optional[str],
    person_types: Iterable[str] | None = None,
    risk_labels: Iterable[str] | None = None,
) -> List[Dict[str, Any]]:
    person_type_list = [x.strip() for x in (person_types or []) if x and x.strip()]
    risk_list = [x.strip() for x in (risk_labels or []) if x and x.strip()]

    sql = """
    SELECT
        COALESCE(s.xm, '') AS "姓名",
        COALESCE(s.zjhm, '') AS "身份证号",
        s.lgsj AS "列管时间",
        COALESCE(s.lgdw, '') AS "列管单位",
        COALESCE(NULLIF(s.ssfj, ''), '未匹配分局') AS "分局",
        COALESCE(s.fxdj_label, '无数据') AS "人员风险",
        COALESCE(s.person_types_text, '') AS "人员类型"
    FROM "jcgkzx_monitor"."jszahz_topic_snapshot" s
    WHERE s.batch_id = %s
      AND s.lgsj >= %s
      AND s.lgsj <= %s
    """
    params: List[Any] = [batch_id, start_time, end_time]

    if branch_code and branch_code != "__ALL__":
        sql += ' AND COALESCE(NULLIF(s.ssfjdm, \'\'), \'__UNMATCHED__\') = %s'
        params.append(branch_code)
    if risk_list:
        sql += ' AND s.fxdj_label = ANY(%s)'
        params.append(risk_list)
    if person_type_list:
        sql += """
          AND EXISTS (
              SELECT 1
              FROM "jcgkzx_monitor"."jszahz_topic_person_type" pt
              WHERE pt.batch_id = s.batch_id
                AND pt.zjhm = s.zjhm
                AND pt.person_type = ANY(%s)
          )
        """
        params.append(person_type_list)

    sql += """
    ORDER BY COALESCE(NULLIF(s.ssfj, ''), '未匹配分局'), s.lgsj DESC NULLS LAST, s.xm, s.zjhm
    """
    return execute_query(sql, tuple(params))
