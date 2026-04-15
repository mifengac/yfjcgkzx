from __future__ import annotations

from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from gonggong.config.database import get_database_connection


UNFINISHED_FIGHT_CASE_STATUS_CODES = ["0101", "0202", "0201", "0207"]
FIGHT_CASE_TYPE_NAME = "打架斗殴"

UNFINISHED_FIGHT_CASE_QUERY = """
WITH ay AS (
    SELECT ctc."ay_pattern"
    FROM "ywdata"."case_type_config" ctc
    WHERE ctc."leixing" = %s
)
SELECT
    zza."ajxx_aymc" AS ay_name,
    zza."ajxx_ajmc" AS case_name,
    zza."ajxx_ajzt" AS case_status,
    zza."ajxx_cbdw_mc" AS handling_unit,
    zza."ajxx_lasj" AS filing_time,
    zza."ajxx_fadd" AS incident_address,
    zza."ajxx_fasj" AS incident_time,
    zza."ajxx_jyaq" AS summary
FROM "ywdata"."zq_zfba_ajxx" zza
WHERE zza."ajxx_lasj" BETWEEN %s AND %s
  AND zza."ajxx_ajzt_dm" = ANY(%s)
  AND EXISTS (
      SELECT 1
      FROM ay
      WHERE zza."ajxx_aymc" SIMILAR TO ay."ay_pattern"
  )
ORDER BY zza."ajxx_lasj" DESC NULLS LAST, zza."ajxx_ajmc" ASC NULLS LAST
"""


def list_unclosed_fight_cases(begin_date: str, end_date: str) -> List[Dict[str, Any]]:
    connection = get_database_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                UNFINISHED_FIGHT_CASE_QUERY,
                (
                    FIGHT_CASE_TYPE_NAME,
                    begin_date,
                    end_date,
                    UNFINISHED_FIGHT_CASE_STATUS_CODES,
                ),
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()