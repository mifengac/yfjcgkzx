from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Sequence

from psycopg2.extras import Json, RealDictCursor

from gonggong.config.database import get_database_connection


SCHEMA_NAME = "jcgkzx_monitor"
SCHEME_TABLE = "custom_case_monitor_scheme"
RULE_TABLE = "custom_case_monitor_rule"


def _quote_table(schema_name: str, table_name: str) -> str:
    return f'"{schema_name}"."{table_name}"'


def _scheme_table() -> str:
    return _quote_table(SCHEMA_NAME, SCHEME_TABLE)


def _rule_table() -> str:
    return _quote_table(SCHEMA_NAME, RULE_TABLE)


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    serialized: Dict[str, Any] = {}
    for key, value in dict(row).items():
        if isinstance(value, datetime):
            serialized[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            serialized[key] = value
    return serialized


def _fetch_rules_by_scheme_ids(cursor: RealDictCursor, scheme_ids: Sequence[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not scheme_ids:
        return {}

    cursor.execute(
        f"""
        SELECT id,
               scheme_id,
               field_name,
               operator,
               rule_values,
               sort_order,
               is_enabled,
               created_at,
               updated_at
          FROM {_rule_table()}
         WHERE scheme_id = ANY(%s)
         ORDER BY scheme_id ASC, sort_order ASC, id ASC
        """,
        (list(scheme_ids),),
    )
    rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
    grouped: Dict[int, List[Dict[str, Any]]] = {scheme_id: [] for scheme_id in scheme_ids}
    for row in rows:
        grouped.setdefault(int(row["scheme_id"]), []).append(row)
    return grouped


def _attach_rules(cursor: RealDictCursor, scheme_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scheme_ids = [int(row["id"]) for row in scheme_rows]
    rules_map = _fetch_rules_by_scheme_ids(cursor, scheme_ids)
    hydrated: List[Dict[str, Any]] = []
    for row in scheme_rows:
        item = _serialize_row(dict(row))
        item["rules"] = rules_map.get(int(row["id"]), [])
        hydrated.append(item)
    return hydrated


def list_schemes(include_disabled: bool = True) -> List[Dict[str, Any]]:
    connection = get_database_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            sql = f"""
                SELECT id,
                       scheme_name,
                       scheme_code,
                       description,
                       is_enabled,
                       created_at,
                       updated_at
                  FROM {_scheme_table()}
            """
            if not include_disabled:
                sql += " WHERE is_enabled IS TRUE"
            sql += " ORDER BY updated_at DESC, id DESC"
            cursor.execute(sql)
            rows = [_serialize_row(dict(row)) for row in cursor.fetchall()]
            return _attach_rules(cursor, rows)
    finally:
        connection.close()


def get_scheme_by_id(scheme_id: int) -> Dict[str, Any] | None:
    connection = get_database_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                SELECT id,
                       scheme_name,
                       scheme_code,
                       description,
                       is_enabled,
                       created_at,
                       updated_at
                  FROM {_scheme_table()}
                 WHERE id = %s
                """,
                (scheme_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            hydrated = _attach_rules(cursor, [_serialize_row(dict(row))])
            return hydrated[0]
    finally:
        connection.close()


def create_scheme(*, scheme_name: str, scheme_code: str, description: str, is_enabled: bool, rules: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    connection = get_database_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                INSERT INTO {_scheme_table()} (
                    scheme_name,
                    scheme_code,
                    description,
                    is_enabled,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
                """,
                (scheme_name, scheme_code, description, is_enabled),
            )
            created = cursor.fetchone()
            scheme_id = int(created["id"])
            _replace_rules(cursor, scheme_id, rules)
            connection.commit()
        scheme = get_scheme_by_id(scheme_id)
        if not scheme:
            raise RuntimeError("创建方案后未能重新读取方案数据")
        return scheme
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def update_scheme(
    scheme_id: int,
    *,
    scheme_name: str,
    description: str,
    is_enabled: bool,
    rules: Sequence[Dict[str, Any]],
) -> Dict[str, Any] | None:
    connection = get_database_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"""
                UPDATE {_scheme_table()}
                   SET scheme_name = %s,
                       description = %s,
                       is_enabled = %s,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = %s
                RETURNING id
                """,
                (scheme_name, description, is_enabled, scheme_id),
            )
            updated = cursor.fetchone()
            if not updated:
                connection.rollback()
                return None
            _replace_rules(cursor, scheme_id, rules)
            connection.commit()
        return get_scheme_by_id(scheme_id)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def delete_scheme(scheme_id: int) -> bool:
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'DELETE FROM {_scheme_table()} WHERE id = %s', (scheme_id,))
            deleted = cursor.rowcount > 0
            connection.commit()
            return deleted
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _replace_rules(cursor: RealDictCursor, scheme_id: int, rules: Sequence[Dict[str, Any]]) -> None:
    cursor.execute(f"DELETE FROM {_rule_table()} WHERE scheme_id = %s", (scheme_id,))
    for index, rule in enumerate(rules, start=1):
        cursor.execute(
            f"""
            INSERT INTO {_rule_table()} (
                scheme_id,
                field_name,
                operator,
                rule_values,
                sort_order,
                is_enabled,
                created_at,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                scheme_id,
                rule["field_name"],
                rule["operator"],
                Json(list(rule["rule_values"])),
                int(rule.get("sort_order") or index),
                bool(rule.get("is_enabled", True)),
            ),
        )
