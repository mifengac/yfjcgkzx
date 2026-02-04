from __future__ import annotations

"""
自定义报表：数据库访问封装（psycopg2）。

背景：
- Kingbase V8 的版本字符串可能不符合 SQLAlchemy PostgreSQL 方言的解析预期，
  会触发类似 "Could not determine version from string 'KingbaseES ...'" 的异常。
- 因此本模块默认使用项目既有的 psycopg2 连接（gonggong/config/database.py）。
"""

import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from psycopg2 import sql

from gonggong.config.database import DB_CONFIG, get_database_connection
from zidingyi_baobiao.core.exceptions import NotFoundError, ValidationError


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    is_nullable: bool
    default: Optional[str]


def get_schema() -> str:
    schema = str(DB_CONFIG.get("schema") or "public").strip()
    return schema or "public"


def get_conn():
    """
    获取 psycopg2 连接（复用主项目连接逻辑）。
    """
    return get_database_connection()


@lru_cache(maxsize=64)
def get_table_columns(schema_name: str, table_name: str) -> List[ColumnMeta]:
    """
    读取表列信息（缓存）。
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            rows = cur.fetchall()
        if not rows:
            raise ValidationError(f"表不存在或无权限访问：{schema_name}.{table_name}")
        return [ColumnMeta(name=r[0], is_nullable=(r[1] == "YES"), default=r[2]) for r in rows]
    finally:
        conn.close()


@lru_cache(maxsize=64)
def get_table_pk(schema_name: str, table_name: str) -> str:
    """
    获取主键列名（缓存）。
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
                """,
                (schema_name, table_name),
            )
            row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
        # 兜底：常见主键名
        cols = {c.name for c in get_table_columns(schema_name, table_name)}
        if "id" in cols:
            return "id"
        raise ValidationError(f"无法识别主键：{schema_name}.{table_name}")
    finally:
        conn.close()


def pick_existing_column(schema_name: str, table_name: str, candidates: Sequence[str]) -> str:
    cols = {c.name for c in get_table_columns(schema_name, table_name)}
    for c in candidates:
        if c in cols:
            return c
    raise ValidationError(f"{schema_name}.{table_name} 缺少字段（候选：{list(candidates)}）")


def insert_row(schema_name: str, table_name: str, data: Mapping[str, Any]) -> Optional[int]:
    """
    插入一行并返回主键（若主键可返回）。
    """
    cols_meta = get_table_columns(schema_name, table_name)
    cols = {c.name for c in cols_meta}
    filtered = {k: v for k, v in data.items() if k in cols}

    if not filtered:
        raise ValidationError("无可写入字段")

    pk = get_table_pk(schema_name, table_name)
    stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join([sql.Identifier(k) for k in filtered.keys()]),
        sql.SQL(", ").join([sql.Placeholder(k) for k in filtered.keys()]),
        sql.Identifier(pk),
    )

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(stmt, filtered)
                new_id = cur.fetchone()[0]
        return int(new_id) if new_id is not None else None
    except Exception as exc:
        raise ValidationError(f"{table_name} 写入失败：{exc}") from None
    finally:
        conn.close()


def update_row_safe(schema_name: str, table_name: str, row_id: int, data: Mapping[str, Any]) -> None:
    """
    update_row 的安全实现（兼容 dict 命名参数）。
    """
    cols_meta = get_table_columns(schema_name, table_name)
    cols = {c.name for c in cols_meta}
    filtered = {k: v for k, v in data.items() if k in cols}
    if not filtered:
        return

    pk = get_table_pk(schema_name, table_name)
    stmt = sql.SQL("UPDATE {}.{} SET {} WHERE {} = {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(", ").join([sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder(k)) for k in filtered.keys()]),
        sql.Identifier(pk),
        sql.Placeholder("_id"),
    )

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(stmt, {**filtered, "_id": row_id})
    except Exception as exc:
        raise ValidationError(f"{table_name} 更新失败：{exc}") from None
    finally:
        conn.close()


def fetch_one(schema_name: str, table_name: str, row_id: int) -> Mapping[str, Any]:
    pk = get_table_pk(schema_name, table_name)
    stmt = sql.SQL("SELECT * FROM {}.{} WHERE {} = %s").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(pk),
    )
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(stmt, (row_id,))
            row = cur.fetchone()
            if not row:
                raise NotFoundError(f"{table_name} 不存在：{row_id}")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    except NotFoundError:
        raise
    except Exception as exc:
        raise ValidationError(f"{table_name} 查询失败：{exc}") from None
    finally:
        conn.close()


def fetch_by_column(schema_name: str, table_name: str, column: str, value: Any) -> Mapping[str, Any]:
    stmt = sql.SQL("SELECT * FROM {}.{} WHERE {} = %s").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(column),
    )
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(stmt, (value,))
            row = cur.fetchone()
            if not row:
                raise NotFoundError(f"{table_name} 不存在：{value}")
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
    except NotFoundError:
        raise
    except Exception as exc:
        raise ValidationError(f"{table_name} 查询失败：{exc}") from None
    finally:
        conn.close()


def fetch_all(schema_name: str, table_name: str, *, limit: int = 500) -> List[Mapping[str, Any]]:
    pk = get_table_pk(schema_name, table_name)
    stmt = sql.SQL("SELECT * FROM {}.{} ORDER BY {} DESC LIMIT %s").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(pk),
    )
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(stmt, (int(limit),))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]
    except Exception as exc:
        raise ValidationError(f"{table_name} 查询失败：{exc}") from None
    finally:
        conn.close()


def json_dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)
