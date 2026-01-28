from __future__ import annotations

import json
import re
from typing import Any, Dict, Mapping, Sequence, Tuple

from psycopg2 import sql
from psycopg2.extras import Json, execute_values


_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def ensure_schema(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.commit()


def infer_col_types(rows: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    """
    轻量推断列类型（避免把所有值收集到 bucket 里导致内存/耗时过大）。

    规则：
    - 若某列出现过任何一个“非时间格式”的非空值 => TEXT
    - 否则（至少出现过一个时间格式值）=> TIMESTAMP
    - 若全空/占位符 => TEXT
    """
    state: Dict[str, str] = {}  # UNKNOWN / TIMESTAMP / TEXT
    for r in rows or []:
        for k, v in (r or {}).items():
            if not k:
                continue
            cur = state.get(k, "UNKNOWN")
            if cur == "TEXT":
                continue
            if v in (None, "", "-", "无数据"):
                continue
            if isinstance(v, str) and _TS_RE.match(v.strip()):
                if cur == "UNKNOWN":
                    state[k] = "TIMESTAMP"
                continue
            state[k] = "TEXT"
    inferred: Dict[str, str] = {}
    for k, st in state.items():
        inferred[k] = "TIMESTAMP" if st == "TIMESTAMP" else "TEXT"
    return inferred


def ensure_table_and_columns(
    *,
    conn,
    schema: str,
    table: str,
    pk_fields: Sequence[str],
    inferred_types: Dict[str, str],
    table_comment: str = "",
) -> Dict[str, str]:
    pk_fields = [f for f in pk_fields if f]
    if not pk_fields:
        raise ValueError("pk_fields 不能为空")

    with conn.cursor() as cur:
        # 建表（先只建 PK 列）
        pk_cols = [sql.SQL("{} TEXT").format(sql.Identifier(f)) for f in pk_fields]
        pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
            sql.SQL(", ").join(sql.Identifier(f) for f in pk_fields)
        )
        create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}, {})").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(pk_cols),
            pk_constraint,
        )
        cur.execute(create_stmt)

        if table_comment:
            cur.execute(
                sql.SQL("COMMENT ON TABLE {}.{} IS {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Literal(table_comment),
                )
            )

        # 补列：默认 TEXT；疑似时间列用 TIMESTAMP
        for col, col_type in sorted((inferred_types or {}).items()):
            if not col or col in pk_fields:
                continue
            safe_type = "TIMESTAMP" if col_type == "TIMESTAMP" else "TEXT"
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(col),
                    sql.SQL(safe_type),
                )
            )

    conn.commit()

    col_types: Dict[str, str] = {k: ("TIMESTAMP" if v == "TIMESTAMP" else "TEXT") for k, v in (inferred_types or {}).items()}
    for f in pk_fields:
        col_types[f] = "TEXT"
    return col_types


def ensure_table_jsonb(
    *,
    conn,
    schema: str,
    table: str,
    pk_fields: Sequence[str],
    table_comment: str = "",
) -> None:
    """
    快速模式：只建主键 + data(jsonb) + fetched_at。
    """
    pk_fields = [f for f in pk_fields if f]
    if not pk_fields:
        raise ValueError("pk_fields 不能为空")

    with conn.cursor() as cur:
        # 若表不存在：创建主键列 + data + fetched_at
        pk_cols = [sql.SQL("{} TEXT").format(sql.Identifier(f)) for f in pk_fields]
        pk_constraint = sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(sql.Identifier(f) for f in pk_fields))
        create_stmt = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {}.{} ({}, data JSONB, fetched_at TIMESTAMP DEFAULT NOW(), {})"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(pk_cols),
            pk_constraint,
        )
        cur.execute(create_stmt)

        # 若表已存在（比如之前是“宽表”模式）：补齐 data/fetched_at 字段即可，无需删表
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS data JSONB").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        # 老表补列后 fetched_at 可能为 NULL；不强制回填，写入/更新时会用 NOW()

        if table_comment:
            cur.execute(
                sql.SQL("COMMENT ON TABLE {}.{} IS {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Literal(table_comment),
                )
            )
    conn.commit()


def upsert_rows_jsonb(
    *,
    conn,
    schema: str,
    table: str,
    pk_fields: Sequence[str],
    rows: Sequence[Dict[str, Any]],
    batch_size: int = 2000,
) -> int:
    """
    快速模式 upsert：将整条记录写入 data(jsonb)，并刷新 fetched_at。
    """
    if not rows:
        return 0
    pk_fields = [f for f in pk_fields if f]
    if not pk_fields:
        raise ValueError("pk_fields 不能为空")

    # 同一批次内若存在相同主键，PostgreSQL 会报：
    # ON CONFLICT DO UPDATE command cannot affect row a second time
    # 这里先按主键去重，保留最后一条（后写覆盖先写）。
    dedup: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for r in rows:
        key = tuple("" if (r or {}).get(pk) is None else str((r or {}).get(pk)) for pk in pk_fields)
        dedup[key] = (r or {})
    rows = list(dedup.values())

    columns = list(pk_fields) + ["data"]
    insert_cols = [sql.Identifier(c) for c in columns]
    conflict_cols = sql.SQL(", ").join(sql.Identifier(c) for c in pk_fields)
    stmt = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES %s "
        "ON CONFLICT ({}) DO UPDATE SET data = EXCLUDED.data, fetched_at = NOW()"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(insert_cols),
        conflict_cols,
    )

    data_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        tup: List[Any] = []
        for pk in pk_fields:
            v = (r or {}).get(pk)
            tup.append("" if v is None else str(v))
        tup.append(Json(r or {}, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, separators=(",", ":"))))
        data_rows.append(tuple(tup))

    with conn.cursor() as cur:
        execute_values(
            cur,
            stmt.as_string(cur),
            data_rows,
            page_size=max(1, int(batch_size)),
        )
    conn.commit()
    return len(data_rows)


def upsert_rows(
    *,
    conn,
    schema: str,
    table: str,
    pk_fields: Sequence[str],
    rows: Sequence[Dict[str, Any]],
    col_types: Mapping[str, str] | None = None,
    batch_size: int = 500,
) -> int:
    if not rows:
        return 0

    pk_fields = [f for f in pk_fields if f]
    if pk_fields:
        dedup: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        for r in rows:
            key = tuple("" if (r or {}).get(pk) is None else str((r or {}).get(pk)) for pk in pk_fields)
            dedup[key] = (r or {})
        rows = list(dedup.values())
    columns = sorted({k for r in rows for k in (r or {}).keys() if k})
    for pk in reversed(pk_fields):
        if pk not in columns:
            columns.insert(0, pk)

    insert_cols = [sql.Identifier(c) for c in columns]
    conflict_cols = sql.SQL(", ").join(sql.Identifier(c) for c in pk_fields)
    update_cols = [c for c in columns if c not in pk_fields]
    if update_cols:
        update_sql = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
        )
        conflict_action = sql.SQL("DO UPDATE SET {}").format(update_sql)
    else:
        conflict_action = sql.SQL("DO NOTHING")

    stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) ").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(insert_cols),
        conflict_cols,
    ) + conflict_action

    def _adapt_value(v: Any, ctype: str) -> Any:
        if v is None:
            return None
        if ctype == "TIMESTAMP":
            if v in ("", "-", "无数据"):
                return None
            if isinstance(v, str):
                s = v.strip()
                if not s or s == "-" or s == "无数据":
                    return None
                if _TS_RE.match(s):
                    return s
                return None
            return None
        if isinstance(v, (str, int, float, bool)):
            return v
        try:
            return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return str(v)

    data_rows: List[Tuple[Any, ...]] = []
    for r in rows:
        tup: List[Any] = []
        for c in columns:
            ctype = (col_types or {}).get(c, "TEXT")
            tup.append(_adapt_value((r or {}).get(c), ctype))
        data_rows.append(tuple(tup))

    with conn.cursor() as cur:
        # execute_values 会按 page_size 分批拼 VALUES，大幅减少数据库往返次数
        execute_values(
            cur,
            stmt.as_string(cur),
            data_rows,
            page_size=max(1, int(batch_size)),
        )
    conn.commit()
    return len(data_rows)
