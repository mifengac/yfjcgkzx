from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values


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
            # 即使值全为空/占位符，也要把列名纳入推断集合；
            # 否则 ensure_table_and_columns 不会补列，但 upsert 时仍会在 INSERT 列表中出现，导致“column does not exist”。
            cur = state.setdefault(k, "UNKNOWN")
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


def ensure_qsryxx_composite_pk(*, conn, schema: str, table: str = "zq_zfba_qsryxx") -> Tuple[int, int]:
    """
    将 zq_zfba_qsryxx 主键统一为 (ajxx_ajbh, qsryxx_rybh)。
    返回：(清理空主键条数, 去重条数)。
    去重策略：同一 (ajxx_ajbh, qsryxx_rybh) 仅保留 qsryxx_tfsj 最新记录。
    """
    dropped_empty_pk = 0
    dropped_dup = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
            """,
            (schema, table),
        )
        if cur.fetchone() is None:
            conn.commit()
            return dropped_empty_pk, dropped_dup

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, table),
        )
        cols = {str(r[0]) for r in cur.fetchall() if r and r[0]}
        required = {"ajxx_ajbh", "qsryxx_rybh"}
        missing = required - cols
        if missing:
            raise RuntimeError(f"{schema}.{table} 缺少主键列: {','.join(sorted(missing))}")

        cur.execute(
            sql.SQL(
                """
                DELETE FROM {}.{}
                WHERE NULLIF(BTRIM(COALESCE(ajxx_ajbh::text, '')), '') IS NULL
                   OR NULLIF(BTRIM(COALESCE(qsryxx_rybh::text, '')), '') IS NULL
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )
        dropped_empty_pk = int(cur.rowcount or 0)

        order_parts = []
        if "qsryxx_tfsj" in cols:
            order_parts.append(sql.SQL("qsryxx_tfsj DESC NULLS LAST"))
        if "qsryxx_id" in cols:
            order_parts.append(sql.SQL("qsryxx_id DESC NULLS LAST"))
        order_parts.append(sql.SQL("ctid DESC"))

        cur.execute(
            sql.SQL(
                """
                WITH ranked AS (
                    SELECT
                        ctid AS rid,
                        ROW_NUMBER() OVER (
                            PARTITION BY ajxx_ajbh, qsryxx_rybh
                            ORDER BY {}
                        ) AS rn
                    FROM {}.{}
                )
                DELETE FROM {}.{} t
                USING ranked r
                WHERE t.ctid = r.rid
                  AND r.rn > 1
                """
            ).format(
                sql.SQL(", ").join(order_parts),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        dropped_dup = int(cur.rowcount or 0)

        cur.execute(
            """
            SELECT con.conname
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE con.contype='p'
              AND nsp.nspname=%s
              AND rel.relname=%s
            """,
            (schema, table),
        )
        pk_constraints = [str(r[0]) for r in cur.fetchall() if r and r[0]]
        for c in pk_constraints:
            cur.execute(
                sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT {}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.Identifier(c),
                )
            )

        constraint_name = f"pk_{table}_ajxx_ajbh_qsryxx_rybh"
        if len(constraint_name) > 63:
            constraint_name = "pk_zq_zfba_qsryxx_ajbh_rybh"
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({}, {})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(constraint_name),
                sql.Identifier("ajxx_ajbh"),
                sql.Identifier("qsryxx_rybh"),
            )
        )
    conn.commit()
    return dropped_empty_pk, dropped_dup


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
    pk_set = set(pk_fields)
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

    def _adapt_value(v: Any, ctype: str, *, is_pk: bool) -> Any:
        # 综查接口常用占位符："-" / "无数据" / ""。
        # 对非主键字段一律按“空值”处理，避免写入到 TIMESTAMP 等强类型列时报错。
        if is_pk:
            if v is None:
                return ""
            return str(v)
        if v is None:
            return None
        if v in ("", "-", "无数据"):
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
            tup.append(_adapt_value((r or {}).get(c), ctype, is_pk=(c in pk_set)))
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
