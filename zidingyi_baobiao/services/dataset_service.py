from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from zidingyi_baobiao.core.db import (
    fetch_all,
    fetch_one,
    get_schema,
    insert_row,
    pick_existing_column,
    update_row_safe,
)
from zidingyi_baobiao.core.exceptions import QueryExecutionError, ValidationError
from zidingyi_baobiao.utils.sql_validator import extract_named_params, to_psycopg2_named_paramstyle, validate_sql_template, wrap_limit


DATASET_TABLE = "dataset"
DEFAULT_TIMEOUT_MS = 300_000
DEFAULT_MAX_ROWS = 100_000


def create_dataset(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    新增 dataset（SQL 模板）。

    注意：默认使用主项目既有数据库连接执行查询，因此不强制依赖 data_source 管理。
    若 dataset 表存在 data_source_id 且为 NOT NULL 且无默认值，则仍需要由调用方提供。
    """
    schema = get_schema()
    sql_col = _sql_col(schema)

    sql_template = str(payload.get("sql_template") or "").strip()
    validate_sql_template(sql_template)

    data: Dict[str, Any] = {
        sql_col: sql_template,
        "timeout_ms": int(payload.get("timeout_ms") or DEFAULT_TIMEOUT_MS),
        "max_rows": int(payload.get("max_rows") or DEFAULT_MAX_ROWS),
    }
    if "name" in payload:
        data["name"] = str(payload.get("name") or "").strip()
    if "data_source_id" in payload:
        data["data_source_id"] = int(payload.get("data_source_id") or 0) or None

    new_id = insert_row(schema, DATASET_TABLE, data)
    return {"id": new_id}


def update_dataset(dataset_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    schema = get_schema()
    sql_col = _sql_col(schema)

    update_data: Dict[str, Any] = {}
    if "sql_template" in payload:
        sql_template = str(payload.get("sql_template") or "").strip()
        validate_sql_template(sql_template)
        update_data[sql_col] = sql_template
    for key in ("timeout_ms", "max_rows", "name", "data_source_id"):
        if key in payload:
            update_data[key] = payload.get(key)

    update_row_safe(schema, DATASET_TABLE, dataset_id, update_data)
    return {"id": dataset_id}


def list_datasets() -> Dict[str, Any]:
    schema = get_schema()
    items = fetch_all(schema, DATASET_TABLE, limit=500)
    return {"items": items, "count": len(items)}


def get_dataset(dataset_id: int) -> Dict[str, Any]:
    schema = get_schema()
    return dict(fetch_one(schema, DATASET_TABLE, dataset_id))


def preview_dataset(dataset_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    SQL 预览（自动 LIMIT 10）。
    """
    schema = get_schema()
    row = fetch_one(schema, DATASET_TABLE, dataset_id)
    sql_col = _sql_col(schema)
    sql_template = str(row.get(sql_col) or "").strip()
    validate_sql_template(sql_template)

    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise ValidationError("params 必须为 JSON 对象")

    allowed = extract_named_params(sql_template)
    extra = set(params.keys()) - set(allowed)
    if extra:
        raise ValidationError(f"存在未在 SQL 中声明的参数：{sorted(extra)}")

    timeout_ms = int(row.get("timeout_ms") or DEFAULT_TIMEOUT_MS)
    sql_to_run = wrap_limit(sql_template, limit_param="_limit")
    sql_to_run = to_psycopg2_named_paramstyle(sql_to_run)
    run_params = dict(params)
    run_params["_limit"] = 10

    from zidingyi_baobiao.core.db import get_conn

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                cur.execute(sql_to_run, run_params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
        result_rows = [dict(zip(cols, r)) for r in rows]
        return {"columns": cols, "rows": result_rows, "count": len(result_rows)}
    except Exception as exc:
        logging.exception("dataset preview failed: %s", exc)
        raise QueryExecutionError(f"SQL 预览执行失败：{exc}") from None
    finally:
        conn.close()


def _sql_col(schema: str) -> str:
    return pick_existing_column(schema, DATASET_TABLE, ["sql_template", "sql_text", "sql", "template_sql"])
