from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

from sqlalchemy import Table, insert, select, update
from sqlalchemy.engine import RowMapping
from sqlalchemy.sql import text as sql_text

from zidingyi_baobiao.core.exceptions import NotFoundError, QueryExecutionError, ValidationError
from zidingyi_baobiao.models.meta_tables import get_meta_tables
from zidingyi_baobiao.services._row_accessors import first_key, first_of
from zidingyi_baobiao.services.datasource_service import get_datasource_row, get_engine_for_datasource_row
from zidingyi_baobiao.utils.sql_validator import extract_named_params, validate_sql_template, wrap_limit


DEFAULT_TIMEOUT_MS = 300_000
DEFAULT_MAX_ROWS = 100_000


def create_dataset(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    新增 dataset（SQL 模板）。
    """
    tables = get_meta_tables()
    dt = tables.dataset

    data_source_id = int(payload.get("data_source_id") or 0)
    if not data_source_id:
        raise ValidationError("data_source_id 为必填")

    sql_template = str(payload.get("sql_template") or "").strip()
    validate_sql_template(sql_template)

    timeout_ms = int(payload.get("timeout_ms") or DEFAULT_TIMEOUT_MS)
    max_rows = int(payload.get("max_rows") or DEFAULT_MAX_ROWS)
    if timeout_ms <= 0:
        raise ValidationError("timeout_ms 必须 > 0")
    if max_rows <= 0:
        raise ValidationError("max_rows 必须 > 0")

    data: Dict[str, Any] = {
        "data_source_id": data_source_id,
        _sql_col(dt): sql_template,
        "timeout_ms": timeout_ms,
        "max_rows": max_rows,
    }
    if "name" in dt.c:
        data["name"] = str(payload.get("name") or "").strip()

    data = _only_existing_columns(dt, data)

    pk = _pk_column_name(dt)
    with tables.engine.begin() as conn:
        stmt = insert(dt).values(**data)
        if pk:
            stmt = stmt.returning(dt.c[pk])
        res = conn.execute(stmt)
        new_id = res.scalar_one() if pk else None
    return {"id": new_id, "data_source_id": data_source_id, "timeout_ms": timeout_ms, "max_rows": max_rows}


def update_dataset(dataset_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    tables = get_meta_tables()
    dt = tables.dataset
    row = get_dataset_row(dataset_id)

    update_data: Dict[str, Any] = {}
    if "data_source_id" in payload:
        update_data["data_source_id"] = int(payload.get("data_source_id") or 0)
    if "sql_template" in payload:
        sql_template = str(payload.get("sql_template") or "").strip()
        validate_sql_template(sql_template)
        update_data[_sql_col(dt)] = sql_template
    for key in ("timeout_ms", "max_rows", "name"):
        if key in payload:
            update_data[key] = payload.get(key)

    update_data = _only_existing_columns(dt, update_data)
    if not update_data:
        return {"id": dataset_id}

    pk = _pk_column_name(dt)
    if not pk:
        raise ValidationError("dataset 缺少主键，无法更新")

    with tables.engine.begin() as conn:
        conn.execute(update(dt).where(dt.c[pk] == dataset_id).values(**update_data))
    return {"id": dataset_id}


def list_datasets() -> Dict[str, Any]:
    tables = get_meta_tables()
    dt = tables.dataset
    pk = _pk_column_name(dt) or "id"
    with tables.engine.connect() as conn:
        rows = conn.execute(select(dt)).mappings().all()
    items = []
    for r in rows:
        item = dict(r)
        if pk in item:
            item["id"] = item.get(pk)
        items.append(item)
    return {"items": items, "count": len(items)}


def get_dataset_row(dataset_id: int) -> RowMapping:
    tables = get_meta_tables()
    dt = tables.dataset
    pk = _pk_column_name(dt)
    if not pk:
        raise ValidationError("dataset 缺少主键，无法查询")
    with tables.engine.connect() as conn:
        row = conn.execute(select(dt).where(dt.c[pk] == dataset_id)).mappings().first()
    if not row:
        raise NotFoundError(f"dataset 不存在：{dataset_id}")
    return row


def preview_dataset(dataset_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    SQL 预览（自动 LIMIT 10）。
    """
    row = get_dataset_row(dataset_id)
    data_source_id = int(first_of(row, "data_source_id", default=0) or 0)
    if not data_source_id:
        raise ValidationError("dataset.data_source_id 为空")

    ds_row = get_datasource_row(data_source_id)
    engine = get_engine_for_datasource_row(ds_row)

    sql_template = str(row.get(_sql_col(get_meta_tables().dataset)) or "").strip()
    validate_sql_template(sql_template)

    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise ValidationError("params 必须为 JSON 对象")

    allowed_params = extract_named_params(sql_template)
    extra = set(params.keys()) - set(allowed_params)
    if extra:
        raise ValidationError(f"存在未在 SQL 中声明的参数：{sorted(extra)}")

    sql_to_run = wrap_limit(sql_template, limit_param="_limit")
    run_params = dict(params)
    run_params["_limit"] = 10
    timeout_ms = int(first_of(row, "timeout_ms", default=DEFAULT_TIMEOUT_MS) or DEFAULT_TIMEOUT_MS)

    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            rows = conn.execute(sql_text(sql_to_run), run_params).mappings().all()
        return {"columns": list(rows[0].keys()) if rows else [], "rows": [dict(r) for r in rows], "count": len(rows)}
    except Exception as exc:
        logging.exception("dataset preview failed: %s", exc)
        raise QueryExecutionError(f"SQL 预览执行失败：{exc}") from None


def _only_existing_columns(table: Table, data: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if k in table.c}


def _pk_column_name(table: Table) -> Optional[str]:
    pks = list(table.primary_key.columns)
    if pks:
        return pks[0].name
    if "id" in table.c:
        return "id"
    return None


def _sql_col(table: Table) -> str:
    key = first_key(table.c, "sql_template", "sql_text", "sql", "template_sql")
    if not key:
        raise ValidationError("dataset 表缺少 SQL 字段（sql_template/sql_text/sql）")
    return key
