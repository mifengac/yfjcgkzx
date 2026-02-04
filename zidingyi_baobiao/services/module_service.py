from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from zidingyi_baobiao.core.db import (
    fetch_all,
    fetch_by_column,
    fetch_one,
    get_schema,
    insert_row,
    json_dumps,
    pick_existing_column,
    update_row_safe,
)
from zidingyi_baobiao.core.exceptions import ValidationError
from zidingyi_baobiao.schemas.module_config import ModuleConfig, parse_module_config


MODULE_TABLE = "module_def"
DATASET_TABLE = "dataset"


def create_module(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    新增 module_def（Tab 配置）。
    """
    schema = get_schema()
    tab_col = _tab_key_col(schema)
    cfg_col = _config_col(schema)

    tab_key = str(payload.get("tab_key") or "").strip()
    if not tab_key:
        raise ValidationError("tab_key 为必填")

    cfg = parse_module_config(payload.get("config_json"))
    _ensure_datasets_exist(schema, [d.dataset_id for d in cfg.datasets])

    data: Dict[str, Any] = {
        tab_col: tab_key,
        cfg_col: json_dumps(_config_to_dict(cfg)),
    }
    if "name" in payload:
        data["name"] = str(payload.get("name") or tab_key).strip()

    new_id = insert_row(schema, MODULE_TABLE, data)
    return {"id": new_id, "tab_key": tab_key}


def update_module(module_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    更新 module_def。
    """
    schema = get_schema()
    tab_col = _tab_key_col(schema)
    cfg_col = _config_col(schema)

    _ = fetch_one(schema, MODULE_TABLE, module_id)

    update_data: Dict[str, Any] = {}
    if "tab_key" in payload:
        update_data[tab_col] = str(payload.get("tab_key") or "").strip()
    if "name" in payload:
        update_data["name"] = str(payload.get("name") or "").strip()
    if "config_json" in payload:
        cfg = parse_module_config(payload.get("config_json"))
        _ensure_datasets_exist(schema, [d.dataset_id for d in cfg.datasets])
        update_data[cfg_col] = json_dumps(_config_to_dict(cfg))

    update_row_safe(schema, MODULE_TABLE, module_id, update_data)
    return {"id": module_id}


def list_modules() -> Dict[str, Any]:
    schema = get_schema()
    items = fetch_all(schema, MODULE_TABLE, limit=500)
    return {"items": items, "count": len(items)}


def get_module_by_tab_key(tab_key: str) -> Mapping[str, Any]:
    schema = get_schema()
    tab_col = _tab_key_col(schema)
    return fetch_by_column(schema, MODULE_TABLE, tab_col, tab_key)


def get_module_config_by_tab_key(tab_key: str) -> ModuleConfig:
    schema = get_schema()
    row = get_module_by_tab_key(tab_key)
    cfg_col = _config_col(schema)
    return parse_module_config(row.get(cfg_col))


def _tab_key_col(schema: str) -> str:
    return pick_existing_column(schema, MODULE_TABLE, ["tab_key", "key", "tab"])


def _config_col(schema: str) -> str:
    return pick_existing_column(schema, MODULE_TABLE, ["config_json", "config", "config_text"])


def _ensure_datasets_exist(schema: str, dataset_ids: list[int]) -> None:
    """
    校验 module_config 中引用的 dataset_id 必须存在。
    """
    if not dataset_ids:
        raise ValidationError("datasets 不能为空")

    from zidingyi_baobiao.core.db import get_table_pk, get_conn
    from psycopg2 import sql

    pk = get_table_pk(schema, DATASET_TABLE)
    ids = sorted({int(x) for x in dataset_ids if int(x) > 0})
    if not ids:
        raise ValidationError("datasets.dataset_id 不能为空")

    stmt = sql.SQL("SELECT {} FROM {}.{} WHERE {} = ANY(%s)").format(
        sql.Identifier(pk),
        sql.Identifier(schema),
        sql.Identifier(DATASET_TABLE),
        sql.Identifier(pk),
    )

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(stmt, (ids,))
            rows = cur.fetchall()
        existing = {int(r[0]) for r in rows}
        missing = [x for x in ids if x not in existing]
        if missing:
            raise ValidationError(f"datasets.dataset_id 不存在：{missing}")
    finally:
        conn.close()


def _config_to_dict(cfg: ModuleConfig) -> Dict[str, Any]:
    return {
        "datasets": [
            {"dataset_id": d.dataset_id, "alias": d.alias, "dimension_mapping": d.dimension_mapping} for d in cfg.datasets
        ],
        "filters": [{"dim": f.dim, "op": f.op, "control": f.control, "required": f.required} for f in cfg.filters],
        "groups": cfg.groups,
        "metrics": [
            {
                "key": m.key,
                "label": m.label,
                "agg": m.agg,
                **({"field": m.field} if m.field else {}),
                **({"dim": m.dim} if m.dim else {}),
            }
            for m in cfg.metrics
        ],
        "columns": [
            {
                "type": c.type,
                "label": c.label,
                **({"dim": c.dim} if c.dim else {}),
                **({"key": c.key} if c.key else {}),
            }
            for c in cfg.columns
        ],
        "merge_rule": cfg.merge_rule,
        "export": {"allow": cfg.export.allow, "formats": cfg.export.formats},
    }

