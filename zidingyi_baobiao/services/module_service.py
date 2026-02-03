from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sqlalchemy import Table, insert, select, update
from sqlalchemy.engine import RowMapping

from zidingyi_baobiao.core.exceptions import NotFoundError, ValidationError
from zidingyi_baobiao.models.meta_tables import get_meta_tables
from zidingyi_baobiao.schemas.module_config import ModuleConfig, parse_module_config
from zidingyi_baobiao.services._row_accessors import first_key


def create_module(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    新增 module_def（Tab 配置）。
    """
    tables = get_meta_tables()
    mt = tables.module_def

    tab_key = str(payload.get("tab_key") or "").strip()
    if not tab_key:
        raise ValidationError("tab_key 为必填")

    config_json = payload.get("config_json")
    config = parse_module_config(config_json)
    _ensure_datasets_exist([d.dataset_id for d in config.datasets])

    data: Dict[str, Any] = {
        _tab_key_col(mt): tab_key,
        _config_col(mt): json.dumps(_config_to_dict(config), ensure_ascii=False),
    }
    if "name" in mt.c:
        data["name"] = str(payload.get("name") or tab_key).strip()

    data = _only_existing_columns(mt, data)

    pk = _pk_column_name(mt)
    with tables.engine.begin() as conn:
        stmt = insert(mt).values(**data)
        if pk:
            stmt = stmt.returning(mt.c[pk])
        res = conn.execute(stmt)
        new_id = res.scalar_one() if pk else None
    return {"id": new_id, "tab_key": tab_key}


def update_module(module_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    更新 module_def。
    """
    tables = get_meta_tables()
    mt = tables.module_def
    row = get_module_row(module_id)

    update_data: Dict[str, Any] = {}
    if "tab_key" in payload:
        update_data[_tab_key_col(mt)] = str(payload.get("tab_key") or "").strip()
    if "name" in payload and "name" in mt.c:
        update_data["name"] = str(payload.get("name") or "").strip()
    if "config_json" in payload:
        config = parse_module_config(payload.get("config_json"))
        _ensure_datasets_exist([d.dataset_id for d in config.datasets])
        update_data[_config_col(mt)] = json.dumps(_config_to_dict(config), ensure_ascii=False)

    update_data = _only_existing_columns(mt, update_data)
    if not update_data:
        return {"id": module_id}

    pk = _pk_column_name(mt)
    if not pk:
        raise ValidationError("module_def 缺少主键，无法更新")

    with tables.engine.begin() as conn:
        conn.execute(update(mt).where(mt.c[pk] == module_id).values(**update_data))
    return {"id": module_id}


def list_modules() -> Dict[str, Any]:
    tables = get_meta_tables()
    mt = tables.module_def
    pk = _pk_column_name(mt) or "id"
    with tables.engine.connect() as conn:
        rows = conn.execute(select(mt)).mappings().all()
    items = []
    for r in rows:
        item = dict(r)
        if pk in item:
            item["id"] = item.get(pk)
        items.append(item)
    return {"items": items, "count": len(items)}


def get_module_by_tab_key(tab_key: str) -> RowMapping:
    tables = get_meta_tables()
    mt = tables.module_def
    col = _tab_key_col(mt)
    with tables.engine.connect() as conn:
        row = conn.execute(select(mt).where(mt.c[col] == tab_key)).mappings().first()
    if not row:
        raise NotFoundError(f"module_def 不存在：{tab_key}")
    return row


def get_module_row(module_id: int) -> RowMapping:
    tables = get_meta_tables()
    mt = tables.module_def
    pk = _pk_column_name(mt)
    if not pk:
        raise ValidationError("module_def 缺少主键，无法查询")
    with tables.engine.connect() as conn:
        row = conn.execute(select(mt).where(mt.c[pk] == module_id)).mappings().first()
    if not row:
        raise NotFoundError(f"module_def 不存在：{module_id}")
    return row


def get_module_config_by_tab_key(tab_key: str) -> ModuleConfig:
    row = get_module_by_tab_key(tab_key)
    config_raw = row.get(_config_col(get_meta_tables().module_def))
    return parse_module_config(config_raw)


def _ensure_datasets_exist(dataset_ids: Sequence[int]) -> None:
    """
    校验 module_config 中引用的 dataset_id 必须存在。
    """
    tables = get_meta_tables()
    dt = tables.dataset
    pk = _pk_column_name(dt)
    if not pk:
        raise ValidationError("dataset 缺少主键，无法校验 dataset_id 存在性")
    ids = sorted({int(x) for x in dataset_ids if int(x) > 0})
    if not ids:
        raise ValidationError("datasets 不能为空")
    with tables.engine.connect() as conn:
        rows = conn.execute(select(dt.c[pk]).where(dt.c[pk].in_(ids))).all()
    existing = {int(r[0]) for r in rows}
    missing = [x for x in ids if x not in existing]
    if missing:
        raise ValidationError(f"datasets.dataset_id 不存在：{missing}")


def _only_existing_columns(table: Table, data: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if k in table.c}


def _pk_column_name(table: Table) -> Optional[str]:
    pks = list(table.primary_key.columns)
    if pks:
        return pks[0].name
    if "id" in table.c:
        return "id"
    return None


def _tab_key_col(table: Table) -> str:
    key = first_key(table.c, "tab_key", "key", "tab")
    if not key:
        raise ValidationError("module_def 表缺少 tab_key 字段")
    return key


def _config_col(table: Table) -> str:
    key = first_key(table.c, "config_json", "config", "config_text")
    if not key:
        raise ValidationError("module_def 表缺少 config_json 字段")
    return key


def _config_to_dict(cfg: ModuleConfig) -> Dict[str, Any]:
    return {
        "datasets": [
            {"dataset_id": d.dataset_id, "alias": d.alias, "dimension_mapping": d.dimension_mapping} for d in cfg.datasets
        ],
        "filters": [{"dim": f.dim, "op": f.op, "control": f.control, "required": f.required} for f in cfg.filters],
        "groups": cfg.groups,
        "metrics": [
            {"key": m.key, "label": m.label, "agg": m.agg, **({"field": m.field} if m.field else {}), **({"dim": m.dim} if m.dim else {})}
            for m in cfg.metrics
        ],
        "columns": [
            {"type": c.type, "label": c.label, **({"dim": c.dim} if c.dim else {}), **({"key": c.key} if c.key else {})}
            for c in cfg.columns
        ],
        "merge_rule": cfg.merge_rule,
        "export": {"allow": cfg.export.allow, "formats": cfg.export.formats},
    }

