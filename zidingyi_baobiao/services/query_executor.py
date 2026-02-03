from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import select
from sqlalchemy.sql import text as sql_text

from zidingyi_baobiao.core.exceptions import QueryExecutionError, ValidationError
from zidingyi_baobiao.models.meta_tables import get_meta_tables
from zidingyi_baobiao.schemas.module_config import ColumnDef, FilterDef, MetricDef, ModuleConfig, parse_module_config
from zidingyi_baobiao.services._row_accessors import first_key, first_of
from zidingyi_baobiao.services.datasource_service import get_datasource_row, get_engine_for_datasource_row
from zidingyi_baobiao.services.module_service import get_module_by_tab_key
from zidingyi_baobiao.utils.sql_validator import extract_named_params, validate_sql_template, wrap_limit


DEFAULT_QUERY_LIMIT = 2000
DEFAULT_TIMEOUT_MS = 300_000
DEFAULT_MAX_ROWS = 100_000
EXPORT_MAX_ROWS = 100_000


@dataclass(frozen=True)
class QueryResult:
    columns: List[Dict[str, Any]]
    rows: List[Dict[str, Any]]


class QueryExecutor:
    """
    查询执行引擎（核心模块）。

    职责：
    - 解析 module_def.config_json
    - 多 dataset 串行执行（便于控制超时与错误返回）
    - 维度字段映射（语义维度 -> 统一字段）
    - Python 层按维度聚合合并（merge_by_dimension）
    """

    def execute(self, tab_key: str, raw_params: Mapping[str, Any], *, export: bool) -> QueryResult:
        module_row = get_module_by_tab_key(tab_key)
        cfg = parse_module_config(module_row.get(_config_col(get_meta_tables().module_def)))

        rows_all: List[Dict[str, Any]] = []
        for ds_cfg in cfg.datasets:
            dataset_row = _get_dataset_row(ds_cfg.dataset_id)
            sql_template = _get_dataset_sql(dataset_row)
            validate_sql_template(sql_template)

            data_source_id = int(first_of(dataset_row, "data_source_id", default=0) or 0)
            if not data_source_id:
                raise ValidationError(f"dataset({ds_cfg.dataset_id}).data_source_id 为空")

            timeout_ms = int(first_of(dataset_row, "timeout_ms", default=DEFAULT_TIMEOUT_MS) or DEFAULT_TIMEOUT_MS)
            max_rows = int(first_of(dataset_row, "max_rows", default=DEFAULT_MAX_ROWS) or DEFAULT_MAX_ROWS)
            if timeout_ms <= 0:
                timeout_ms = DEFAULT_TIMEOUT_MS
            if max_rows <= 0:
                max_rows = DEFAULT_MAX_ROWS

            if export:
                limit = min(max_rows, EXPORT_MAX_ROWS)
            else:
                limit = min(max_rows, DEFAULT_QUERY_LIMIT)

            named_params = extract_named_params(sql_template)
            bind_params = _build_dataset_bind_params(
                cfg_filters=cfg.filters,
                dimension_mapping=ds_cfg.dimension_mapping,
                request_params=raw_params,
                named_params=named_params,
            )

            # 强制 LIMIT（非导出必须；导出也用来兜底 max_rows）
            sql_to_run = wrap_limit(sql_template, limit_param="_limit")
            bind_params["_limit"] = limit

            ds_row = get_datasource_row(data_source_id)
            engine = get_engine_for_datasource_row(ds_row)

            try:
                with engine.begin() as conn:
                    # 每次执行前设置 statement_timeout（毫秒），避免单条 SQL 拖垮服务
                    conn.exec_driver_sql(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                    result_rows = conn.execute(sql_text(sql_to_run), bind_params).mappings().all()
            except Exception as exc:
                logging.exception("dataset query failed: dataset_id=%s tab_key=%s", ds_cfg.dataset_id, tab_key)
                raise QueryExecutionError(f"dataset({ds_cfg.dataset_id}) 查询失败：{exc}") from None

            # 维度字段映射：输出统一语义维度字段，同时保留原字段，便于指标计算
            for r in result_rows:
                mapped = dict(r)
                for dim, field in ds_cfg.dimension_mapping.items():
                    mapped[dim] = r.get(field)
                mapped["_dataset_id"] = ds_cfg.dataset_id
                mapped["_dataset_alias"] = ds_cfg.alias
                rows_all.append(mapped)

        merged_rows = _merge_rows(cfg, rows_all)
        columns = _build_output_columns(cfg.columns, cfg.metrics)
        output_rows = _project_columns(merged_rows, cfg.columns)
        return QueryResult(columns=columns, rows=output_rows)


def _merge_rows(cfg: ModuleConfig, rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    if cfg.merge_rule == "append":
        return [dict(r) for r in rows]
    return _merge_by_dimension(groups=cfg.groups, metrics=cfg.metrics, rows=rows)


def _merge_by_dimension(*, groups: Sequence[str], metrics: Sequence[MetricDef], rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """按语义维度分组聚合。"""
    buckets: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    avg_state: Dict[Tuple[Any, ...], Dict[str, Tuple[float, int]]] = {}

    for row in rows:
        key = tuple(row.get(dim) for dim in groups)
        if key not in buckets:
            base: Dict[str, Any] = {dim: row.get(dim) for dim in groups}
            for m in metrics:
                base[m.key] = 0 if m.agg in {"count", "sum"} else None
            buckets[key] = base
            avg_state[key] = {}

        agg_row = buckets[key]
        for m in metrics:
            if m.agg == "count":
                agg_row[m.key] = int(agg_row.get(m.key) or 0) + 1
                continue

            val = _metric_value(row, m)
            if val is None:
                continue

            if m.agg == "sum":
                agg_row[m.key] = (agg_row.get(m.key) or 0) + _to_number(val)
            elif m.agg == "min":
                agg_row[m.key] = val if agg_row.get(m.key) is None else min(agg_row[m.key], val)  # type: ignore[arg-type]
            elif m.agg == "max":
                agg_row[m.key] = val if agg_row.get(m.key) is None else max(agg_row[m.key], val)  # type: ignore[arg-type]
            elif m.agg == "avg":
                s, c = avg_state[key].get(m.key, (0.0, 0))
                s += float(_to_number(val))
                c += 1
                avg_state[key][m.key] = (s, c)
            else:
                raise ValidationError(f"不支持的指标聚合：{m.agg}")

    for key, state in avg_state.items():
        for mkey, (s, c) in state.items():
            if c > 0:
                buckets[key][mkey] = s / c

    return list(buckets.values())


def _metric_value(row: Mapping[str, Any], metric: MetricDef) -> Any:
    """从行中取指标字段值。优先 field，其次 dim（语义维度）。"""
    if metric.field and metric.field in row:
        return row.get(metric.field)
    if metric.dim and metric.dim in row:
        return row.get(metric.dim)
    if metric.key in row:
        return row.get(metric.key)
    return None


def _to_number(val: Any) -> float:
    if isinstance(val, bool):
        return 1.0 if val else 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val))
    except Exception:
        raise ValidationError(f"指标值不是数字：{val}") from None


def _build_dataset_bind_params(
    *,
    cfg_filters: Sequence[FilterDef],
    dimension_mapping: Mapping[str, str],
    request_params: Mapping[str, Any],
    named_params: Iterable[str],
) -> Dict[str, Any]:
    """将 module filters + 语义维度映射，转换为 dataset SQL 所需命名参数。"""
    allowed = set(named_params)
    out: Dict[str, Any] = {}

    required_between = [f for f in cfg_filters if f.required and f.op == "between"]
    allow_short_between = len(required_between) == 1

    for f in cfg_filters:
        dim = f.dim
        field = dimension_mapping.get(dim)
        if not field:
            raise ValidationError(f"dimension_mapping 缺少维度映射：{dim}")

        if f.op == "between":
            start_val = _pick_param(request_params, f"{dim}_start", f"{field}_start")
            end_val = _pick_param(request_params, f"{dim}_end", f"{field}_end")
            if allow_short_between:
                start_val = start_val if start_val is not None else _pick_param(request_params, "start")
                end_val = end_val if end_val is not None else _pick_param(request_params, "end")

            if f.required and (start_val in (None, "") or end_val in (None, "")):
                raise ValidationError(f"时间范围过滤必填：{dim}（需要 {dim}_start/{dim}_end）")

            start_key = f"{field}_start"
            end_key = f"{field}_end"
            if start_key in allowed:
                out[start_key] = start_val
            if end_key in allowed:
                out[end_key] = end_val

            if f.required and (start_key not in allowed or end_key not in allowed):
                raise ValidationError(
                    f"dataset SQL 未声明时间参数 :{start_key}/:{end_key}（请检查 SQL 模板与 dimension_mapping）"
                )
            continue

        value = _pick_param(request_params, dim, field)
        if f.required and value in (None, ""):
            raise ValidationError(f"过滤条件必填：{dim}")
        if value is None:
            continue

        if f.op == "in" and isinstance(value, str):
            value = [x.strip() for x in value.split(",") if x.strip()]

        if field in allowed:
            out[field] = value
        elif f.required:
            raise ValidationError(f"dataset SQL 未声明参数 :{field}（请检查 SQL 模板与 dimension_mapping）")

    return out


def _pick_param(params: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in params and params[k] not in (None, ""):
            return params[k]
    return None


def _project_columns(rows: Sequence[Mapping[str, Any]], columns: Sequence[ColumnDef]) -> List[Dict[str, Any]]:
    if not columns:
        return [dict(r) for r in rows]
    out: List[Dict[str, Any]] = []
    for r in rows:
        item: Dict[str, Any] = {}
        for c in columns:
            if c.type == "dimension" and c.dim:
                item[c.dim] = r.get(c.dim)
            elif c.type == "metric" and c.key:
                item[c.key] = r.get(c.key)
        out.append(item)
    return out


def _build_output_columns(columns: Sequence[ColumnDef], metrics: Sequence[MetricDef]) -> List[Dict[str, Any]]:
    metric_map = {m.key: m for m in metrics}
    out: List[Dict[str, Any]] = []
    for c in columns:
        if c.type == "dimension" and c.dim:
            out.append({"type": "dimension", "key": c.dim, "label": c.label or c.dim})
        elif c.type == "metric" and c.key:
            m = metric_map.get(c.key)
            out.append({"type": "metric", "key": c.key, "label": c.label or (m.label if m else c.key)})
    return out


def _get_dataset_row(dataset_id: int) -> Mapping[str, Any]:
    tables = get_meta_tables()
    dt = tables.dataset
    pk = _pk_column_name(dt)
    if not pk:
        raise ValidationError("dataset 缺少主键，无法查询")
    with tables.engine.connect() as conn:
        row = conn.execute(select(dt).where(dt.c[pk] == dataset_id)).mappings().first()
    if not row:
        raise ValidationError(f"dataset 不存在：{dataset_id}")
    return row


def _get_dataset_sql(row: Mapping[str, Any]) -> str:
    key = first_key(row, "sql_template", "sql_text", "sql", "template_sql")
    if not key:
        raise ValidationError("dataset 缺少 SQL 字段（sql_template/sql_text/sql）")
    sql = str(row.get(key) or "").strip()
    if not sql:
        raise ValidationError("dataset SQL 为空")
    return sql


def _config_col(table) -> str:  # type: ignore[no-untyped-def]
    key = first_key(table.c, "config_json", "config", "config_text")
    if not key:
        raise ValidationError("module_def 表缺少 config_json 字段")
    return key


def _pk_column_name(table) -> Optional[str]:  # type: ignore[no-untyped-def]
    pks = list(table.primary_key.columns)
    if pks:
        return pks[0].name
    if "id" in table.c:
        return "id"
    return None

