from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from zidingyi_baobiao.core.exceptions import ValidationError


@dataclass(frozen=True)
class DatasetRef:
    dataset_id: int
    alias: str
    dimension_mapping: Dict[str, str]  # 语义维度 -> 结果字段名


@dataclass(frozen=True)
class FilterDef:
    dim: str
    op: str
    control: str
    required: bool


@dataclass(frozen=True)
class MetricDef:
    key: str
    label: str
    agg: str  # count/sum/min/max/avg
    field: Optional[str] = None  # sum/min/max/avg 时需要
    dim: Optional[str] = None  # 也可用语义维度指定字段来源


@dataclass(frozen=True)
class ColumnDef:
    type: str  # dimension/metric
    label: str
    dim: Optional[str] = None
    key: Optional[str] = None


@dataclass(frozen=True)
class ExportDef:
    allow: bool
    formats: List[str]


@dataclass(frozen=True)
class ModuleConfig:
    datasets: List[DatasetRef]
    filters: List[FilterDef]
    groups: List[str]
    metrics: List[MetricDef]
    columns: List[ColumnDef]
    merge_rule: str
    export: ExportDef


def parse_module_config(raw: Any) -> ModuleConfig:
    """
    校验 module_def.config_json 结构。
    """
    data = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception as exc:
            raise ValidationError(f"config_json 不是合法 JSON：{exc}") from None

    if not isinstance(data, dict):
        raise ValidationError("config_json 必须为 JSON 对象")

    datasets_raw = data.get("datasets") or []
    if not isinstance(datasets_raw, list) or not datasets_raw:
        raise ValidationError("config_json.datasets 不能为空")

    datasets: List[DatasetRef] = []
    for i, item in enumerate(datasets_raw):
        if not isinstance(item, dict):
            raise ValidationError(f"datasets[{i}] 必须为对象")
        dataset_id = int(item.get("dataset_id") or 0)
        if not dataset_id:
            raise ValidationError(f"datasets[{i}].dataset_id 为必填")
        alias = str(item.get("alias") or f"dataset_{dataset_id}").strip()
        dm = item.get("dimension_mapping") or {}
        if not isinstance(dm, dict) or not dm:
            raise ValidationError(f"datasets[{i}].dimension_mapping 不能为空")
        dimension_mapping: Dict[str, str] = {}
        for k, v in dm.items():
            dk = str(k or "").strip()
            dv = str(v or "").strip()
            if not dk or not dv:
                raise ValidationError(f"datasets[{i}].dimension_mapping 存在空 key/value")
            dimension_mapping[dk] = dv
        datasets.append(DatasetRef(dataset_id=dataset_id, alias=alias, dimension_mapping=dimension_mapping))

    filters: List[FilterDef] = []
    filters_raw = data.get("filters") or []
    if filters_raw and not isinstance(filters_raw, list):
        raise ValidationError("filters 必须为数组")
    for i, item in enumerate(filters_raw or []):
        if not isinstance(item, dict):
            raise ValidationError(f"filters[{i}] 必须为对象")
        dim = str(item.get("dim") or "").strip()
        op = str(item.get("op") or "").strip().lower()
        control = str(item.get("control") or "").strip()
        required = bool(item.get("required") is True)
        if not dim or not op:
            raise ValidationError(f"filters[{i}].dim/op 为必填")
        if op not in {"between", "eq", "in", "like", "gte", "lte"}:
            raise ValidationError(f"filters[{i}].op 不支持：{op}")
        filters.append(FilterDef(dim=dim, op=op, control=control, required=required))

    groups_raw = data.get("groups") or []
    if groups_raw and not isinstance(groups_raw, list):
        raise ValidationError("groups 必须为数组")
    groups = [str(x or "").strip() for x in groups_raw or [] if str(x or "").strip()]

    metrics_raw = data.get("metrics") or []
    if metrics_raw and not isinstance(metrics_raw, list):
        raise ValidationError("metrics 必须为数组")
    metrics: List[MetricDef] = []
    for i, item in enumerate(metrics_raw or []):
        if not isinstance(item, dict):
            raise ValidationError(f"metrics[{i}] 必须为对象")
        key = str(item.get("key") or "").strip()
        label = str(item.get("label") or key).strip() or key
        agg = str(item.get("agg") or "").strip().lower()
        field = str(item.get("field") or "").strip() or None
        dim = str(item.get("dim") or "").strip() or None
        if not key or not agg:
            raise ValidationError(f"metrics[{i}].key/agg 为必填")
        if agg not in {"count", "sum", "min", "max", "avg"}:
            raise ValidationError(f"metrics[{i}].agg 不支持：{agg}")
        if agg != "count" and not (field or dim):
            raise ValidationError(f"metrics[{i}] agg={agg} 需要提供 field 或 dim")
        metrics.append(MetricDef(key=key, label=label, agg=agg, field=field, dim=dim))

    columns_raw = data.get("columns") or []
    if columns_raw and not isinstance(columns_raw, list):
        raise ValidationError("columns 必须为数组")
    columns: List[ColumnDef] = []
    for i, item in enumerate(columns_raw or []):
        if not isinstance(item, dict):
            raise ValidationError(f"columns[{i}] 必须为对象")
        ctype = str(item.get("type") or "").strip().lower()
        label = str(item.get("label") or "").strip()
        dim = str(item.get("dim") or "").strip() or None
        key = str(item.get("key") or "").strip() or None
        if ctype not in {"dimension", "metric"}:
            raise ValidationError(f"columns[{i}].type 必须为 dimension/metric")
        if ctype == "dimension" and not dim:
            raise ValidationError(f"columns[{i}] type=dimension 必须提供 dim")
        if ctype == "metric" and not key:
            raise ValidationError(f"columns[{i}] type=metric 必须提供 key")
        columns.append(ColumnDef(type=ctype, label=label or (dim or key or ""), dim=dim, key=key))

    merge_rule = str(data.get("merge_rule") or "merge_by_dimension").strip()
    if merge_rule not in {"merge_by_dimension", "append"}:
        raise ValidationError("merge_rule 仅支持 merge_by_dimension/append")

    export_raw = data.get("export") or {}
    if export_raw and not isinstance(export_raw, dict):
        raise ValidationError("export 必须为对象")
    allow = bool((export_raw or {}).get("allow") is True)
    formats_raw = (export_raw or {}).get("formats") or []
    if formats_raw and not isinstance(formats_raw, list):
        raise ValidationError("export.formats 必须为数组")
    formats = [str(x or "").strip().lower() for x in formats_raw if str(x or "").strip()]
    for f in formats:
        if f not in {"csv", "xlsx"}:
            raise ValidationError(f"不支持导出格式：{f}")

    export = ExportDef(allow=allow, formats=formats)

    _ensure_required_time_filter(filters)
    _ensure_dimension_mappings_cover_usage(datasets, filters, groups, metrics, columns)

    return ModuleConfig(
        datasets=datasets,
        filters=filters,
        groups=groups,
        metrics=metrics,
        columns=columns,
        merge_rule=merge_rule,
        export=export,
    )


def _ensure_required_time_filter(filters: Sequence[FilterDef]) -> None:
    """
    强制至少一个 required 的 between 时间范围过滤。
    """
    for f in filters:
        if f.required and f.op == "between":
            return
    raise ValidationError("filters 必须至少包含一个 required=true 且 op=between 的时间范围过滤")


def _ensure_dimension_mappings_cover_usage(
    datasets: Sequence[DatasetRef],
    filters: Sequence[FilterDef],
    groups: Sequence[str],
    metrics: Sequence[MetricDef],
    columns: Sequence[ColumnDef],
) -> None:
    """
    校验 datasets[].dimension_mapping 必须覆盖被使用到的语义维度：
    - filters.dim
    - groups
    - columns(type=dimension).dim
    - metrics.dim（若使用）
    """
    required_dims = set(groups)
    required_dims.update([f.dim for f in filters])
    required_dims.update([c.dim for c in columns if c.type == "dimension" and c.dim])
    required_dims.update([m.dim for m in metrics if m.dim])
    required_dims = {d for d in required_dims if d}

    for ds in datasets:
        missing = sorted(required_dims - set(ds.dimension_mapping.keys()))
        if missing:
            raise ValidationError(f"dataset({ds.dataset_id}).dimension_mapping 缺少维度：{missing}")
