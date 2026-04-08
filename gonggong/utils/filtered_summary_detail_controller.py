from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional, Sequence, Tuple

from flask import Response, jsonify, render_template, request

from gonggong.utils.summary_detail_controller import download_rows


SummaryPayloadBuilder = Callable[[Mapping[str, Any]], Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]]
DetailRowsLoader = Callable[[Mapping[str, Any], str, str, int], Tuple[Sequence[Mapping[str, Any]], bool]]
RegionNameResolver = Callable[[str], str]
FilenameBuilder = Callable[[Mapping[str, Any], str, str], str]


def _parse_list_arg(name: str) -> list[str]:
    values = request.args.getlist(name)
    return [str(value).strip() for value in values if str(value).strip()]


def _parse_bool_arg(name: str) -> bool:
    value = (request.args.get(name) or "").strip().lower()
    return value in ("1", "true", "yes", "on")


def _normalize_meta(meta: Mapping[str, Any] | Any) -> Mapping[str, Any]:
    if isinstance(meta, Mapping):
        return meta
    meta_dict = getattr(meta, "__dict__", None)
    if isinstance(meta_dict, Mapping):
        return meta_dict
    return {}


@dataclass(frozen=True)
class FilteredSummaryDetailController:
    list_arg_map: Mapping[str, str] = field(default_factory=dict)
    bool_arg_map: Mapping[str, str] = field(default_factory=dict)
    format_param_name: str = "fmt"
    metric_param_name: str = "metric"
    region_param_name: str = "diqu"
    default_region_code: str = "__ALL__"
    preserve_header_order: bool = True
    sheet_title: str = "数据"

    def parse_filters(
        self,
        default_time_range_factory: Callable[[], Tuple[str, str]],
    ) -> Dict[str, Any]:
        start_time = (request.args.get("start_time") or "").strip()
        end_time = (request.args.get("end_time") or "").strip()
        hb_start_time = (request.args.get("hb_start_time") or "").strip()
        hb_end_time = (request.args.get("hb_end_time") or "").strip()
        if not start_time or not end_time:
            start_time, end_time = default_time_range_factory()

        filters: Dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
            "hb_start_time": hb_start_time,
            "hb_end_time": hb_end_time,
        }
        for arg_name, filter_name in self.list_arg_map.items():
            filters[filter_name] = _parse_list_arg(arg_name)
        for arg_name, filter_name in self.bool_arg_map.items():
            filters[filter_name] = _parse_bool_arg(arg_name)
        return filters

    def build_summary_response(
        self,
        filters: Mapping[str, Any],
        summary_payload_builder: SummaryPayloadBuilder,
    ) -> Response:
        meta, rows = summary_payload_builder(filters)
        return jsonify({"success": True, "meta": _normalize_meta(meta), "rows": list(rows)})

    def build_summary_export_response(
        self,
        filters: Mapping[str, Any],
        summary_payload_builder: SummaryPayloadBuilder,
        filename_builder: FilenameBuilder,
    ) -> Response:
        export_format = self._get_export_format()
        _meta, rows = summary_payload_builder(filters)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = filename_builder(filters, export_format, timestamp)
        return download_rows(
            rows,
            filename,
            "csv" if export_format == "csv" else "excel",
            preserve_header_order=self.preserve_header_order,
            sheet_title=self.sheet_title,
        )

    def build_detail_page(
        self,
        *,
        filters: Mapping[str, Any],
        detail_rows_loader: DetailRowsLoader,
        region_name_resolver: RegionNameResolver,
        template_name: str,
        limit: int = 5000,
        extra_context: Optional[Mapping[str, Any]] = None,
    ) -> str:
        metric = self._get_metric()
        region_code = self._get_region_code()
        rows, truncated = detail_rows_loader(filters, metric, region_code, limit)
        context: MutableMapping[str, Any] = {
            self.metric_param_name: metric,
            self.region_param_name: region_code,
            "region_name": region_name_resolver(region_code),
            "rows": list(rows),
            "truncated": truncated,
            **dict(filters),
        }
        if extra_context:
            context.update(extra_context)
        return render_template(template_name, **context)

    def build_detail_export_response(
        self,
        *,
        filters: Mapping[str, Any],
        detail_rows_loader: DetailRowsLoader,
        region_name_resolver: RegionNameResolver,
        filename_builder: Callable[[Mapping[str, Any], str, str, str], str],
        limit: int = 0,
    ) -> Response:
        metric = self._get_metric()
        region_code = self._get_region_code()
        export_format = self._get_export_format()
        rows, _truncated = detail_rows_loader(filters, metric, region_code, limit)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = filename_builder(filters, region_name_resolver(region_code), export_format, timestamp)
        return download_rows(
            rows,
            filename,
            "csv" if export_format == "csv" else "excel",
            preserve_header_order=self.preserve_header_order,
            sheet_title=self.sheet_title,
        )

    def _get_metric(self) -> str:
        return (request.args.get(self.metric_param_name) or "").strip()

    def _get_region_code(self) -> str:
        region_code = (request.args.get(self.region_param_name) or "").strip()
        return region_code or self.default_region_code

    def _get_export_format(self) -> str:
        return (request.args.get(self.format_param_name) or "xlsx").strip().lower()
