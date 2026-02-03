from __future__ import annotations

from io import BytesIO
from typing import Any, Dict

from flask import Blueprint, Response, jsonify, request, send_file

from zidingyi_baobiao.core.exceptions import ValidationError
from zidingyi_baobiao.services.module_service import (
    create_module,
    get_module_config_by_tab_key,
    list_modules,
    update_module,
)
from zidingyi_baobiao.services.query_executor import EXPORT_MAX_ROWS, QueryExecutor
from zidingyi_baobiao.utils.export import build_export_columns, export_csv, export_xlsx


module_bp = Blueprint("zdybb_module", __name__)


@module_bp.post("/module")
def api_create_module():  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = create_module(payload)
    return jsonify({"success": True, "data": result})


@module_bp.put("/module/<int:module_id>")
def api_update_module(module_id: int):  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = update_module(module_id, payload)
    return jsonify({"success": True, "data": result})


@module_bp.get("/module")
def api_list_module():  # type: ignore[no-untyped-def]
    result = list_modules()
    return jsonify({"success": True, "data": result})


@module_bp.get("/module/<string:tab_key>/query")
def api_module_query(tab_key: str):  # type: ignore[no-untyped-def]
    params = {k: request.args.get(k) for k in request.args.keys()}
    executor = QueryExecutor()
    result = executor.execute(tab_key, params, export=False)
    return jsonify({"success": True, "data": {"columns": result.columns, "rows": result.rows, "count": len(result.rows)}})


@module_bp.post("/module/<string:tab_key>/export")
def api_module_export(tab_key: str):  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    fmt = str(payload.get("format") or "xlsx").strip().lower()
    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise ValidationError("params 必须为 JSON 对象")

    cfg = get_module_config_by_tab_key(tab_key)
    if not cfg.export.allow:
        raise ValidationError("该模块不允许导出")
    if fmt not in set(cfg.export.formats or []):
        raise ValidationError(f"不支持的导出格式：{fmt}")

    executor = QueryExecutor()
    result = executor.execute(tab_key, params, export=True)

    if len(result.rows) > EXPORT_MAX_ROWS:
        raise ValidationError(f"导出行数超限：{len(result.rows)} > {EXPORT_MAX_ROWS}")

    export_columns = build_export_columns([(c["key"], c["label"]) for c in result.columns])
    if fmt == "csv":
        data = export_csv(result.rows, export_columns)
        return _send_bytes(data, filename=f"{tab_key}.csv", mimetype="text/csv")
    if fmt == "xlsx":
        data = export_xlsx(result.rows, export_columns)
        return _send_bytes(
            data,
            filename=f"{tab_key}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    raise ValidationError(f"不支持的导出格式：{fmt}")


def _send_bytes(data: bytes, *, filename: str, mimetype: str) -> Response:
    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)
