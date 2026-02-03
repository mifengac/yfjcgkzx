from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, jsonify, request

from zidingyi_baobiao.services.datasource_service import (
    create_datasource,
    list_datasources,
    test_datasource,
    update_datasource,
)


datasource_bp = Blueprint("zdybb_datasource", __name__)


@datasource_bp.post("/datasource")
def api_create_datasource():  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = create_datasource(payload)
    return jsonify({"success": True, "data": result})


@datasource_bp.put("/datasource/<int:datasource_id>")
def api_update_datasource(datasource_id: int):  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = update_datasource(datasource_id, payload)
    return jsonify({"success": True, "data": result})


@datasource_bp.get("/datasource")
def api_list_datasource():  # type: ignore[no-untyped-def]
    result = list_datasources()
    return jsonify({"success": True, "data": result})


@datasource_bp.post("/datasource/<int:datasource_id>/test")
def api_test_datasource(datasource_id: int):  # type: ignore[no-untyped-def]
    result = test_datasource(datasource_id)
    ok = bool(result.get("success") is True)
    status = 200 if ok else 400
    return jsonify(result), status

