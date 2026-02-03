from __future__ import annotations

from typing import Any, Dict

from flask import Blueprint, jsonify, request

from zidingyi_baobiao.services.dataset_service import create_dataset, list_datasets, preview_dataset, update_dataset


dataset_bp = Blueprint("zdybb_dataset", __name__)


@dataset_bp.post("/dataset")
def api_create_dataset():  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = create_dataset(payload)
    return jsonify({"success": True, "data": result})


@dataset_bp.put("/dataset/<int:dataset_id>")
def api_update_dataset(dataset_id: int):  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = update_dataset(dataset_id, payload)
    return jsonify({"success": True, "data": result})


@dataset_bp.get("/dataset")
def api_list_dataset():  # type: ignore[no-untyped-def]
    result = list_datasets()
    return jsonify({"success": True, "data": result})


@dataset_bp.post("/dataset/<int:dataset_id>/preview")
def api_preview_dataset(dataset_id: int):  # type: ignore[no-untyped-def]
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    result = preview_dataset(dataset_id, payload)
    return jsonify({"success": True, "data": result})

