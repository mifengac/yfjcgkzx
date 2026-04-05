from __future__ import annotations

import re
import uuid
from typing import Any, Dict, Sequence

from jingqing_fenxi.dao import custom_case_monitor_dao as dao
from jingqing_fenxi.service.special_case_tab_service import (
    CUSTOM_CASE_MONITOR_LABEL,
    build_defaults_payload as build_special_case_defaults_payload,
    build_export_filename,
    export_special_case_records,
    field_options,
    operator_options,
    query_special_case_records,
    validate_scheme_rules,
)


def _normalize_scheme_name(value: Any) -> str:
    return str(value or "").strip()


def _normalize_scheme_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    scheme_name = _normalize_scheme_name(payload.get("scheme_name"))
    if not scheme_name:
        raise ValueError("方案名称不能为空")

    description = str(payload.get("description") or "").strip()
    is_enabled = bool(payload.get("is_enabled", True))
    rules = validate_scheme_rules(payload.get("rules") or [])
    return {
        "scheme_name": scheme_name,
        "description": description,
        "is_enabled": is_enabled,
        "rules": rules,
    }


def _ensure_unique_name(scheme_name: str, exclude_scheme_id: int | None = None) -> None:
    for row in dao.list_schemes(include_disabled=True):
        if exclude_scheme_id is not None and int(row["id"]) == int(exclude_scheme_id):
            continue
        if str(row.get("scheme_name") or "").strip() == scheme_name:
            raise ValueError(f"方案名称已存在：{scheme_name}")


def _build_scheme_code(scheme_name: str) -> str:
    ascii_part = re.sub(r"[^a-z0-9]+", "_", scheme_name.lower()).strip("_")
    prefix = ascii_part or "scheme"
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def build_defaults_payload() -> Dict[str, Any]:
    enabled_schemes = dao.list_schemes(include_disabled=False)
    selected_scheme_id = enabled_schemes[0]["id"] if enabled_schemes else None
    payload = build_special_case_defaults_payload()
    payload.update(
        {
            "label": CUSTOM_CASE_MONITOR_LABEL,
            "schemes": enabled_schemes,
            "selected_scheme_id": selected_scheme_id,
            "field_options": field_options(),
            "operator_options": operator_options(),
        }
    )
    return payload


def list_scheme_payload() -> Dict[str, Any]:
    return {
        "success": True,
        "label": CUSTOM_CASE_MONITOR_LABEL,
        "schemes": dao.list_schemes(include_disabled=True),
        "field_options": field_options(),
        "operator_options": operator_options(),
    }


def create_scheme(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_scheme_payload(payload)
    _ensure_unique_name(normalized["scheme_name"])
    scheme = dao.create_scheme(
        scheme_name=normalized["scheme_name"],
        scheme_code=_build_scheme_code(normalized["scheme_name"]),
        description=normalized["description"],
        is_enabled=normalized["is_enabled"],
        rules=normalized["rules"],
    )
    return {"success": True, "scheme": scheme}


def update_scheme(scheme_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_scheme_payload(payload)
    _ensure_unique_name(normalized["scheme_name"], exclude_scheme_id=scheme_id)
    scheme = dao.update_scheme(
        scheme_id,
        scheme_name=normalized["scheme_name"],
        description=normalized["description"],
        is_enabled=normalized["is_enabled"],
        rules=normalized["rules"],
    )
    if not scheme:
        raise ValueError("方案不存在")
    return {"success": True, "scheme": scheme}


def delete_scheme(scheme_id: int) -> Dict[str, Any]:
    if not dao.delete_scheme(scheme_id):
        raise ValueError("方案不存在")
    return {"success": True}


def _load_enabled_scheme_or_raise(scheme_id: Any) -> Dict[str, Any]:
    try:
        normalized_scheme_id = int(scheme_id)
    except Exception as exc:
        raise ValueError("scheme_id 参数不正确") from exc

    scheme = dao.get_scheme_by_id(normalized_scheme_id)
    if not scheme:
        raise ValueError("方案不存在")
    if not scheme.get("is_enabled"):
        raise ValueError("该方案已禁用，无法查询")
    return scheme


def query_custom_case_monitor_records(
    *,
    scheme_id: Any,
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
) -> Dict[str, Any]:
    scheme = _load_enabled_scheme_or_raise(scheme_id)
    return query_special_case_records(
        label=CUSTOM_CASE_MONITOR_LABEL,
        scheme_id=int(scheme["id"]),
        scheme_name=str(scheme["scheme_name"]),
        rules=scheme.get("rules") or [],
        start_time=start_time,
        end_time=end_time,
        branches=branches,
        page_num=page_num,
        page_size=page_size,
    )


def export_custom_case_monitor_records(
    *,
    export_format: str,
    scheme_id: Any,
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
):
    scheme = _load_enabled_scheme_or_raise(scheme_id)
    return export_special_case_records(
        label=CUSTOM_CASE_MONITOR_LABEL,
        scheme_name=str(scheme["scheme_name"]),
        rules=scheme.get("rules") or [],
        export_format=export_format,
        start_time=start_time,
        end_time=end_time,
        branches=branches,
        include_hit_keywords=True,
        download_name=build_export_filename(str(scheme["scheme_name"]), start_time, end_time, export_format),
    )
