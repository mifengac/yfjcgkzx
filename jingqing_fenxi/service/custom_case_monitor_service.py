from __future__ import annotations

import re
import threading
import time
import uuid
from copy import deepcopy
from typing import Any, Dict, Sequence

from jingqing_fenxi.dao import custom_case_monitor_dao as dao
from jingqing_fenxi.service.special_case_tab_service import (
    CUSTOM_CASE_MONITOR_LABEL,
    ProgressCallback,
    build_defaults_payload as build_special_case_defaults_payload,
    build_export_filename,
    export_special_case_records,
    field_options,
    operator_options,
    query_special_case_records,
    validate_scheme_rules,
)


_STATUS_LOCK = threading.Lock()
_QUERY_JOB_STATUS: Dict[tuple[str, str], Dict[str, Any]] = {}
_QUERY_JOB_TTL_SECONDS = 15 * 60


def _default_query_stats() -> Dict[str, int]:
    return {
        "upstream_row_count": 0,
        "rule_scanned_count": 0,
        "rule_match_count": 0,
        "branch_scanned_count": 0,
        "branch_filtered_count": 0,
    }


def _cleanup_expired_query_jobs() -> None:
    expire_before = time.time() - _QUERY_JOB_TTL_SECONDS
    with _STATUS_LOCK:
        expired_keys = [
            key
            for key, value in _QUERY_JOB_STATUS.items()
            if float(value.get("updated_at") or value.get("created_at") or 0) < expire_before
        ]
        for key in expired_keys:
            _QUERY_JOB_STATUS.pop(key, None)


def _update_query_job_status(key: tuple[str, str], **patch: Any) -> None:
    with _STATUS_LOCK:
        current = _QUERY_JOB_STATUS.get(key)
        if not current:
            return
        for field, value in patch.items():
            current[field] = value
        current["updated_at"] = time.time()


def _build_query_job_progress(stage: str, stats: Dict[str, int]) -> Dict[str, int]:
    if stage == "rule_filtering":
        return {
            "current": int(stats.get("rule_scanned_count") or 0),
            "total": int(stats.get("upstream_row_count") or 0),
        }
    if stage == "branch_filtering":
        return {
            "current": int(stats.get("branch_scanned_count") or 0),
            "total": int(stats.get("rule_match_count") or 0),
        }
    if stage == "done":
        final_count = int(stats.get("branch_filtered_count") or 0)
        return {"current": final_count, "total": final_count}
    upstream_count = int(stats.get("upstream_row_count") or 0)
    return {"current": upstream_count, "total": upstream_count}


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


def _query_records_with_scheme(
    *,
    scheme: Dict[str, Any],
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
    progress_callback: ProgressCallback | None = None,
) -> Dict[str, Any]:
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
        progress_callback=progress_callback,
        include_hit_keyword_details=True,
    )


def query_custom_case_monitor_records(
    *,
    scheme_id: Any,
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
    progress_callback: ProgressCallback | None = None,
) -> Dict[str, Any]:
    scheme = _load_enabled_scheme_or_raise(scheme_id)
    return _query_records_with_scheme(
        scheme=scheme,
        start_time=start_time,
        end_time=end_time,
        branches=branches,
        page_num=page_num,
        page_size=page_size,
        progress_callback=progress_callback,
    )


def start_query_custom_case_monitor_job(
    *,
    username: str,
    scheme_id: Any,
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
) -> str:
    scheme = _load_enabled_scheme_or_raise(scheme_id)
    _cleanup_expired_query_jobs()
    job_id = uuid.uuid4().hex
    key = (username or "", job_id)
    now = time.time()
    with _STATUS_LOCK:
        _QUERY_JOB_STATUS[key] = {
            "job_id": job_id,
            "username": username or "",
            "created_at": now,
            "updated_at": now,
            "state": "queued",
            "stage": "fetching",
            "message": "任务已创建，准备开始查询...",
            "progress": {"current": 0, "total": 0},
            "stats": _default_query_stats(),
            "result": None,
        }

    worker = threading.Thread(
        target=_run_query_job,
        kwargs={
            "key": key,
            "scheme": deepcopy(scheme),
            "start_time": start_time,
            "end_time": end_time,
            "branches": list(branches or []),
            "page_num": int(page_num or 1),
            "page_size": int(page_size or 15),
        },
        daemon=True,
    )
    worker.start()
    return job_id


def get_query_custom_case_monitor_job_status(*, username: str, job_id: str) -> Dict[str, Any] | None:
    _cleanup_expired_query_jobs()
    key = (username or "", job_id)
    with _STATUS_LOCK:
        status = _QUERY_JOB_STATUS.get(key)
        if not status:
            return None
        return deepcopy(status)


def _run_query_job(
    *,
    key: tuple[str, str],
    scheme: Dict[str, Any],
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
) -> None:
    def on_progress(payload: Dict[str, Any]) -> None:
        stage = str(payload.get("stage") or "fetching")
        stats = _default_query_stats()
        stats.update(payload.get("stats") or {})
        _update_query_job_status(
            key,
            state="running",
            stage=stage,
            message=str(payload.get("message") or "查询进行中..."),
            progress=_build_query_job_progress(stage, stats),
            stats=stats,
        )

    try:
        on_progress({"stage": "fetching", "message": "正在拉取警情...", "stats": {}})
        result = _query_records_with_scheme(
            scheme=scheme,
            start_time=start_time,
            end_time=end_time,
            branches=branches,
            page_num=page_num,
            page_size=page_size,
            progress_callback=on_progress,
        )
        final_stats = _default_query_stats()
        final_stats.update((result.get("debug") or {}))
        _update_query_job_status(
            key,
            state="success",
            stage="done",
            message=f"查询完成：命中 {result.get('total', 0)} 条",
            progress=_build_query_job_progress("done", final_stats),
            stats=final_stats,
            result=result,
        )
    except Exception as exc:
        _update_query_job_status(
            key,
            state="failed",
            message=str(exc),
            progress={"current": 0, "total": 0},
            result=None,
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
