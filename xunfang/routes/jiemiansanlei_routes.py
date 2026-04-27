"""
街面三类警情（地址分类）路由。
"""

from __future__ import annotations

from io import BytesIO
from typing import Any, List, Optional

from flask import Response, jsonify, request, send_file

from gonggong.utils.error_handler import handle_errors, log_error
from xunfang.routes.xunfang_routes import xunfang_bp
from xunfang.service.jiemiansanlei_service import export_classified, export_report, get_case_types, query_classified


def _as_list(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    return [str(val).strip()] if str(val).strip() else []


def _parse_page_size(val: Any) -> Optional[int]:
    if val is None:
        return 20
    if isinstance(val, str) and val.strip() in ("全部", "all", "ALL"):
        return None
    try:
        n = int(val)
    except Exception:
        return 20
    if n <= 0:
        return None
    return n


def _as_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _street_filter_mode(payload: dict, *, default_street: bool = False) -> str:
    mode = str(payload.get("streetFilterMode") or "").strip()
    if "streetOnly" in payload and not _as_bool(payload.get("streetOnly"), default=False):
        return "none"
    if mode == "none":
        return "none"
    if mode:
        return "recommended"
    if _as_bool(payload.get("streetOnly"), default=default_street):
        return "recommended"
    return "none"


@xunfang_bp.route("/jiemiansanlei/case_types", methods=["GET"])
@handle_errors("获取警情性质")
def jiemiansanlei_case_types() -> Response:
    return jsonify({"success": True, "data": get_case_types()})


@xunfang_bp.route("/jiemiansanlei/query", methods=["POST"])
@handle_errors("街面三类警情查询")
def jiemiansanlei_query() -> Response:
    payload = request.json or {}
    start_time = str(payload.get("startTime") or "").strip()
    end_time = str(payload.get("endTime") or "").strip()
    leixing_list = _as_list(payload.get("leixingList"))
    source_list = _as_list(payload.get("yuanshiquerenList"))
    page = int(payload.get("page") or 1)
    page_size = _parse_page_size(payload.get("pageSize"))
    street_filter_mode = _street_filter_mode(payload)
    street_only = street_filter_mode != "none"
    minor_only = _as_bool(payload.get("minorOnly"), default=False)

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400
    if not leixing_list:
        return jsonify({"success": False, "message": "请至少选择一个警情性质"}), 400
    if not source_list:
        return jsonify({"success": False, "message": "请至少选择一个 yuanshiqueren（原始/确认）"}), 400

    try:
        data = query_classified(
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            source_list=source_list,  # type: ignore[arg-type]
            page=page,
            page_size=page_size,
            street_only=street_only,
            street_filter_mode=street_filter_mode,  # type: ignore[arg-type]
            minor_only=minor_only,
        )
        return jsonify({"success": True, "data": data})
    except Exception as exc:  # noqa: BLE001
        log_error(f"街面三类警情查询失败: {exc}")
        return jsonify({"success": False, "message": f"查询失败: {exc}"}), 500


@xunfang_bp.route("/jiemiansanlei/export", methods=["POST"])
@handle_errors("街面三类警情导出")
def jiemiansanlei_export() -> Response:
    fmt = (request.args.get("format") or (request.json or {}).get("format") or "xlsx").strip().lower()
    if fmt not in ("xls", "xlsx"):
        return jsonify({"success": False, "message": "format 仅支持 xls/xlsx"}), 400

    payload = request.json or {}
    start_time = str(payload.get("startTime") or "").strip()
    end_time = str(payload.get("endTime") or "").strip()
    leixing_list = _as_list(payload.get("leixingList"))
    source_list = _as_list(payload.get("yuanshiquerenList"))
    street_filter_mode = _street_filter_mode(payload)
    street_only = street_filter_mode != "none"
    minor_only = _as_bool(payload.get("minorOnly"), default=False)

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400
    if not leixing_list:
        return jsonify({"success": False, "message": "请至少选择一个警情性质"}), 400
    if not source_list:
        return jsonify({"success": False, "message": "请至少选择一个 yuanshiqueren（原始/确认）"}), 400

    file_bytes, mimetype, filename = export_classified(
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        source_list=source_list,  # type: ignore[arg-type]
        fmt=fmt,  # type: ignore[arg-type]
        street_only=street_only,
        street_filter_mode=street_filter_mode,  # type: ignore[arg-type]
        minor_only=minor_only,
    )

    bio = BytesIO(file_bytes)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=filename, mimetype=mimetype)


@xunfang_bp.route("/jiemiansanlei/export_report", methods=["POST"])
@handle_errors("街面三类警情导出报表")
def jiemiansanlei_export_report() -> Response:
    payload = request.json or {}
    start_time = str(payload.get("startTime") or "").strip()
    end_time = str(payload.get("endTime") or "").strip()
    hb_start_time = str(payload.get("hbStartTime") or "").strip()
    hb_end_time = str(payload.get("hbEndTime") or "").strip()
    street_filter_mode = _street_filter_mode(payload, default_street=True)

    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间和结束时间不能为空"}), 400
    if not hb_start_time or not hb_end_time:
        return jsonify({"success": False, "message": "环比开始和环比结束不能为空"}), 400

    try:
        file_bytes, mimetype, filename = export_report(
            start_time=start_time,
            end_time=end_time,
            hb_start_time=hb_start_time,
            hb_end_time=hb_end_time,
            street_filter_mode=street_filter_mode,  # type: ignore[arg-type]
        )
    except Exception as exc:  # noqa: BLE001
        log_error(f"街面三类警情导出报表失败: {exc}")
        return jsonify({"success": False, "message": f"导出报表失败: {exc}"}), 500

    bio = BytesIO(file_bytes)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=filename, mimetype=mimetype)
