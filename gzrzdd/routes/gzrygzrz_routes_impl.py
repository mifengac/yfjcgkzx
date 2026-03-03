from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from flask import Response, jsonify, request, send_file

from gonggong.utils.error_handler import handle_errors
from gzrzdd.routes.gzrzdd_routes import gzrzdd_bp
from gzrzdd.service.gzrygzrz_service import (
    default_time_range,
    export_gzrygzrz_records,
    query_gzrygzrz_records,
)


@gzrzdd_bp.route("/api/gzrygzrz/defaults", methods=["GET"])
@handle_errors("关注人员工作日志默认筛选")
def api_gzrygzrz_defaults() -> Response:
    start_time, end_time = default_time_range()
    return jsonify(
        {
            "success": True,
            "start_time": start_time,
            "end_time": end_time,
            "sfczjjzx": "",
            "branches": [],
        }
    )


@gzrzdd_bp.route("/api/gzrygzrz/query", methods=["POST"])
@handle_errors("关注人员工作日志查询")
def api_gzrygzrz_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    start_time = (payload.get("start_time") or "").strip()
    end_time = (payload.get("end_time") or "").strip()
    sfczjjzx = (payload.get("sfczjjzx") or "").strip()
    branches: List[str] = payload.get("branches") or []

    result = query_gzrygzrz_records(
        start_time=start_time,
        end_time=end_time,
        sfczjjzx=sfczjjzx,
        branches=branches,
    )
    return jsonify(result)


@gzrzdd_bp.route("/download/gzrygzrz")
@handle_errors("关注人员工作日志导出")
def download_gzrygzrz() -> Response:
    fmt = (request.args.get("format") or "xlsx").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    sfczjjzx = (request.args.get("sfczjjzx") or "").strip()
    branches_raw = (request.args.get("branches") or "").strip()
    branches = [x.strip() for x in branches_raw.split(",") if x.strip()] if branches_raw else []

    data, mimetype, filename = export_gzrygzrz_records(
        fmt=fmt,
        start_time=start_time,
        end_time=end_time,
        sfczjjzx=sfczjjzx,
        branches=branches,
    )
    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)
