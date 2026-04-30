from __future__ import annotations

from typing import Any, Dict, List

from flask import Response, jsonify, request, send_file
from io import BytesIO

from gonggong.utils.error_handler import handle_errors
from gzrzdd.routes.gzrzdd_routes import gzrzdd_bp
from gzrzdd.service.gzrzdd_cqtj_service import export_cqtj, query_cqtj


@gzrzdd_bp.route("/api/cqtj/query", methods=["POST"])
@handle_errors("矛盾纠纷风险人员工作日志超期统计查询")
def api_cqtj_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    mode = (payload.get("mode") or "detail").strip()
    level = (payload.get("level") or "remind").strip()
    risk_types: List[str] = payload.get("risk_types") or []
    branches: List[str] = payload.get("branches") or []
    start_time = str(payload.get("start_time") or "").strip()
    end_time = str(payload.get("end_time") or "").strip()

    now, records = query_cqtj(
        mode=mode,
        level=level,
        risk_types=risk_types,
        branches=branches,
        start_time=start_time,
        end_time=end_time,
    )
    return jsonify(
        {
            "success": True,
            "mode": mode,
            "level": level,
            "risk_types": risk_types,
            "branches": branches,
            "start_time": start_time,
            "end_time": end_time,
            "now": now.strftime("%Y-%m-%d %H:%M:%S"),
            "records": records,
            "count": len(records),
        }
    )


@gzrzdd_bp.route("/download/cqtj")
def download_cqtj() -> Response:
    fmt = (request.args.get("format") or "xlsx").strip()
    mode = (request.args.get("mode") or "detail").strip()
    level = (request.args.get("level") or "remind").strip()
    risk_types_raw = (request.args.get("risk_types") or "").strip()
    risk_types = [x.strip() for x in risk_types_raw.split(",") if x.strip()] if risk_types_raw else []
    branches_raw = (request.args.get("branches") or "").strip()
    branches = [x.strip() for x in branches_raw.split(",") if x.strip()] if branches_raw else []
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    try:
        data, mimetype, filename = export_cqtj(
            fmt=fmt,
            mode=mode,
            level=level,
            risk_types=risk_types,
            branches=branches,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)
