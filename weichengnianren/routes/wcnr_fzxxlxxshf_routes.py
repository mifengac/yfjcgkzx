from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from flask import Response, jsonify, request, send_file

from weichengnianren.routes.wcnr_routes import weichengnianren_bp
from weichengnianren.service.wcnr_fzxxlxxshf_service import (
    defaults_payload,
    export_fzxxlxxshf_records,
    query_fzxxlxxshf_records,
)


@weichengnianren_bp.route("/api/fzxxlxxshf/defaults", methods=["GET"])
def api_fzxxlxxshf_defaults() -> Response:
    try:
        return jsonify(defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@weichengnianren_bp.route("/api/fzxxlxxshf/query", methods=["POST"])
def api_fzxxlxxshf_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        start_time = (payload.get("start_time") or "").strip()
        end_time = (payload.get("end_time") or "").strip()
        branches: List[str] = payload.get("branches") or []
        page = payload.get("page", 1)
        page_size = payload.get("page_size", 20)
        return jsonify(
            query_fzxxlxxshf_records(
                start_time=start_time,
                end_time=end_time,
                branches=branches,
                page=page,
                page_size=page_size,
            )
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@weichengnianren_bp.route("/download/fzxxlxxshf")
def download_fzxxlxxshf() -> Response:
    fmt = (request.args.get("format") or "xlsx").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    branches_raw = (request.args.get("branches") or "").strip()
    branches = [x.strip() for x in branches_raw.split(",") if x.strip()] if branches_raw else []
    try:
        data, mimetype, filename = export_fzxxlxxshf_records(
            fmt=fmt,
            start_time=start_time,
            end_time=end_time,
            branches=branches,
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)
