from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from flask import Response, jsonify, request, send_file

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp
from jszahzyj.service.jsbrjqajtj_service import (
    defaults_payload,
    export_jsbrjqajtj_records,
    query_jsbrjqajtj_records,
)


@jszahzyj_bp.route("/api/jsbrjqajtj/defaults", methods=["GET"])
def api_jsbrjqajtj_defaults() -> Response:
    try:
        return jsonify(defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/api/jsbrjqajtj/query", methods=["POST"])
def api_jsbrjqajtj_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        start_time = (payload.get("start_time") or "").strip()
        end_time = (payload.get("end_time") or "").strip()
        branches: List[str] = payload.get("branches") or []
        return jsonify(
            query_jsbrjqajtj_records(
                start_time=start_time,
                end_time=end_time,
                branches=branches,
            )
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/download/jsbrjqajtj")
def download_jsbrjqajtj() -> Response:
    fmt = (request.args.get("format") or "xlsx").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    branches_raw = (request.args.get("branches") or "").strip()
    branches = [x.strip() for x in branches_raw.split(",") if x.strip()] if branches_raw else []
    try:
        data, mimetype, filename = export_jsbrjqajtj_records(
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

