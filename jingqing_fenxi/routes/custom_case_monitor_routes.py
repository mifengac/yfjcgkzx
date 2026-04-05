from __future__ import annotations

from flask import Response, jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.custom_case_monitor_service import (
    build_defaults_payload,
    create_scheme,
    delete_scheme,
    export_custom_case_monitor_records,
    list_scheme_payload,
    query_custom_case_monitor_records,
    update_scheme,
)


@jingqing_fenxi_bp.route("/api/custom-case-monitor/defaults", methods=["GET"])
def api_custom_case_monitor_defaults() -> Response:
    try:
        return jsonify(build_defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/api/custom-case-monitor/schemes", methods=["GET"])
def api_custom_case_monitor_scheme_list() -> Response:
    try:
        return jsonify(list_scheme_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/api/custom-case-monitor/schemes", methods=["POST"])
def api_custom_case_monitor_scheme_create() -> Response:
    payload = request.get_json(silent=True) or {}
    try:
        return jsonify(create_scheme(payload))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/api/custom-case-monitor/schemes/<int:scheme_id>", methods=["PUT"])
def api_custom_case_monitor_scheme_update(scheme_id: int) -> Response:
    payload = request.get_json(silent=True) or {}
    try:
        return jsonify(update_scheme(scheme_id, payload))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/api/custom-case-monitor/schemes/<int:scheme_id>", methods=["DELETE"])
def api_custom_case_monitor_scheme_delete(scheme_id: int) -> Response:
    try:
        return jsonify(delete_scheme(scheme_id))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/api/custom-case-monitor/query", methods=["POST"])
def api_custom_case_monitor_query() -> Response:
    payload = request.get_json(silent=True) or {}
    try:
        return jsonify(
            query_custom_case_monitor_records(
                scheme_id=payload.get("scheme_id"),
                start_time=(payload.get("start_time") or "").strip(),
                end_time=(payload.get("end_time") or "").strip(),
                branches=payload.get("branches") or [],
                page_num=payload.get("page_num") or 1,
                page_size=payload.get("page_size") or 15,
            )
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/custom-case-monitor", methods=["GET"])
def download_custom_case_monitor() -> Response:
    export_format = (request.args.get("format") or "xlsx").strip().lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    scheme_id = request.args.get("scheme_id")
    branches_raw = (request.args.get("branches") or "").strip()
    branches = [item.strip() for item in branches_raw.split(",") if item.strip()] if branches_raw else []

    try:
        export_file, mimetype, download_name = export_custom_case_monitor_records(
            export_format=export_format,
            scheme_id=scheme_id,
            start_time=start_time,
            end_time=end_time,
            branches=branches,
        )
        return send_file(export_file, mimetype=mimetype, as_attachment=True, download_name=download_name)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500
