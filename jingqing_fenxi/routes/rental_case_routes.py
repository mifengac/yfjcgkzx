from __future__ import annotations

from flask import Response, jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.rental_case_service import (
    defaults_payload,
    export_rental_case_records,
    query_rental_case_records,
)


@jingqing_fenxi_bp.route('/api/rental-case/defaults', methods=['GET'])
def api_rental_case_defaults() -> Response:
    try:
        return jsonify(defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route('/api/rental-case/query', methods=['POST'])
def api_rental_case_query() -> Response:
    payload = request.json or {}
    try:
        return jsonify(
            query_rental_case_records(
                start_time=(payload.get('start_time') or '').strip(),
                end_time=(payload.get('end_time') or '').strip(),
                branches=payload.get('branches') or [],
                page_num=payload.get('page_num') or 1,
                page_size=payload.get('page_size') or 15,
            )
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route('/download/rental-case')
def download_rental_case() -> Response:
    fmt = (request.args.get('format') or 'xlsx').strip().lower()
    start_time = (request.args.get('start_time') or '').strip()
    end_time = (request.args.get('end_time') or '').strip()
    branches_raw = (request.args.get('branches') or '').strip()
    branches = [item.strip() for item in branches_raw.split(',') if item.strip()] if branches_raw else []

    try:
        export_file, mimetype, download_name = export_rental_case_records(
            export_format=fmt,
            start_time=start_time,
            end_time=end_time,
            branches=branches,
        )
        return send_file(export_file, mimetype=mimetype, as_attachment=True, download_name=download_name)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500