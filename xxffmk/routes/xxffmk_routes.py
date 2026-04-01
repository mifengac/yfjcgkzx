from __future__ import annotations

from typing import Any, Iterable, List

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException

from gonggong.config.database import get_database_connection
from xxffmk.service import xxffmk_service


MODULE_NAME = "学校赋分模块"

xxffmk_bp = Blueprint(
    "xxffmk",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


def _parse_multi_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_values = value
    else:
        raw_values = str(value).split(",")
    return [str(item or "").strip() for item in raw_values if str(item or "").strip()]


def _get_request_payload() -> dict:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return payload
    return request.form.to_dict(flat=False) if request.method == "POST" else request.args.to_dict(flat=False)


def _first_value(payload: dict, key: str, default: str = "") -> str:
    value = payload.get(key, default)
    if isinstance(value, list):
        return str(value[0] if value else default).strip()
    return str(value or default).strip()


@xxffmk_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], MODULE_NAME),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except HTTPException:
        raise
    except Exception:
        abort(500)


@xxffmk_bp.route("/", methods=["GET"])
def index() -> Any:
    start_time, end_time = xxffmk_service.default_time_range_for_page()
    return render_template(
        "xxffmk_index.html",
        start_time=start_time,
        end_time=end_time,
        module_name=MODULE_NAME,
    )


@xxffmk_bp.route("/api/rank", methods=["POST"])
def api_rank() -> Any:
    payload = _get_request_payload()
    begin_date = _first_value(payload, "beginDate")
    end_date = _first_value(payload, "endDate")
    if not begin_date or not end_date:
        begin_date, end_date = xxffmk_service.default_time_range_for_page()
    limit = int(_first_value(payload, "limit", "10") or 10)
    school_codes = _parse_multi_value(payload.get("school_codes"))
    school_name = _first_value(payload, "school_name")
    try:
        data = xxffmk_service.build_rank_payload(
            start_time=begin_date,
            end_time=end_date,
            limit=limit,
            school_codes=school_codes,
            school_name=school_name,
        )
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400


@xxffmk_bp.route("/api/school_detail", methods=["GET"])
def api_school_detail() -> Any:
    xxbsm = str(request.args.get("xxbsm") or "").strip()
    begin_date = str(request.args.get("beginDate") or "").strip()
    end_date = str(request.args.get("endDate") or "").strip()
    if not begin_date or not end_date:
        begin_date, end_date = xxffmk_service.default_time_range_for_page()
    try:
        data = xxffmk_service.get_school_detail(
            xxbsm=xxbsm,
            start_time=begin_date,
            end_time=end_date,
        )
        return jsonify({"success": True, "data": data})
    except LookupError as exc:
        return jsonify({"success": False, "message": str(exc)}), 404
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400


@xxffmk_bp.route("/api/dimension_detail", methods=["GET"])
def api_dimension_detail() -> Any:
    dimension = str(request.args.get("dimension") or "").strip()
    xxbsm = str(request.args.get("xxbsm") or "").strip()
    begin_date = str(request.args.get("beginDate") or "").strip()
    end_date = str(request.args.get("endDate") or "").strip()
    if not begin_date or not end_date:
        begin_date, end_date = xxffmk_service.default_time_range_for_page()
    page = int(str(request.args.get("page") or "1").strip() or 1)
    page_size = int(str(request.args.get("page_size") or "20").strip() or 20)
    try:
        data = xxffmk_service.get_dimension_detail(
            dimension=dimension,
            xxbsm=xxbsm,
            start_time=begin_date,
            end_time=end_date,
            page=page,
            page_size=page_size,
        )
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400


@xxffmk_bp.route("/api/refresh", methods=["POST"])
def api_refresh() -> Any:
    try:
        data = xxffmk_service.refresh_materialized_views()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
