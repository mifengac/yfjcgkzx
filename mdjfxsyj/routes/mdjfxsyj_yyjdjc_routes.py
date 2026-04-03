from __future__ import annotations

import logging

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from mdjfxsyj.service.mdjfxsyj_yyjdjc_service import (
    DEFAULT_KEYWORDS,
    SOURCE_SPECS,
    build_all_sources_export,
    build_source_export,
    get_monitor_data,
)


logger = logging.getLogger(__name__)


mdjfxsyj_yyjdjc_bp = Blueprint(
    "mdjfxsyj_yyjdjc",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@mdjfxsyj_yyjdjc_bp.before_request
def _check_access():
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "矛盾纠纷"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        logger.exception("扬言极端监测权限校验失败")
        abort(500)


@mdjfxsyj_yyjdjc_bp.get("/")
def index() -> str:
    return render_template("mdjfxsyj_yyjdjc.html", keyword_list=list(DEFAULT_KEYWORDS))


@mdjfxsyj_yyjdjc_bp.get("/api/data")
def api_data():
    try:
        payload = get_monitor_data(
            start_time=request.args.get("start_time", "").strip() or None,
            end_time=request.args.get("end_time", "").strip() or None,
        )
        return jsonify({"success": True, "data": payload})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        logger.exception("扬言极端监测查询失败")
        return jsonify({"success": False, "message": str(exc)}), 500


@mdjfxsyj_yyjdjc_bp.get("/export")
def export_all():
    return build_all_sources_export(
        start_time=request.args.get("start_time", "").strip() or None,
        end_time=request.args.get("end_time", "").strip() or None,
    )


@mdjfxsyj_yyjdjc_bp.get("/export/source")
def export_source():
    export_format = (request.args.get("format", "xlsx") or "xlsx").strip().lower()
    source = (request.args.get("source", "") or "").strip().lower()
    if export_format not in {"csv", "xlsx"}:
        abort(400, description="仅支持导出 csv 或 xlsx")
    if source not in SOURCE_SPECS:
        abort(400, description="未知数据源")
    return build_source_export(
        source=source,
        export_format=export_format,
        start_time=request.args.get("start_time", "").strip() or None,
        end_time=request.args.get("end_time", "").strip() or None,
    )
