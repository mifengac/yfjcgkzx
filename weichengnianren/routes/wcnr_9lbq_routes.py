from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from weichengnianren.service.wcnr_9lbq_service import query_by_upload


wcnr_9lbq_bp = Blueprint("wcnr_9lbq", __name__, template_folder="../templates")


@wcnr_9lbq_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "未成年人"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


@wcnr_9lbq_bp.route("/wcnr_9lbq")
def index() -> str:
    return render_template("wcnr_9lbq.html")


@wcnr_9lbq_bp.route("/wcnr_9lbq/api/query", methods=["POST"])
def api_query() -> Any:
    upload = request.files.get("file")
    column_name = str(request.form.get("column_name") or "").strip()
    try:
        rows, extract_info = query_by_upload(upload, column_name)
        return jsonify(
            {
                "success": True,
                "rows": rows,
                "extract_info": extract_info,
                "total": len(rows),
            }
        )
    except Exception as exc:
        logging.exception("wcnr_9lbq api_query failed")
        return jsonify({"success": False, "message": str(exc)}), 400

