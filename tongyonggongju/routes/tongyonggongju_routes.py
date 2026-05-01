from __future__ import annotations

import logging

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from tongyonggongju.service.background_check_service import (
    inspect_and_store_workbook,
    run_background_check,
)


logger = logging.getLogger(__name__)


tongyonggongju_bp = Blueprint(
    "tongyonggongju",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@tongyonggongju_bp.before_request
def _check_access():
    if not session.get("username"):
        return redirect(url_for("login"))

    conn = None
    row = None
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "通用工具"),
            )
            row = cur.fetchone()
    except Exception:
        logger.exception("通用工具权限检查失败")
        abort(500)
    finally:
        if conn:
            conn.close()

    if not row:
        abort(403)


@tongyonggongju_bp.get("/")
def index() -> str:
    return render_template("tongyonggongju.html")


@tongyonggongju_bp.post("/api/background/upload")
def api_background_upload():
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"success": False, "message": "请先选择 xlsx 文件"}), 400

    try:
        payload = inspect_and_store_workbook(file.stream.read(), file.filename)
        session["tygj_background_token"] = payload["token"]
        return jsonify({"success": True, "data": payload})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logger.exception("背景审查文件上传失败")
        return jsonify({"success": False, "message": f"文件上传失败: {exc}"}), 500


@tongyonggongju_bp.post("/api/background/check")
def api_background_check():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token") or "").strip()
    if not token or token != session.get("tygj_background_token"):
        return jsonify({"success": False, "message": "上传文件已失效，请重新上传"}), 400
    if not payload.get("name_column_index"):
        return jsonify({"success": False, "message": "请选择姓名所在列"}), 400

    try:
        data = run_background_check(token, payload.get("id_column_index"), payload.get("name_column_index"))
        return jsonify({"success": True, "data": data})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logger.exception("背景审查失败")
        return jsonify({"success": False, "message": f"审查失败: {exc}"}), 500
