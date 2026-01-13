from __future__ import annotations

from typing import Any, Dict

from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    render_template,
    request,
    send_file,
)
from flask import session as flask_session, redirect, url_for
from io import BytesIO

from gonggong.config.database import get_database_connection
from gonggong.utils.error_handler import handle_errors
from gzrzdd.service.gzrzdd_service import (
    compute_stats,
    export_detail,
    export_summary,
    get_detail_records,
)


gzrzdd_bp = Blueprint("gzrzdd", __name__, template_folder="../templates")


@gzrzdd_bp.before_request
def _ensure_access() -> None:
    if not flask_session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (flask_session["username"], "工作日志督导"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


@gzrzdd_bp.route("/")
def index() -> str:
    return render_template("gzrzdd.html")


@gzrzdd_bp.route("/detail")
def detail() -> str:
    return render_template("gzrzdd_detail.html")


@gzrzdd_bp.route("/api/stats", methods=["POST"])
@handle_errors("工作日志督导统计")
def api_stats() -> Response:
    payload: Dict[str, Any] = request.json or {}
    sql = (payload.get("sql") or "").strip()
    count = int(payload.get("count") or 5)
    chongfudu = payload.get("chongfudu", 80)
    if not sql:
        return jsonify({"success": False, "message": "SQL 不能为空"}), 400
    if count <= 0 or count > 100:
        return jsonify({"success": False, "message": "count 取值范围建议 1~100"}), 400

    result_id, pivot = compute_stats(sql=sql, count=count, threshold_percent=chongfudu)
    return jsonify({"success": True, "result_id": result_id, "count": count, "threshold_percent": chongfudu, "pivot": pivot})


@gzrzdd_bp.route("/api/detail")
@handle_errors("工作日志督导明细")
def api_detail() -> Response:
    result_id = (request.args.get("result_id") or "").strip()
    branch = (request.args.get("branch") or "").strip()
    station = (request.args.get("station") or "").strip()
    if not result_id or not branch or not station:
        return jsonify({"success": False, "message": "缺少参数 result_id/branch/station"}), 400
    records = get_detail_records(result_id, branch=branch, station=station)
    return jsonify({"success": True, "records": records, "count": len(records)})


@gzrzdd_bp.route("/download/summary")
def download_summary() -> Response:
    result_id = (request.args.get("result_id") or "").strip()
    fmt = (request.args.get("format") or "xlsx").strip()
    count = int(request.args.get("count") or 5)
    try:
        data, mimetype, filename = export_summary(result_id, fmt=fmt, count=count)
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)


@gzrzdd_bp.route("/download/detail")
def download_detail() -> Response:
    result_id = (request.args.get("result_id") or "").strip()
    fmt = (request.args.get("format") or "xlsx").strip()
    branch = (request.args.get("branch") or "").strip()
    station = (request.args.get("station") or "").strip()
    count = int(request.args.get("count") or 5)
    try:
        data, mimetype, filename = export_detail(result_id, branch=branch, station=station, fmt=fmt, count=count)
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    buf = BytesIO(data)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=filename, mimetype=mimetype)

