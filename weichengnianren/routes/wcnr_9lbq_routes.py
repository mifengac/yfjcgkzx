from __future__ import annotations

import csv
import logging
from datetime import datetime
from io import BytesIO
from io import StringIO
from typing import Any

from flask import Blueprint, abort, jsonify, redirect, request, send_file, session, url_for
from openpyxl import Workbook

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
    return redirect(url_for("weichengnianren.index", tab="wcnr9lbq"))


@wcnr_9lbq_bp.route("/wcnr_9lbq/template")
def download_template() -> Any:
    # UTF-8 BOM + 单列表头，便于 Excel 直接打开并编辑
    content = "证件号码\r\n"
    buffer = BytesIO(content.encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name="未成年人9类标签碰撞模板.csv",
        mimetype="text/csv; charset=utf-8",
    )


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


def _download_csv(rows, filename: str):
    output = StringIO()
    if rows:
        headers = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (row.get(k) if row.get(k) is not None else "") for k in headers})
    else:
        output.write("无数据\n")

    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


def _download_excel(rows, filename: str):
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "数据"
    if rows:
        headers = list(rows[0].keys())
        sheet.append(headers)
        for row in rows:
            sheet.append([(row.get(k) if row.get(k) is not None else "") for k in headers])
    else:
        sheet.append(["无数据"])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@wcnr_9lbq_bp.route("/wcnr_9lbq/api/export", methods=["POST"])
def export_query_result() -> Any:
    upload = request.files.get("file")
    column_name = str(request.form.get("column_name") or "").strip()
    fmt = str(request.form.get("fmt") or "xlsx").strip().lower()
    if fmt not in ("xlsx", "csv"):
        return jsonify({"success": False, "message": "不支持的导出格式"}), 400

    try:
        rows, _extract_info = query_by_upload(upload, column_name)
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"未成年人九类标签{ts}.{fmt}"
        if fmt == "csv":
            return _download_csv(rows, filename)
        return _download_excel(rows, filename)
    except Exception as exc:
        logging.exception("wcnr_9lbq export failed")
        return jsonify({"success": False, "message": str(exc)}), 400
