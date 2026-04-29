from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
import re
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.routes.route_helpers import parse_list_arg, user_has_module_access
from hqzcsj.service import tqzmjy_tj_service


tqzmjy_tj_bp = Blueprint("tqzmjy_tj", __name__, template_folder="../templates")


@tqzmjy_tj_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        if not user_has_module_access(get_database_connection, username=session["username"]):
            abort(403)
    except Exception:
        abort(500)


def _parse_list_args(name: str) -> List[str]:
    return parse_list_arg(name)


def _safe_filename_part(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r'[\\/:*?"<>|\s]+', "-", text)


def _download_csv(rows: List[Dict[str, Any]], filename: str, *, columns: List[str]) -> Response:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: (row.get(key) if row.get(key) is not None else "") for key in columns})

    buffer = BytesIO(output.getvalue().encode("utf-8-sig"))
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8",
    )


def _download_excel(rows: List[Dict[str, Any]], filename: str, *, columns: List[str]) -> Response:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "数据"
    sheet.append(columns)
    for row in rows:
        sheet.append([(row.get(key) if row.get(key) is not None else "") for key in columns])

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@tqzmjy_tj_bp.route("/tqzmjy_tj/api/leixing")
def api_leixing() -> Any:
    try:
        data = tqzmjy_tj_service.fetch_leixing_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("tqzmjy_tj api_leixing failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@tqzmjy_tj_bp.route("/tqzmjy_tj/api/fenju")
def api_fenju() -> Any:
    try:
        data = tqzmjy_tj_service.fetch_fenju_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("tqzmjy_tj api_fenju failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@tqzmjy_tj_bp.route("/tqzmjy_tj/api/query")
def api_query() -> Any:
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = tqzmjy_tj_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")
    try:
        meta, rows = tqzmjy_tj_service.query_rows(
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        return jsonify(
            {
                "success": True,
                "meta": meta,
                "columns": tqzmjy_tj_service.DISPLAY_COLUMNS,
                "rows": rows,
            }
        )
    except Exception as exc:
        logging.exception(
            "tqzmjy_tj api_query failed: start=%s end=%s leixing=%s ssfjdm=%s",
            start_time,
            end_time,
            leixing,
            ssfjdm,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@tqzmjy_tj_bp.route("/tqzmjy_tj/export")
def export_rows() -> Response:
    fmt = str(request.args.get("fmt") or "xlsx").strip().lower()
    if fmt not in ("xlsx", "csv"):
        return jsonify({"success": False, "message": "fmt 仅支持 xlsx/csv"}), 400

    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = tqzmjy_tj_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")

    meta, rows = tqzmjy_tj_service.query_rows(
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        ssfjdm_list=ssfjdm,
    )
    approval_part = meta["start_time"]
    if meta["end_time"] != meta["start_time"]:
        approval_part = f"{meta['start_time']}至{meta['end_time']}"
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{_safe_filename_part(approval_part)}提请专门教育申请书{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename, columns=tqzmjy_tj_service.DISPLAY_COLUMNS)
    return _download_excel(rows, filename, columns=tqzmjy_tj_service.DISPLAY_COLUMNS)