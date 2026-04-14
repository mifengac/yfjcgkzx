from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
import re
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.routes.route_helpers import parse_list_arg, user_has_module_access
from hqzcsj.service import pcsjqajtj_service


pcsjqajtj_bp = Blueprint("pcsjqajtj", __name__, template_folder="../templates")


@pcsjqajtj_bp.before_request
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


def _download_csv(rows: List[Dict[str, Any]], filename: str, *, columns: List[str] | None = None) -> Response:
    output = StringIO()
    if rows or columns:
        headers = columns or list(rows[0].keys())
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


def _download_excel(rows: List[Dict[str, Any]], filename: str, *, columns: List[str] | None = None) -> Response:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "数据"

    if rows or columns:
        headers = columns or list(rows[0].keys())
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


@pcsjqajtj_bp.route("/pcsjqajtj/api/leixing")
def api_leixing() -> Any:
    try:
        data = pcsjqajtj_service.fetch_leixing_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("pcsjqajtj api_leixing failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@pcsjqajtj_bp.route("/pcsjqajtj/api/fenju")
def api_fenju() -> Any:
    try:
        data = pcsjqajtj_service.fetch_fenju_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("pcsjqajtj api_fenju failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@pcsjqajtj_bp.route("/pcsjqajtj/api/summary")
def api_summary() -> Any:
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = pcsjqajtj_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")
    try:
        meta, rows = pcsjqajtj_service.build_summary(
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        return jsonify({"success": True, "meta": meta.__dict__, "rows": rows})
    except Exception as exc:
        logging.exception(
            "pcsjqajtj api_summary failed: start=%s end=%s leixing=%s ssfjdm=%s",
            start_time,
            end_time,
            leixing,
            ssfjdm,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@pcsjqajtj_bp.route("/pcsjqajtj/detail")
def detail_page() -> Any:
    metric = str(request.args.get("metric") or "").strip()
    pcsdm = str(request.args.get("pcsdm") or "__ALL__").strip()
    pcs_name = str(request.args.get("pcs_name") or "").strip()
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = pcsjqajtj_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    rows, truncated = pcsjqajtj_service.fetch_detail(
        metric=metric,
        pcsdm=pcsdm,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        limit=5000,
    )
    title_name = pcs_name or ("全市" if pcsdm in ("", "__ALL__", "全市") else pcsdm)
    return render_template(
        "pcsjqajtj_detail.html",
        metric=metric,
        pcsdm=pcsdm,
        pcs_name=title_name,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        rows=rows,
        truncated=truncated,
    )


@pcsjqajtj_bp.route("/pcsjqajtj/detail/export")
def detail_export() -> Response:
    fmt = str(request.args.get("fmt") or "xlsx").strip().lower()
    metric = str(request.args.get("metric") or "").strip()
    pcsdm = str(request.args.get("pcsdm") or "__ALL__").strip()
    pcs_name = str(request.args.get("pcs_name") or "").strip()
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = pcsjqajtj_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    rows, _ = pcsjqajtj_service.fetch_detail(
        metric=metric,
        pcsdm=pcsdm,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        limit=0,
    )
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    name = pcs_name or ("全市" if pcsdm in ("", "__ALL__", "全市") else pcsdm)
    filename = f"{_safe_filename_part(name)}_{_safe_filename_part(metric)}明细数据{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


def _rows_with_columns(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        out.append({col: row.get(col) for col in columns})
    return out


@pcsjqajtj_bp.route("/pcsjqajtj/export", methods=["POST"])
def export_summary() -> Response:
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    fmt = str(payload.get("fmt") or "xlsx").strip().lower()
    if fmt not in ("xlsx", "csv"):
        return jsonify({"success": False, "message": "fmt 仅支持 xlsx/csv"}), 400

    rows = payload.get("rows") or []
    columns = payload.get("columns") or []
    if not isinstance(rows, list):
        rows = []
    if not isinstance(columns, list):
        columns = []
    columns = [str(col or "").strip() for col in columns if str(col or "").strip()]

    start_time = str(payload.get("start_time") or "").strip()
    end_time = str(payload.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = pcsjqajtj_service.default_time_range_for_page()

    all_fenju_selected = bool(payload.get("all_fenju_selected"))
    selected_fenju_names = payload.get("selected_fenjv_names")
    if selected_fenju_names is None:
        selected_fenju_names = payload.get("selected_fenju_names")
    if not isinstance(selected_fenju_names, list):
        selected_fenju_names = []
    selected_fenju_names = [str(x or "").strip() for x in selected_fenju_names if str(x or "").strip()]

    if all_fenju_selected or not selected_fenju_names:
        fenju_part = "全市"
    elif len(selected_fenju_names) == 1:
        fenju_part = selected_fenju_names[0]
    else:
        fenju_part = "+".join(selected_fenju_names)

    export_rows = _rows_with_columns(rows, columns)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = (
        f"{_safe_filename_part(start_time)}至{_safe_filename_part(end_time)}_"
        f"{_safe_filename_part(fenju_part)}警情案件统计{ts}.{fmt}"
    )
    if fmt == "csv":
        return _download_csv(export_rows, filename, columns=columns or None)
    return _download_excel(export_rows, filename, columns=columns or None)
