from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
import re
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook
from werkzeug.exceptions import HTTPException

from gonggong.config.database import get_database_connection
from jingqing_anjian_fenxi.service import jingqing_anjian_fenxi_service


jingqing_anjian_fenxi_bp = Blueprint(
    "jingqing_anjian_fenxi",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@jingqing_anjian_fenxi_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "警情案件分析"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except HTTPException:
        raise
    except Exception:
        abort(500)


def _parse_list_args(name: str) -> List[str]:
    values = request.args.getlist(name)
    out: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _safe_filename_part(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r'[\\/:*?"<>|\s]+', "-", text)


def _rows_with_columns(rows: List[Dict[str, Any]], columns: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        out.append({col: row.get(col) for col in columns})
    return out


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


@jingqing_anjian_fenxi_bp.route("/")
def index() -> Any:
    start_time, end_time = jingqing_anjian_fenxi_service.default_time_range_for_page()
    return render_template(
        "jingqing_anjian_fenxi_index.html",
        start_time=start_time,
        end_time=end_time,
    )


@jingqing_anjian_fenxi_bp.route("/api/fenju")
def api_fenju() -> Any:
    try:
        data = jingqing_anjian_fenxi_service.fetch_fenju_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("jingqing_anjian_fenxi api_fenju failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_anjian_fenxi_bp.route("/api/leixing")
def api_leixing() -> Any:
    try:
        data = jingqing_anjian_fenxi_service.fetch_leixing_options()
        return jsonify({"success": True, "data": data})
    except Exception as exc:
        logging.exception("jingqing_anjian_fenxi api_leixing failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_anjian_fenxi_bp.route("/api/summary")
def api_summary() -> Any:
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = jingqing_anjian_fenxi_service.default_time_range_for_page()
    group_mode = str(request.args.get("group_mode") or "").strip()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")
    try:
        meta, rows = jingqing_anjian_fenxi_service.build_summary(
            start_time=start_time,
            end_time=end_time,
            group_mode=group_mode,
            leixing_list=leixing,
            ssfjdm_list=ssfjdm,
        )
        return jsonify({"success": True, "meta": meta.__dict__, "rows": rows})
    except Exception as exc:
        logging.exception(
            "jingqing_anjian_fenxi api_summary failed: start=%s end=%s group_mode=%s leixing=%s ssfjdm=%s",
            start_time,
            end_time,
            group_mode,
            leixing,
            ssfjdm,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@jingqing_anjian_fenxi_bp.route("/detail")
def detail_page() -> Any:
    metric = str(request.args.get("metric") or "").strip()
    group_code = str(request.args.get("group_code") or "__ALL__").strip()
    group_name = str(request.args.get("group_name") or "").strip()
    group_mode = str(request.args.get("group_mode") or "").strip()
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = jingqing_anjian_fenxi_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")
    rows, truncated = jingqing_anjian_fenxi_service.fetch_detail(
        metric=metric,
        group_code=group_code,
        group_mode=group_mode,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        ssfjdm_list=ssfjdm,
        limit=5000,
    )
    title_name = group_name or ("全市" if group_code in ("", "__ALL__", "全市") else group_code)
    return render_template(
        "jingqing_anjian_fenxi_detail.html",
        metric=metric,
        metric_label=jingqing_anjian_fenxi_service.resolve_metric_label(metric),
        group_code=group_code,
        group_name=title_name,
        group_mode=jingqing_anjian_fenxi_service.normalize_group_mode(group_mode),
        group_mode_label=jingqing_anjian_fenxi_service.get_group_mode_label(group_mode),
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        ssfjdm_list=ssfjdm,
        rows=rows,
        truncated=truncated,
    )


@jingqing_anjian_fenxi_bp.route("/detail/export")
def detail_export() -> Response:
    fmt = str(request.args.get("fmt") or "xlsx").strip().lower()
    metric = str(request.args.get("metric") or "").strip()
    group_code = str(request.args.get("group_code") or "__ALL__").strip()
    group_name = str(request.args.get("group_name") or "").strip()
    group_mode = str(request.args.get("group_mode") or "").strip()
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = jingqing_anjian_fenxi_service.default_time_range_for_page()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")
    rows, _ = jingqing_anjian_fenxi_service.fetch_detail(
        metric=metric,
        group_code=group_code,
        group_mode=group_mode,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing,
        ssfjdm_list=ssfjdm,
        limit=0,
    )
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    name = group_name or ("全市" if group_code in ("", "__ALL__", "全市") else group_code)
    metric_label = jingqing_anjian_fenxi_service.resolve_metric_label(metric)
    filename = f"{_safe_filename_part(name)}_{_safe_filename_part(metric_label)}_明细_{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@jingqing_anjian_fenxi_bp.route("/export")
def export_summary() -> Response:
    fmt = str(request.args.get("fmt") or "xlsx").strip().lower()
    if fmt not in ("xlsx", "csv"):
        return jsonify({"success": False, "message": "fmt 仅支持 xlsx/csv"}), 400

    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = jingqing_anjian_fenxi_service.default_time_range_for_page()
    group_mode = str(request.args.get("group_mode") or "").strip()
    leixing = _parse_list_args("leixing")
    ssfjdm = _parse_list_args("ssfjdm")

    meta, rows = jingqing_anjian_fenxi_service.build_summary(
        start_time=start_time,
        end_time=end_time,
        group_mode=group_mode,
        leixing_list=leixing,
        ssfjdm_list=ssfjdm,
    )
    export_rows = _rows_with_columns(rows, jingqing_anjian_fenxi_service.SUMMARY_COLUMNS)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = (
        f"{_safe_filename_part(meta.start_time)}至{_safe_filename_part(meta.end_time)}_"
        f"警情案件分析_{_safe_filename_part(meta.group_mode_label)}_{ts}.{fmt}"
    )
    if fmt == "csv":
        return _download_csv(export_rows, filename, columns=jingqing_anjian_fenxi_service.SUMMARY_COLUMNS)
    return _download_excel(export_rows, filename, columns=jingqing_anjian_fenxi_service.SUMMARY_COLUMNS)
