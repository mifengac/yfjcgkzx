from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.dao.jzqk_tongji_dao import fetch_leixing_list
from hqzcsj.service.jzqk_tongji_service import build_summary, default_time_range_for_page, fetch_detail


jzqk_tongji_bp = Blueprint("jzqk_tongji", __name__, template_folder="../templates")


@jzqk_tongji_bp.before_request
def _check_access() -> None:
    """检查用户访问权限"""
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "获取综查数据"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


def _parse_list_args(name: str) -> List[str]:
    """解析列表参数"""
    vals = request.args.getlist(name)
    out: List[str] = []
    for v in vals:
        s = (v or "").strip()
        if s:
            out.append(s)
    return out


@jzqk_tongji_bp.route("/jzqk_tongji/api/leixing")
def api_leixing() -> Any:
    """获取类型下拉框列表"""
    try:
        conn = get_database_connection()
        try:
            items = fetch_leixing_list(conn)
        finally:
            conn.close()
        return jsonify({"success": True, "data": items})
    except Exception as exc:
        logging.exception("jzqk_tongji api_leixing failed")
        return jsonify({"success": False, "message": str(exc)}), 500


@jzqk_tongji_bp.route("/jzqk_tongji/api/summary")
def api_summary() -> Any:
    """获取统计汇总数据"""
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")

    try:
        meta, rows = build_summary(start_time=start_time, end_time=end_time, leixing_list=leixing_list)
        return jsonify({"success": True, "meta": meta, "rows": rows})
    except Exception as exc:
        logging.exception(
            "jzqk_tongji api_summary failed: start_time=%s end_time=%s leixing_list=%s",
            start_time,
            end_time,
            leixing_list,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@jzqk_tongji_bp.route("/jzqk_tongji/export")
def export_summary() -> Response:
    """导出统计报表"""
    fmt = (request.args.get("fmt") or "xlsx").lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")

    meta, rows = build_summary(start_time=start_time, end_time=end_time, leixing_list=leixing_list)
    _ = meta
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"全市矫治教育统计{ts}.{fmt}"

    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@jzqk_tongji_bp.route("/jzqk_tongji/export_detail")
def export_detail() -> Response:
    """导出详情数据"""
    fmt = (request.args.get("fmt") or "xlsx").lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")

    rows, _truncated = fetch_detail(
        start_time=start_time, end_time=end_time, leixing_list=leixing_list, limit=0
    )

    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"全市矫治教育详情{ts}.{fmt}"

    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


def _download_csv(rows: List[Dict[str, Any]], filename: str) -> Response:
    """下载 CSV 文件"""
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


def _download_excel(rows: List[Dict[str, Any]], filename: str) -> Response:
    """下载 Excel 文件"""
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
