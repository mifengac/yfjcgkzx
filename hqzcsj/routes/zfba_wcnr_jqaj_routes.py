from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.dao.zfba_wcnr_jqaj_dao import fetch_leixing_list
from hqzcsj.service.zfba_wcnr_report_service import build_report_file
from hqzcsj.service.zfba_wcnr_jqaj_service import (
    REGION_ORDER,
    append_ratio_columns,
    build_summary,
    default_time_range_for_page,
    fetch_detail,
)


zfba_wcnr_jqaj_bp = Blueprint("zfba_wcnr_jqaj", __name__, template_folder="../templates")


@zfba_wcnr_jqaj_bp.before_request
def _check_access() -> None:
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
    vals = request.args.getlist(name)
    out: List[str] = []
    for v in vals:
        s = (v or "").strip()
        if s:
            out.append(s)
    return out


def _parse_bool_arg(name: str) -> bool:
    v = (request.args.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/api/leixing")
def api_leixing() -> Any:
    try:
        conn = get_database_connection()
        try:
            items = fetch_leixing_list(conn)
        finally:
            conn.close()
        return jsonify({"success": True, "data": items})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/api/summary")
def api_summary() -> Any:
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    hb_start_time = (request.args.get("hb_start_time") or "").strip()
    hb_end_time = (request.args.get("hb_end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")
    show_ratio = _parse_bool_arg("show_ratio")
    show_hb = _parse_bool_arg("show_hb")
    try:
        meta, rows = build_summary(
            start_time=start_time,
            end_time=end_time,
            hb_start_time=hb_start_time or None,
            hb_end_time=hb_end_time or None,
            leixing_list=leixing_list,
            za_types=za_types,
        )
        if not show_hb:
            rows = [{k: v for k, v in row.items() if not str(k).startswith("环比")} for row in rows]
        if show_ratio:
            rows = append_ratio_columns(rows)
        return jsonify({"success": True, "meta": meta.__dict__, "rows": rows})
    except Exception as exc:
        logging.exception(
            "zfba_wcnr_jqaj api_summary failed: start_time=%s end_time=%s hb_start_time=%s hb_end_time=%s leixing_list=%s za_types=%s",
            start_time,
            end_time,
            hb_start_time,
            hb_end_time,
            leixing_list,
            za_types,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/export")
def export_summary() -> Response:
    fmt = (request.args.get("fmt") or "xlsx").lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    hb_start_time = (request.args.get("hb_start_time") or "").strip()
    hb_end_time = (request.args.get("hb_end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")
    show_ratio = _parse_bool_arg("show_ratio")
    show_hb = _parse_bool_arg("show_hb")

    meta, rows = build_summary(
        start_time=start_time,
        end_time=end_time,
        hb_start_time=hb_start_time or None,
        hb_end_time=hb_end_time or None,
        leixing_list=leixing_list,
        za_types=za_types,
    )
    if not show_hb:
        rows = [{k: v for k, v in row.items() if not str(k).startswith("环比")} for row in rows]
    if show_ratio:
        rows = append_ratio_columns(rows)
    _ = meta
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"未成年人警情案件统计{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/report_export")
def report_export() -> Response:
    fmt = (request.args.get("fmt") or "xlsx").lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")

    try:
        data, filename, mimetype = build_report_file(
            fmt=fmt,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
        )
        buffer = BytesIO(data)
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype=mimetype,
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except FileNotFoundError as exc:
        logging.error("zfba_wcnr_jqaj report_export template missing: %s", exc)
        return jsonify({"success": False, "message": str(exc)}), 500
    except Exception as exc:
        logging.exception(
            "zfba_wcnr_jqaj report_export failed: start_time=%s end_time=%s leixing_list=%s fmt=%s",
            start_time,
            end_time,
            leixing_list,
            fmt,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/detail")
def detail_page() -> Any:
    metric = (request.args.get("metric") or "").strip()
    diqu = (request.args.get("diqu") or "__ALL__").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")

    region_name = "全市"
    if diqu and diqu not in ("__ALL__", "全市"):
        region_name = next((name for code, name in REGION_ORDER if code == diqu), diqu)

    rows, truncated = fetch_detail(
        metric=metric,
        diqu=diqu,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        za_types=za_types,
        limit=5000,
    )
    return render_template(
        "zfba_wcnr_jqaj_detail.html",
        metric=metric,
        diqu=diqu,
        region_name=region_name,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        za_types=za_types,
        rows=rows,
        truncated=truncated,
    )


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/detail/export")
def export_detail() -> Response:
    fmt = (request.args.get("fmt") or "xlsx").lower()
    metric = (request.args.get("metric") or "").strip()
    diqu = (request.args.get("diqu") or "__ALL__").strip()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")

    region_name = "全市"
    if diqu and diqu not in ("__ALL__", "全市"):
        region_name = next((name for code, name in REGION_ORDER if code == diqu), diqu)

    rows, _truncated = fetch_detail(
        metric=metric,
        diqu=diqu,
        start_time=start_time,
        end_time=end_time,
        leixing_list=leixing_list,
        za_types=za_types,
        limit=0,
    )
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{region_name}未成年人详细数据{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/detail/export_all")
def export_detail_all() -> Response:
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")

    metrics = [
        "警情",
        "转案数",
        "案件数(被侵害)",
        "场所案件(被侵害)",
        "行政",
        "刑事",
        "场所案件",
        "治安处罚",
        "治安处罚(不执行)",
        "刑拘",
        "矫治文书(行政)",
        "矫治文书(刑事)",
        "加强监督教育(行政)",
        "加强监督教育(刑事)",
        "符合送校",
        "送校",
    ]
    wb = Workbook()
    ws0 = wb.active
    ws0.title = metrics[0][:31] if metrics else "数据"

    for i, metric in enumerate(metrics):
        rows, _truncated = fetch_detail(
            metric=metric,
            diqu="__ALL__",
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=za_types,
            limit=0,
        )
        ws = ws0 if i == 0 else wb.create_sheet(title=metric[:31])
        if rows:
            headers = list(rows[0].keys())
            ws.append(headers)
            for r in rows:
                ws.append([(r.get(k) if r.get(k) is not None else "") for k in headers])
        else:
            ws.append(["无数据"])

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"未成年人警情案件统计详细{ts}.xlsx"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _download_csv(rows: List[Dict[str, Any]], filename: str) -> Response:
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
