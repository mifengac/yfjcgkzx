from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.dao.zfba_jq_aj_dao import fetch_leixing_list
from hqzcsj.service.zfba_jq_aj_report_service import ZfbaJqAjReportService
from hqzcsj.service.zfba_jq_aj_service import REGION_ORDER, build_summary, default_time_range_for_page, fetch_detail


zfba_jq_aj_bp = Blueprint("zfba_jq_aj", __name__, template_folder="../templates")


@zfba_jq_aj_bp.before_request
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


@zfba_jq_aj_bp.route("/zfba_jq_aj/api/leixing")
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


def _parse_list_args(name: str) -> List[str]:
    vals = request.args.getlist(name)
    out: List[str] = []
    for v in vals:
        s = (v or "").strip()
        if s:
            out.append(s)
    return out


@zfba_jq_aj_bp.route("/zfba_jq_aj/api/summary")
def api_summary() -> Any:
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")
    try:
        meta, rows = build_summary(start_time=start_time, end_time=end_time, leixing_list=leixing_list, za_types=za_types)
        return jsonify({"success": True, "meta": meta.__dict__, "rows": rows})
    except Exception as exc:
        logging.exception(
            "zfba_jq_aj api_summary failed: start_time=%s end_time=%s leixing_list=%s za_types=%s",
            start_time,
            end_time,
            leixing_list,
            za_types,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@zfba_jq_aj_bp.route("/zfba_jq_aj/export")
def export_summary() -> Response:
    fmt = (request.args.get("fmt") or "xlsx").lower()
    start_time = (request.args.get("start_time") or "").strip()
    end_time = (request.args.get("end_time") or "").strip()
    if not start_time or not end_time:
        start_time, end_time = default_time_range_for_page()
    leixing_list = _parse_list_args("leixing")
    za_types = _parse_list_args("za_type")

    meta, rows = build_summary(start_time=start_time, end_time=end_time, leixing_list=leixing_list, za_types=za_types)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"警情案件统计{ts}.{fmt}"

    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@zfba_jq_aj_bp.route("/zfba_jq_aj/detail")
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
        "zfba_jq_aj_detail.html",
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


@zfba_jq_aj_bp.route("/zfba_jq_aj/detail/export")
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
    filename = f"{region_name}警情案件详细数据{ts}.{fmt}"
    if fmt == "csv":
        return _download_csv(rows, filename)
    return _download_excel(rows, filename)


@zfba_jq_aj_bp.route("/zfba_jq_aj/report_export", methods=["POST"])
def report_export() -> Response:
    """导出报表（写入 xls 模板；固定类型，不受多选框影响）"""
    try:
        params = request.get_json() or {}
        kssj = (params.get("kssj") or "").strip()
        jssj = (params.get("jssj") or "").strip()
        hbkssj = (params.get("hbkssj") or "").strip()
        hbjssj = (params.get("hbjssj") or "").strip()
        za_types = params.get("za_types") or []
        if not isinstance(za_types, list):
            za_types = []
        za_types = [str(x).strip() for x in za_types if str(x).strip()]

        if not kssj or not jssj or not hbkssj or not hbjssj:
            return jsonify({"success": False, "message": "缺少参数：kssj/jssj/hbkssj/hbjssj"}), 400

        service = ZfbaJqAjReportService()
        data = service.build_report_xls(kssj, jssj, hbkssj, hbjssj, za_types=za_types)
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"警情案件处罚统计报表_{ts}.xls"

        buffer = BytesIO(data)
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.ms-excel",
        )
    except ValueError as exc:
        logging.error("导出报表参数错误: %s", exc)
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logging.error("导出报表失败: %s", exc)
        return jsonify({"success": False, "message": f"导出报表失败: {exc}"}), 500


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

