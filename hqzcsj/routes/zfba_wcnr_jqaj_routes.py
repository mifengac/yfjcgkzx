from __future__ import annotations

from datetime import datetime
from io import BytesIO
import logging
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, request, send_file, session, url_for
from openpyxl import Workbook
from werkzeug.exceptions import HTTPException

from gonggong.config.database import get_database_connection
from gonggong.utils.filtered_summary_detail_controller import FilteredSummaryDetailController
from hqzcsj.routes.route_helpers import parse_list_arg, user_has_module_access
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
zfba_wcnr_jqaj_controller = FilteredSummaryDetailController(
    list_arg_map={"leixing": "leixing_list", "za_type": "za_types"},
    bool_arg_map={"show_ratio": "show_ratio", "show_hb": "show_hb"},
    format_param_name="fmt",
    default_region_code="__ALL__",
    sheet_title="\u6570\u636e",
)


def _resolve_region_name(diqu: str) -> str:
    if diqu and diqu not in ("__ALL__", "\u5168\u5e02"):
        return next((name for code, name in REGION_ORDER if code == diqu), diqu)
    return "\u5168\u5e02"


def _build_summary_payload(filters: Dict[str, Any]) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    meta, rows = build_summary(
        start_time=filters["start_time"],
        end_time=filters["end_time"],
        hb_start_time=filters["hb_start_time"] or None,
        hb_end_time=filters["hb_end_time"] or None,
        leixing_list=filters["leixing_list"],
        za_types=filters["za_types"],
    )
    if not filters["show_hb"]:
        rows = [{k: v for k, v in row.items() if not str(k).startswith("\u73af\u6bd4")} for row in rows]
    if filters["show_ratio"]:
        rows = append_ratio_columns(rows)
    return meta.__dict__, rows


def _load_detail_rows(
    filters: Dict[str, Any],
    metric: str,
    diqu: str,
    limit: int,
) -> tuple[List[Dict[str, Any]], bool]:
    return fetch_detail(
        metric=metric,
        diqu=diqu,
        start_time=filters["start_time"],
        end_time=filters["end_time"],
        leixing_list=filters["leixing_list"],
        za_types=filters["za_types"],
        limit=limit,
    )


def _build_summary_filename(_filters: Dict[str, Any], export_format: str, timestamp: str) -> str:
    return f"\u672a\u6210\u5e74\u4eba\u8b66\u60c5\u6848\u4ef6\u7edf\u8ba1{timestamp}.{export_format}"


def _build_detail_filename(
    _filters: Dict[str, Any],
    region_name: str,
    export_format: str,
    timestamp: str,
) -> str:
    return f"{region_name}\u672a\u6210\u5e74\u4eba\u8be6\u7ec6\u6570\u636e{timestamp}.{export_format}"


@zfba_wcnr_jqaj_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        if not user_has_module_access(get_database_connection, username=session["username"]):
            abort(403)
    except HTTPException:
        raise
    except Exception:
        abort(500)


def _parse_list_args(name: str) -> List[str]:
    return parse_list_arg(name)


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
    filters = zfba_wcnr_jqaj_controller.parse_filters(default_time_range_for_page)
    try:
        return zfba_wcnr_jqaj_controller.build_summary_response(filters, _build_summary_payload)
    except Exception as exc:
        logging.exception(
            "zfba_wcnr_jqaj api_summary failed: start_time=%s end_time=%s hb_start_time=%s hb_end_time=%s leixing_list=%s za_types=%s",
            filters["start_time"],
            filters["end_time"],
            filters["hb_start_time"],
            filters["hb_end_time"],
            filters["leixing_list"],
            filters["za_types"],
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/export")
def export_summary() -> Response:
    filters = zfba_wcnr_jqaj_controller.parse_filters(default_time_range_for_page)
    return zfba_wcnr_jqaj_controller.build_summary_export_response(
        filters,
        _build_summary_payload,
        _build_summary_filename,
    )


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
    filters = zfba_wcnr_jqaj_controller.parse_filters(default_time_range_for_page)
    return zfba_wcnr_jqaj_controller.build_detail_page(
        filters=filters,
        detail_rows_loader=_load_detail_rows,
        region_name_resolver=_resolve_region_name,
        template_name="zfba_wcnr_jqaj_detail.html",
        limit=5000,
    )


@zfba_wcnr_jqaj_bp.route("/zfba_wcnr_jqaj/detail/export")
def export_detail() -> Response:
    filters = zfba_wcnr_jqaj_controller.parse_filters(default_time_range_for_page)
    return zfba_wcnr_jqaj_controller.build_detail_export_response(
        filters=filters,
        detail_rows_loader=_load_detail_rows,
        region_name_resolver=_resolve_region_name,
        filename_builder=_build_detail_filename,
        limit=0,
    )


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


