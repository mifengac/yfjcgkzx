from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
import logging
from pathlib import Path
import subprocess
import tempfile
import os
import sys
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, send_file, session, url_for
from openpyxl import Workbook

from gonggong.config.database import get_database_connection
from hqzcsj.dao.zfba_jq_aj_dao import fetch_leixing_list
from hqzcsj.service.zfba_jq_aj_report_service import ZfbaJqAjReportService
from hqzcsj.service.zfba_jq_aj_service import (
    REGION_ORDER,
    append_ratio_columns,
    build_summary,
    default_time_range_for_page,
    fetch_detail,
)


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


def _parse_bool_arg(name: str) -> bool:
    v = (request.args.get(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


@zfba_jq_aj_bp.route("/zfba_jq_aj/api/summary")
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
            "zfba_jq_aj api_summary failed: start_time=%s end_time=%s hb_start_time=%s hb_end_time=%s leixing_list=%s za_types=%s",
            start_time,
            end_time,
            hb_start_time,
            hb_end_time,
            leixing_list,
            za_types,
        )
        return jsonify({"success": False, "message": str(exc)}), 400


@zfba_jq_aj_bp.route("/zfba_jq_aj/export")
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


def _normalize_to_script_date(value: str) -> str:
    text = (value or "").strip().replace("T", " ")
    if not text:
        raise ValueError("开始时间和结束时间不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    raise ValueError(f"无法解析时间: {value}")


@zfba_jq_aj_bp.route("/zfba_jq_aj/daily_report_export", methods=["POST"])
def daily_report_export() -> Response:
    """
    导出警情日报（HTML）。
    逻辑直接复用 hqzcsj/service/0111_hddj_ypbg.py 脚本。
    """
    if (session.get("username") or "").strip() != "admin":
        return jsonify({"success": False, "message": "仅 admin 用户可导出警情日报"}), 403

    params = request.get_json(silent=True) or {}
    start_time = (params.get("start_time") or "").strip()
    end_time = (params.get("end_time") or "").strip()
    try:
        start_date = _normalize_to_script_date(start_time)
        end_date = _normalize_to_script_date(end_time)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    script_path = Path(__file__).resolve().parents[1] / "service" / "0111_hddj_ypbg.py"
    if not script_path.exists():
        return jsonify({"success": False, "message": f"脚本不存在: {script_path}"}), 500
    template_path = Path(__file__).resolve().parents[1] / "templates" / "report_template.html"
    if not template_path.exists():
        return jsonify({"success": False, "message": f"模板不存在: {template_path}"}), 500

    tmp_dir = tempfile.mkdtemp(prefix="jq_daily_report_")
    output_path = os.path.join(tmp_dir, f"警情日报_{start_date}.html")
    cmd = [
        sys.executable,
        str(script_path),
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--output",
        output_path,
        "--template",
        str(template_path),
        "--quiet",
    ]

    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=1800,
        )
        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout or "").strip() or "生成失败"
            logging.error("导出警情日报失败: %s", msg)
            return jsonify({"success": False, "message": f"导出警情日报失败: {msg}"}), 500

        if not os.path.exists(output_path):
            return jsonify({"success": False, "message": "脚本执行成功但未生成HTML文件"}), 500

        with open(output_path, "rb") as f:
            html_bytes = f.read()

        return send_file(
            BytesIO(html_bytes),
            as_attachment=True,
            download_name=f"警情日报_{start_date}.html",
            mimetype="text/html; charset=utf-8",
        )
    except subprocess.TimeoutExpired:
        return jsonify({"success": False, "message": "生成超时，请缩小时间范围后重试"}), 500
    except Exception as exc:
        logging.exception("导出警情日报失败: %s", exc)
        return jsonify({"success": False, "message": f"导出警情日报失败: {exc}"}), 500
    finally:
        try:
            if os.path.exists(output_path):
                os.remove(output_path)
            os.rmdir(tmp_dir)
        except Exception:
            pass


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

