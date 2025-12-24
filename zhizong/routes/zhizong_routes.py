"""
治综平台数据统计模块的路由层。

功能概述：
1. 提供治综首页，展示数据库中配置的任务按钮；
2. 展示单个任务的地区统计结果，支持点击地区查看详情；
3. 提供汇总与详情两类导出接口，覆盖 CSV 与 Excel。
"""

from __future__ import annotations

import csv
from datetime import datetime
from io import BytesIO, StringIO
from typing import List

from flask import (
    Blueprint,
    Response,
    abort,
    g,
    jsonify,
    render_template,
    request,
    send_file,
)
from openpyxl import Workbook

from flask import session, redirect, url_for
from gonggong.config.database import get_database_connection
from zhizong.service.zhizong_service import (
    fetch_active_tasks,
    fetch_home_summary,
    fetch_task_detail_rows,
    fetch_task_rows_all,
    fetch_task_summary,
    get_task_metadata,
    REGION_MAP,
)

zhizong_bp = Blueprint("zhizong", __name__, template_folder="../templates")


@zhizong_bp.before_request
def _check_access() -> None:
    """请求前校验访问权限，使模块仅对授权地址开放。"""
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s', (session["username"], "治综"))
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


@zhizong_bp.route("/")
def index() -> str:
    """治综模块首页，展示活动任务的地区汇总表，并支持导出。"""
    summary_rows = fetch_home_summary()
    return render_template("zhizong.html", summary_rows=summary_rows, region_codes=REGION_MAP)


@zhizong_bp.route("/task/<int:task_id>")
def task_detail(task_id: int) -> str:
    """单个任务的地区统计页面。"""
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    summary_rows = fetch_task_summary(task)
    return render_template("zhizong_detail.html", task=task, summary_rows=summary_rows)


@zhizong_bp.route("/api/task/<int:task_id>/region_detail")
def region_detail(task_id: int) -> Response:
    """点击地区后加载详情数据。"""
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    dwdm = request.args.get("dwdm")
    if not dwdm:
        return jsonify({"success": False, "message": "缺少参数 dwdm"}), 400

    try:
        rows = fetch_task_detail_rows(task, dwdm)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    return jsonify({"success": True, "data": rows or []})


@zhizong_bp.route("/api/task/<int:task_id>/region_detail/download")
def download_task_detail(task_id: int) -> Response:
    """导出地区详情表，默认 CSV，可选 Excel。"""
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    dwdm = request.args.get("dwdm")
    if not dwdm:
        return jsonify({"success": False, "message": "缺少参数 dwdm"}), 400

    export_format = request.args.get("format", "csv").lower()

    try:
        rows = fetch_task_detail_rows(task, dwdm)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    if not rows:
        return jsonify({"success": False, "message": "暂无可导出的数据"}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    display_name = task.get("task_name") or "治综数据"
    filename_prefix = f"{display_name}{timestamp}详情表"

    if export_format == "excel":
        return _download_excel(rows, f"{filename_prefix}.xlsx")
    return _download_csv(rows, f"{filename_prefix}.csv")


@zhizong_bp.route("/api/task/<int:task_id>/download")
def download_task_summary(task_id: int) -> Response:
    """导出汇总列表，支持 CSV / Excel。"""
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    export_format = request.args.get("format", "csv").lower()
    dwdm = request.args.get("dwdm")

    try:
        rows = fetch_task_summary(task, dwdm=dwdm)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    display_name = task.get("task_name") or "治综数据"
    filename_prefix = f"{display_name}{timestamp}"

    if export_format == "excel":
        return _download_excel(rows, f"{filename_prefix}.xlsx")
    return _download_csv(rows, f"{filename_prefix}.csv")


@zhizong_bp.route("/download/home_summary")
def download_home_summary() -> Response:
    """导出首页治综任务统计，文件名固定为 '治综任务统计.xlsx'。"""
    rows = fetch_home_summary()
    filename = "治综任务统计.xlsx"
    return _download_excel(rows, filename)


@zhizong_bp.route("/task/<int:task_id>/rows")
def task_rows_page(task_id: int) -> str:
    """新页面：展示任务详情表（可选按地区过滤），并支持下载CSV。"""
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    dwdm = request.args.get("dwdm")
    if dwdm:
        rows = fetch_task_detail_rows(task, dwdm)
        region_name = next((name for name, code in REGION_MAP.items() if code == dwdm), dwdm)
        title = f"{task.get('task_name') or task.get('table_name')} - {region_name}"
    else:
        rows = fetch_task_rows_all(task)
        title = task.get('task_name') or task.get('table_name')

    # 为了简单直接在此模板里渲染
    return render_template("zhizong_rows.html", title=title, task=task, rows=rows, dwdm=dwdm, region_map=REGION_MAP)


@zhizong_bp.route("/task/<int:task_id>/rows/download")
def task_rows_download(task_id: int) -> Response:
    """下载 rows 页面显示的数据为 CSV。

    - 若 dwdm 为空：文件名为 {task.table_name}.csv
    - 若 dwdm 有值：文件名为 {地区名}-{task.table_name}.csv
    """
    task = get_task_metadata(task_id)
    if not task:
        abort(404)

    dwdm = request.args.get("dwdm")
    if dwdm:
        rows = fetch_task_detail_rows(task, dwdm)
        region_name = next((name for name, code in REGION_MAP.items() if code == dwdm), dwdm)
        filename = f"{region_name}-{(task.get('table_name') or 'data')}.csv"
    else:
        rows = fetch_task_rows_all(task)
        filename = f"{(task.get('table_name') or 'data')}.csv"

    return _download_csv(rows or [], filename)


def _download_csv(rows: List[dict], filename: str) -> Response:
    """以 CSV 形式导出数据，包含所有字段。"""
    output = StringIO()
    if rows:
        headers = sorted({key for row in rows for key in row.keys()})
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: (row.get(key) or "") for key in headers})
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


def _download_excel(rows: List[dict], filename: str) -> Response:
    """以 Excel 形式导出数据，包含所有字段。"""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "统计数据"

    if rows:
        headers = sorted({key for row in rows for key in row.keys()})
        sheet.append(headers)
        for row in rows:
            sheet.append([(row.get(key) or "") for key in headers])
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
