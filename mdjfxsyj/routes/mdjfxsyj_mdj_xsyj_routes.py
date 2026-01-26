"""
矛盾纠纷线索移交模块路由

- 访问前校验登录与权限（module = '矛盾纠纷线索移交'）
- 支持筛选、分页、导出（csv/xlsx）
"""

from __future__ import annotations

import logging

from flask import Blueprint, Response, abort, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from mdjfxsyj.service.mdjfxsyj_mdj_xsyj_service import export_to_csv, export_to_xlsx, get_mdj_xsyj_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

mdjfxsyj_bp = Blueprint(
    "mdjfxsyj",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@mdjfxsyj_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "矛盾纠纷线索移交"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception as exc:
        logging.error("权限检查失败: %s", exc)
        abort(500)


@mdjfxsyj_bp.get("/")
def index() -> str:
    try:
        start_time = request.args.get("start_time", "").strip() or None
        end_time = request.args.get("end_time", "").strip() or None
        jfmc = request.args.get("jfmc", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        page_str = request.args.get("page", "1")
        page_size_str = request.args.get("page_size", "20")
        try:
            page = max(int(page_str), 1)
        except ValueError:
            page = 1
        try:
            page_size = max(int(page_size_str), 1)
        except ValueError:
            page_size = 20

        rows, total, start_time_norm, end_time_norm = get_mdj_xsyj_data(
            start_time=start_time,
            end_time=end_time,
            jfmc=jfmc,
            fenju_list=fenju_list,
            page=page,
            page_size=page_size,
        )
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1

        return render_template(
            "mdjfxsyj_mdj_xsyj.html",
            rows=rows,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            start_time=start_time_norm,
            end_time=end_time_norm,
            jfmc=jfmc or "",
            fenju_selected=fenju_list or [],
        )
    except Exception as exc:
        logging.exception("查询失败")
        return render_template(
            "mdjfxsyj_mdj_xsyj.html",
            rows=[],
            total=0,
            page=1,
            page_size=20,
            total_pages=1,
            start_time="",
            end_time="",
            jfmc="",
            fenju_selected=[],
            error_message=f"查询失败: {exc}",
        )


@mdjfxsyj_bp.get("/export")
def export() -> Response:
    try:
        export_format = request.args.get("format", "csv").lower()
        start_time = request.args.get("start_time", "").strip() or None
        end_time = request.args.get("end_time", "").strip() or None
        jfmc = request.args.get("jfmc", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        if export_format == "xlsx":
            return export_to_xlsx(start_time=start_time, end_time=end_time, jfmc=jfmc, fenju_list=fenju_list)
        return export_to_csv(start_time=start_time, end_time=end_time, jfmc=jfmc, fenju_list=fenju_list)
    except Exception as exc:
        logging.exception("导出失败")
        abort(500, description=f"导出失败: {exc}")

