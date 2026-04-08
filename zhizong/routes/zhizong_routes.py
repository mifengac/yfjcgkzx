"""
治综平台数据统计模块的路由层。

功能概述：
1. 提供治综首页，展示数据库中配置的任务按钮；
2. 展示单个任务的地区统计结果，支持点击地区查看详情；
3. 提供汇总与详情两类导出接口，覆盖 CSV 与 Excel。
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from flask import Blueprint, Response, abort, redirect, render_template, session, url_for
from werkzeug.exceptions import HTTPException

from gonggong.config.database import get_database_connection
from gonggong.utils.summary_detail_controller import (
    SummaryDetailController,
    SummaryDetailProvider,
    download_rows_as_excel,
)
from zhizong.service.zhizong_service import (
    REGION_MAP,
    fetch_home_summary,
    fetch_task_detail_rows,
    fetch_task_rows_all,
    fetch_task_summary,
    get_task_metadata,
)

zhizong_bp = Blueprint("zhizong", __name__, template_folder="../templates")


class _ZhizongTaskProvider(SummaryDetailProvider):
    def get_subject(self, subject_id: Any) -> Optional[Mapping[str, Any]]:
        return get_task_metadata(int(subject_id))

    def list_summary_rows(
        self,
        subject: Mapping[str, Any],
        *,
        region_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return fetch_task_summary(dict(subject), dwdm=region_code)

    def list_detail_rows(
        self,
        subject: Mapping[str, Any],
        region_code: str,
    ) -> List[Dict[str, Any]]:
        return fetch_task_detail_rows(dict(subject), region_code)

    def list_all_rows(self, subject: Mapping[str, Any]) -> List[Dict[str, Any]]:
        return fetch_task_rows_all(dict(subject))

    def get_display_name(self, subject: Mapping[str, Any]) -> str:
        return str(subject.get("task_name") or subject.get("table_name") or "治综数据")

    def get_rows_export_name(self, subject: Mapping[str, Any]) -> str:
        return str(subject.get("table_name") or "data")


zhizong_task_controller = SummaryDetailController(
    provider=_ZhizongTaskProvider(),
    region_map=REGION_MAP,
    subject_context_name="task",
)


@zhizong_bp.before_request
def _check_access() -> None:
    """请求前校验访问权限，使模块仅对授权地址开放。"""
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "治综"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except HTTPException:
        raise
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
    return render_template(
        "zhizong_detail.html",
        **zhizong_task_controller.build_summary_page_context(task_id),
    )


@zhizong_bp.route("/api/task/<int:task_id>/region_detail")
def region_detail(task_id: int) -> Response:
    """点击地区后加载详情数据。"""
    return zhizong_task_controller.json_detail(task_id)


@zhizong_bp.route("/api/task/<int:task_id>/region_detail/download")
def download_task_detail(task_id: int) -> Response:
    """导出地区详情表，默认 CSV，可选 Excel。"""
    return zhizong_task_controller.download_detail(task_id)


@zhizong_bp.route("/api/task/<int:task_id>/download")
def download_task_summary(task_id: int) -> Response:
    """导出汇总列表，支持 CSV / Excel。"""
    return zhizong_task_controller.download_summary(task_id)


@zhizong_bp.route("/download/home_summary")
def download_home_summary() -> Response:
    """导出首页治综任务统计，文件名固定为 '治综任务统计.xlsx'。"""
    return download_rows_as_excel(fetch_home_summary(), "治综任务统计.xlsx")


@zhizong_bp.route("/task/<int:task_id>/rows")
def task_rows_page(task_id: int) -> str:
    """新页面：展示任务详情表（可选按地区过滤），并支持下载CSV。"""
    return render_template(
        "zhizong_rows.html",
        **zhizong_task_controller.build_rows_page_context(task_id),
    )


@zhizong_bp.route("/task/<int:task_id>/rows/download")
def task_rows_download(task_id: int) -> Response:
    """下载 rows 页面显示的数据为 CSV。"""
    return zhizong_task_controller.download_rows_csv(task_id)
