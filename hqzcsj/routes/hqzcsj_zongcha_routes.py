"""
hqzcsj - 获取综查数据模块路由。

页面：
- GET /hqzcsj/zongcha

接口：
- POST /hqzcsj/zongcha/api/start
- GET  /hqzcsj/zongcha/api/status/<job_id>
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from hqzcsj.service.jsxx_service import get_jsxx_job_status, get_jsxx_sources, start_jsxx_job
from hqzcsj.service.zongcha_service import start_zongcha_job, get_zongcha_job_status
from hqzcsj.service.tqws_service import get_tqws_job_status, get_tqws_sources, start_tqws_job
from hqzcsj.service.zfba_jq_aj_service import default_time_range_for_page as jqaj_default_range


hqzcsj_bp = Blueprint("hqzcsj", __name__, template_folder="../templates")


@hqzcsj_bp.before_request
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


@hqzcsj_bp.route("/zongcha")
def zongcha_index() -> str:
    default_start = "2024-01-01 00:00:00"
    default_end = datetime.now().strftime("%Y-%m-%d 00:00:00")
    jqaj_default_start, jqaj_default_end = jqaj_default_range()
    return render_template(
        "hqzcsj_zongcha.html",
        default_start=default_start,
        default_end=default_end,
        jqaj_default_start=jqaj_default_start,
        jqaj_default_end=jqaj_default_end,
    )


@hqzcsj_bp.route("/zongcha/api/start", methods=["POST"])
def zongcha_start() -> Any:
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    cookie = (payload.get("cookie") or "").strip()
    authorization = (payload.get("authorization") or "").strip()
    start_time = (payload.get("start_time") or "").strip()
    end_time = (payload.get("end_time") or "").strip()
    sources = payload.get("sources") or []
    if not isinstance(sources, list):
        sources = []
    sources = [str(s).strip() for s in sources if str(s).strip()]

    if not cookie:
        return jsonify({"success": False, "message": "Cookie 不能为空"}), 400
    if not authorization:
        return jsonify({"success": False, "message": "Authorization 不能为空"}), 400
    if not start_time or not end_time:
        return jsonify({"success": False, "message": "开始时间/结束时间不能为空"}), 400

    try:
        datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return jsonify({"success": False, "message": "时间格式必须为 YYYY-MM-DD HH:MM:SS"}), 400

    username = session.get("username") or ""
    job_id = start_zongcha_job(
        username=username,
        cookie=cookie,
        authorization=authorization,
        start_time=start_time,
        end_time=end_time,
        sources=sources,
    )
    return jsonify({"success": True, "job_id": job_id})


@hqzcsj_bp.route("/zongcha/api/status/<job_id>")
def zongcha_status(job_id: str) -> Any:
    username = session.get("username") or ""
    status = get_zongcha_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/tqws/api/start", methods=["POST"])
def tqws_start() -> Any:
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    access_token = (payload.get("access_token") or "").strip()
    sources = payload.get("sources") or []
    if not isinstance(sources, list):
        sources = []
    sources = [str(s).strip() for s in sources if str(s).strip()]

    # 兼容旧版：source 单选
    if not sources:
        legacy_source = (payload.get("source") or "").strip()
        if legacy_source:
            sources = [legacy_source]

    if not access_token:
        return jsonify({"success": False, "message": "access_token 不能为空"}), 400
    if not sources:
        return jsonify({"success": False, "message": "请至少选择 1 个数据源"}), 400

    username = session.get("username") or ""
    job_id = start_tqws_job(
        username=username,
        access_token=access_token,
        sources=sources,
    )
    return jsonify({"success": True, "job_id": job_id})


@hqzcsj_bp.route("/zongcha/tqws/api/status/<job_id>")
def tqws_status(job_id: str) -> Any:
    username = session.get("username") or ""
    status = get_tqws_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/tqws/api/sources")
def tqws_sources() -> Any:
    return jsonify({"success": True, "data": get_tqws_sources()})


@hqzcsj_bp.route("/zongcha/jsxx/api/start", methods=["POST"])
def jsxx_start() -> Any:
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    session_cookie = (payload.get("session_cookie") or "").strip()
    start_date = (payload.get("start_date") or "").strip()
    end_date = (payload.get("end_date") or "").strip()
    sources = payload.get("sources") or []
    page_size = payload.get("page_size")

    if not isinstance(sources, list):
        sources = []
    sources = [str(s).strip() for s in sources if str(s).strip()]

    if not session_cookie:
        return jsonify({"success": False, "message": "SESSION 不能为空"}), 400
    if not start_date or not end_date:
        return jsonify({"success": False, "message": "开始日期/结束日期不能为空"}), 400
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except Exception:
        return jsonify({"success": False, "message": "日期格式必须为 YYYY-MM-DD"}), 400
    if not sources:
        return jsonify({"success": False, "message": "请至少选择 1 个数据源"}), 400
    valid_sources = {x.get("name", "") for x in get_jsxx_sources()}
    if any(s not in valid_sources for s in sources):
        return jsonify({"success": False, "message": "存在不支持的数据源"}), 400

    if page_size in (None, ""):
        page_size_int = None
    else:
        try:
            page_size_int = int(page_size)
        except Exception:
            return jsonify({"success": False, "message": "pageSize 必须是整数"}), 400
        if page_size_int <= 0:
            return jsonify({"success": False, "message": "pageSize 必须大于 0"}), 400

    username = session.get("username") or ""
    job_id = start_jsxx_job(
        username=username,
        session_cookie=session_cookie,
        start_date=start_date,
        end_date=end_date,
        sources=sources,
        page_size=page_size_int,
    )
    return jsonify({"success": True, "job_id": job_id})


@hqzcsj_bp.route("/zongcha/jsxx/api/status/<job_id>")
def jsxx_status(job_id: str) -> Any:
    username = session.get("username") or ""
    status = get_jsxx_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/jsxx/api/sources")
def jsxx_sources() -> Any:
    return jsonify({"success": True, "data": get_jsxx_sources()})
