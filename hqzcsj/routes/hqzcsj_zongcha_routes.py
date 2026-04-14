"""
hqzcsj - 获取综查数据模块路由。

页面：
- GET /hqzcsj/zongcha

接口：
- POST /hqzcsj/zongcha/api/start
- GET  /hqzcsj/zongcha/api/status/<job_id>
"""

from __future__ import annotations

import hmac
import os
from datetime import datetime
from typing import Any, Dict

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from hqzcsj.routes.route_helpers import user_has_module_access
from hqzcsj.service.jsxx_service import get_jsxx_job_status, get_jsxx_sources, start_jsxx_job
from hqzcsj.service.zongcha_source_catalog_service import get_source_catalog
from hqzcsj.service.zongcha_service import start_zongcha_job, get_zongcha_job_status
from hqzcsj.service.tqws_service import get_tqws_job_status, start_tqws_job
from hqzcsj.service.zfba_jq_aj_service import default_time_range_for_page as jqaj_default_range
from hqzcsj.routes import wcnr_10lv_routes as wcnr_10lv_proxy


hqzcsj_bp = Blueprint("hqzcsj", __name__, template_folder="../templates", static_folder="../static")
FETCH_TAB_SESSION_KEY = "hqzcsj_fetch_tab_unlocked"


def _fetch_tab_password() -> str:
    return os.getenv("HQZCSJ_FETCH_TAB_PASSWORD", "qqq")


def _is_fetch_tab_unlocked() -> bool:
    return bool(session.get(FETCH_TAB_SESSION_KEY))


def _guard_fetch_tab_api() -> Any:
    if _is_fetch_tab_unlocked():
        return None
    return jsonify({"success": False, "message": "请先输入“获取综查数据”页面密码"}), 403


@hqzcsj_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        if not user_has_module_access(get_database_connection, username=session["username"]):
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
        fetch_tab_unlocked=_is_fetch_tab_unlocked(),
    )


@hqzcsj_bp.route("/zongcha/wcnr10lv/api/leixing")
def zongcha_wcnr10lv_api_leixing() -> Any:
    return wcnr_10lv_proxy.api_leixing()


@hqzcsj_bp.route("/zongcha/wcnr10lv/api/summary")
def zongcha_wcnr10lv_api_summary() -> Any:
    return wcnr_10lv_proxy.api_summary()


@hqzcsj_bp.route("/zongcha/wcnr10lv/detail")
def zongcha_wcnr10lv_detail_page() -> Any:
    return wcnr_10lv_proxy.detail_page()


@hqzcsj_bp.route("/zongcha/wcnr10lv/api/detail")
def zongcha_wcnr10lv_api_detail() -> Any:
    return wcnr_10lv_proxy.api_detail()


@hqzcsj_bp.route("/zongcha/wcnr10lv/detail/export")
def zongcha_wcnr10lv_export_detail_single() -> Any:
    return wcnr_10lv_proxy.export_detail_single()


@hqzcsj_bp.route("/zongcha/wcnr10lv/export")
def zongcha_wcnr10lv_export_summary() -> Any:
    return wcnr_10lv_proxy.export_summary()


@hqzcsj_bp.route("/zongcha/wcnr10lv/export_detail")
def zongcha_wcnr10lv_export_detail_all() -> Any:
    return wcnr_10lv_proxy.export_detail_all()


@hqzcsj_bp.route("/zongcha/api/fetch-auth", methods=["POST"])
def zongcha_fetch_auth() -> Any:
    payload: Dict[str, Any] = request.get_json(silent=True) or {}
    password = str(payload.get("password") or "")
    if hmac.compare_digest(password, _fetch_tab_password()):
        session[FETCH_TAB_SESSION_KEY] = True
        return jsonify({"success": True})
    return jsonify({"success": False, "message": "密码错误"}), 400


@hqzcsj_bp.route("/zongcha/api/start", methods=["POST"])
def zongcha_start() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

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
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    username = session.get("username") or ""
    status = get_zongcha_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/api/sources")
def zongcha_sources() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    scope = (request.args.get("scope") or "all").strip().lower()
    if scope not in ("fetch", "tqws", "all"):
        return jsonify({"success": False, "message": "scope 仅支持 fetch/tqws/all"}), 400
    data = get_source_catalog(scope=scope)
    return jsonify({"success": True, "data": data})


@hqzcsj_bp.route("/zongcha/tqws/api/start", methods=["POST"])
def tqws_start() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

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

    kjsj_start = (payload.get("kjsj_start") or "").strip()
    kjsj_end = (payload.get("kjsj_end") or "").strip()

    if not access_token:
        return jsonify({"success": False, "message": "access_token 不能为空"}), 400
    if not sources:
        return jsonify({"success": False, "message": "请至少选择 1 个数据源"}), 400

    username = session.get("username") or ""
    job_id = start_tqws_job(
        username=username,
        access_token=access_token,
        sources=sources,
        kjsj_start=kjsj_start,
        kjsj_end=kjsj_end,
    )
    return jsonify({"success": True, "job_id": job_id})


@hqzcsj_bp.route("/zongcha/tqws/api/status/<job_id>")
def tqws_status(job_id: str) -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    username = session.get("username") or ""
    status = get_tqws_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/tqws/api/sources")
def tqws_sources() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    data = get_source_catalog(scope="tqws")
    legacy_data = [{"key": x.get("key"), "name": x.get("name"), "table": x.get("table")} for x in data]
    return jsonify({"success": True, "data": legacy_data})


@hqzcsj_bp.route("/zongcha/jsxx/api/start", methods=["POST"])
def jsxx_start() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

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
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    username = session.get("username") or ""
    status = get_jsxx_job_status(username=username, job_id=job_id)
    if not status:
        return jsonify({"success": False, "message": "任务不存在或已过期"}), 404
    return jsonify({"success": True, "data": status})


@hqzcsj_bp.route("/zongcha/jsxx/api/sources")
def jsxx_sources() -> Any:
    guard_resp = _guard_fetch_tab_api()
    if guard_resp:
        return guard_resp

    return jsonify({"success": True, "data": get_jsxx_sources()})


@hqzcsj_bp.route("/zongcha/api/import/sx_xls", methods=["POST"])
def zongcha_import_sx_xls() -> Any:
    """
    导入送校数据：
    - sheet1（累计招生）→ ywdata.zq_zfba_wcnr_sfzxx
    - sheet3（离校）    → ywdata.zq_zfba_wcnr_sfzxx_lxxx
    """
    import sys
    import tempfile
    import types
    from pathlib import Path as _Path

    file = request.files.get("file")
    if not file:
        return jsonify({"success": False, "message": "未选择文件"}), 400
    filename = str(file.filename or "")
    if not filename.lower().endswith(".xls"):
        return jsonify({"success": False, "message": "仅支持 xls 格式文件"}), 400

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xls") as tmp:
            tmp.write(file.read())
            tmp_path = _Path(tmp.name)

        # 动态加载 import_sfzxx_file（直接编译源码，绕过 pyc 缓存）
        import types
        script_path = _Path(__file__).resolve().parents[2] / "weichengnianren-djdo" / "0125_wcnr_sfzxx_import.py"
        mod_name = "hqzcsj.wcnr_sfzxx_import"
        source = script_path.read_text(encoding="utf-8")
        code = compile(source, str(script_path), "exec")
        mod = types.ModuleType(mod_name)
        mod.__file__ = str(script_path)
        sys.modules[mod_name] = mod
        try:
            exec(code, mod.__dict__)
        except Exception:
            sys.modules.pop(mod_name, None)
            raise

        import_func = getattr(mod, "import_sfzxx_file", None)
        if import_func is None:
            raise RuntimeError("导入脚本缺少 import_sfzxx_file()")

        stats_lj = import_func(
            tmp_path,
            sheet_name="累计招生",
            truncate=False,
            db_schema="ywdata",
            db_table="zq_zfba_wcnr_sfzxx",
        )
        stats_lx = import_func(
            tmp_path,
            sheet_name="离校",
            truncate=False,
            db_schema="ywdata",
            db_table="zq_zfba_wcnr_sfzxx_lxxx",
            header_row_index=2,
        )
        return jsonify({
            "success": True,
            "message": "导入成功",
            "stats": {
                "累计招生（zq_zfba_wcnr_sfzxx）": stats_lj,
                "离校（zq_zfba_wcnr_sfzxx_lxxx）": stats_lx,
            },
        })
    except UnicodeEncodeError as exc:
        bad = ""
        try:
            if isinstance(exc.object, str):
                bad = exc.object[exc.start: exc.end]
        except Exception:
            bad = ""
        codepoints = " ".join(f"U+{ord(ch):04X}" for ch in bad) if bad else ""
        msg = "导入失败：存在数据库编码无法写入的字符"
        if bad:
            msg += f" {bad!r}({codepoints})"
        msg += "。请将数据库客户端编码调整为 GB18030/UTF8，或先在 Excel 中清洗该字符后再导入。"
        import logging
        logging.exception("import sx xls failed (unicode encode)")
        return jsonify({"success": False, "message": msg}), 500
    except Exception as exc:
        import logging
        logging.exception("import sx xls failed")
        return jsonify({"success": False, "message": str(exc)}), 500
    finally:
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
