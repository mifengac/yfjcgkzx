from __future__ import annotations

"""
将自定义驱动报表系统作为主项目的子模块挂载。

路由约定（挂载到主应用 url_prefix="/zidingyi_baobiao" 后）：
- GET  /zidingyi_baobiao/                 子模块入口（返回 JSON 指引）
- POST /zidingyi_baobiao/api/datasource   数据源管理
- POST /zidingyi_baobiao/api/dataset      dataset 管理
- POST /zidingyi_baobiao/api/module       module 管理与查询/导出
"""

from flask import Blueprint, abort, current_app, jsonify, redirect, render_template, request, url_for
from flask import session as flask_session

from gonggong.config.database import get_database_connection
from zidingyi_baobiao.api.datasource import datasource_bp
from zidingyi_baobiao.api.dataset import dataset_bp
from zidingyi_baobiao.api.module import module_bp


zdybb_bp = Blueprint("zdybb", __name__, template_folder="templates")


@zdybb_bp.before_request
def _ensure_access():  # type: ignore[no-untyped-def]
    """
    复用主项目登录态与权限表进行访问控制。

    可通过 Flask config 关闭（用于独立运行/调试）：
    - ZDYBB_REQUIRE_AUTH: False
    """
    if current_app.config.get("ZDYBB_REQUIRE_AUTH") is False:
        return None

    username = flask_session.get("username")
    if not username:
        return redirect(url_for("login"))

    perm_name = str(current_app.config.get("ZDYBB_PERMISSION_NAME") or "自定义报表").strip() or "自定义报表"
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (username, perm_name),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)
    return None


@zdybb_bp.get("/")
def index():  # type: ignore[no-untyped-def]
    """
    子模块入口（最简管理页）。
    """
    api_prefix = f"{request.script_root.rstrip('/')}/zidingyi_baobiao/api".rstrip("/")
    return render_template("zdybb_admin.html", api_prefix=api_prefix)


@zdybb_bp.errorhandler(403)
def _handle_403(_exc):  # type: ignore[no-untyped-def]
    return jsonify({"success": False, "message": "无权限访问该模块"}), 403


@zdybb_bp.errorhandler(500)
def _handle_500(_exc):  # type: ignore[no-untyped-def]
    return jsonify({"success": False, "message": "服务器内部错误"}), 500


# 子模块 API：统一挂载在 /api 前缀下，避免与主项目其它接口冲突
zdybb_bp.register_blueprint(datasource_bp, url_prefix="/api")
zdybb_bp.register_blueprint(dataset_bp, url_prefix="/api")
zdybb_bp.register_blueprint(module_bp, url_prefix="/api")
