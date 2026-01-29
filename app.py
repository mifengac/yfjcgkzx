from __future__ import annotations

"""
Flask 应用入口。

本文件负责：
1. 初始化 Flask 实例与全局配置。
2. 注册三个业务模块的蓝图：警情案件、巡防统计、治综数据统计。
3. 提供首页、健康检查以及若干调试接口。
4. 对外暴露案件统计相关的 REST API。
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

import psycopg2
from flask import Flask, jsonify, render_template, request, url_for
from flask import session, redirect, flash
from werkzeug.security import check_password_hash, generate_password_hash

# ---------------------------------------------------------------------------
# 路径准备：将项目根目录加入 Python 搜索路径，便于导入自定义模块
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

# 未成年人（打架斗殴）模块代码位于 `weichengnianren-djdo/`（目录名含 '-' 不能作为包名）
# 将该目录加入 sys.path，以便导入其下的 `wcnr_djdo` 包。
WCN_DJDO_ROOT = os.path.join(PROJECT_ROOT, "weichengnianren-djdo")
if os.path.isdir(WCN_DJDO_ROOT) and WCN_DJDO_ROOT not in sys.path:
    sys.path.append(WCN_DJDO_ROOT)

# ---------------------------------------------------------------------------
# 导入项目内部依赖
# ---------------------------------------------------------------------------
from gonggong.config.database import DB_CONFIG  # noqa: E402
from gonggong.config.database import get_database_connection  # noqa: E402
from gonggong.service.session_manager import session_manager  # noqa: E402
from jingqing_anjian.routes.jingqing_anjian_routes import jingqing_anjian_bp  # noqa: E402
from jingqing_anjian.service.case_service import CaseService  # noqa: E402
from xunfang.routes.xunfang_routes import xunfang_bp  # noqa: E402
from zhizong.routes.zhizong_routes import zhizong_bp  # noqa: E402
from weichengnianren.routes.wcnr_routes import weichengnianren_bp  # noqa: E402
from gzrzdd.routes.gzrzdd_routes import gzrzdd_bp  # noqa: E402
from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp  # noqa: E402
from mdjfxsyj.routes.mdjfxsyj_mdj_xsyj_routes import mdjfxsyj_bp  # noqa: E402
from hqzcsj.routes.hqzcsj_zongcha_routes import hqzcsj_bp  # noqa: E402
from hqzcsj.routes.zfba_jq_aj_routes import zfba_jq_aj_bp  # noqa: E402
try:
    from wcnr_djdo import wcnr_djdo_bp  # type: ignore  # noqa: E402
except Exception:
    wcnr_djdo_bp = None

# ---------------------------------------------------------------------------
# Flask 应用与全局变量
# ---------------------------------------------------------------------------
app = Flask(__name__)
# 禁用 JSON ASCII 输出，避免中文被转义为 \uXXXX
app.config.setdefault("JSON_AS_ASCII", False)
import secrets
app.config['SECRET_KEY']=secrets.token_urlsafe(32)
# 警情案件数据服务实例，用于处理案件相关查询
case_service = CaseService()

# 首页可展示的模块列表：
# - key：模块唯一标识，对应访问控制配置
# - label：按钮显示文字
# - endpoint：Flask 端点，用于 url_for 生成跳转链接
MODULE_DEFINITIONS: List[Dict[str, str]] = [
    {"key": "jingqing_anjian", "label": "警情案件", "endpoint": "jingqing_anjian.jingqing_anjian"},
    {"key": "xunfang", "label": "巡防统计", "endpoint": "xunfang.xunfang"},
    {"key": "zhizong", "label": "治综平台数据统计", "endpoint": "zhizong.index"},
    {"key": "weichengnianren_djdo", "label": "未成年人(打架斗殴)", "endpoint": "wcnr_djdo.index"},
]


# ---------------------------------------------------------------------------
# 页面路由
# ---------------------------------------------------------------------------

@app.route("/")
def index() -> str:
    """
    首页：根据访问者 IP 地址动态展示可用模块。
    """
    if session.get("username"):
        return redirect(url_for("main"))
    return render_template("login.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    """
    登录页与登录提交：
    - 使用 jcgkzx_user 表校验用户名与密码（密码为Werkzeug哈希）
    - 成功后写入会话并跳转主菜单
    """
    if request.method == "GET":
        if session.get("username"):
            return redirect(url_for("main"))
        return render_template("login.html")

    # POST 提交登录
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    if not username or not password:
        flash("请输入用户名和密码")
        return render_template("login.html"), 400

    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT password FROM "ywdata"."jcgkzx_user" WHERE username = %s', (username,))
            row = cur.fetchone()
        conn.close()
        if not row:
            flash("用户名或密码错误")
            return render_template("login.html"), 401
        hashed = row[0]
        if not check_password_hash(hashed, password):
            flash("用户名或密码错误")
            return render_template("login.html"), 401
        # 登录成功
        session["username"] = username
        return redirect(url_for("main"))
    except Exception as exc:
        return render_template("login.html", error=f"登录失败: {exc}"), 500


@app.route("/logout")
def logout():
    """注销并回到登录页"""
    session.clear()
    return redirect(url_for("login"))


@app.route("/main")
def main():
    """
    主菜单：基于当前登录用户名，从权限表加载模块列表。
    权限表 jcgkzx_permission 的 module 字段取值：巡防/治综/警情/后台。
    """
    username = session.get("username")
    if not username:
        return redirect(url_for("login"))

    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT module FROM "ywdata"."jcgkzx_permission" WHERE username = %s', (username,))
            rows = cur.fetchall()
        conn.close()

        modules: List[Dict[str, str]] = []
        for (module_name,) in rows:
            conf_map = {
                "警情": {"endpoint": "jingqing_anjian.jingqing_anjian", "label": "警情案件"},
                "巡防": {"endpoint": "xunfang.xunfang", "label": "巡防统计"},
                "治综": {"endpoint": "zhizong.index", "label": "治综平台数据统计"},
                "后台": {"endpoint": "houtai.import_page", "label": "后台管理"},
                "未成年人": {"endpoint": "weichengnianren.index", "label": "未成年人"},
                "工作日志督导": {"endpoint": "gzrzdd.index", "label": "工作日志督导"},
                "精神障碍": {"endpoint": "jszahzyj.index", "label": "精神障碍患者预警"},
                "矛盾纠纷线索移交": {"endpoint": "mdjfxsyj.index", "label": "矛盾纠纷线索移交"},
                "获取综查数据": {"endpoint": "hqzcsj.zongcha_index", "label": "获取综查数据"},
            }
            if wcnr_djdo_bp is not None:
                conf_map["未成年人(打架斗殴)"] = {"endpoint": "wcnr_djdo.index", "label": "未成年人(打架斗殴)"}

            conf = conf_map.get(module_name)
            if not conf:
                continue
            modules.append({"label": conf["label"], "url": url_for(conf["endpoint"])})

        return render_template("index.html", modules=modules, username=username)
    except Exception as exc:
        return render_template("index.html", modules=[], username=username, error=f"加载权限失败: {exc}")


@app.route("/health")
def health_check():
    """
    系统健康检查接口：
    - 检查数据库连通性
    - 检查会话管理器状态
    - 尝试执行一次登录以验证凭证有效性
    """
    status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "database": "unknown",
            "session_manager": "unknown",
            "login_test": "unknown",
        },
    }

    # 检查数据库连接是否可用
    try:
        connection = get_database_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        connection.close()
        status["services"]["database"] = "healthy"
    except Exception as exc:
        status["status"] = "unhealthy"
        status["services"]["database"] = f"unhealthy: {exc}"

    # 检查会话管理器与登录状态
    try:
        session_exists = session_manager.get_session() is not None
        status["services"]["session_manager"] = "healthy" if session_exists else "unhealthy: no session"

        login_ok = session_manager.test_login()
        status["services"]["login_test"] = "passed" if login_ok else "failed"
        if not session_exists or not login_ok:
            status["status"] = "unhealthy"
    except Exception as exc:
        status["status"] = "unhealthy"
        status["services"]["session_manager"] = f"unhealthy: {exc}"
        status["services"]["login_test"] = f"failed: {exc}"

    return jsonify(status)


# ---------------------------------------------------------------------------
# 调试接口：便于排查会话/登录问题
# ---------------------------------------------------------------------------

@app.route("/debug/login")
def debug_login():
    """
    查看当前会话状态，并尝试执行一次登录检测。
    """
    try:
        login_test = session_manager.test_login()
        response = {
            "session_exists": session_manager.session is not None,
            "last_login_time": session_manager.last_login_time.isoformat()
            if session_manager.last_login_time
            else None,
            "failure_count": session_manager.login_failure_count,
            "cooldown_active": (
                session_manager.last_login_failure_time
                and datetime.now() - session_manager.last_login_failure_time < session_manager.login_failure_cooldown
            ),
            "login_test": login_test,
        }
        return jsonify(response)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/debug/reset_login")
def reset_login_state():
    """
    将会话管理器重置为初始状态：
    - 清零失败次数
    - 清空会话缓存
    - 恢复冷却时间
    """
    try:
        session_manager.login_failure_count = 0
        session_manager.last_login_failure_time = None
        session_manager.login_failure_cooldown = timedelta(minutes=1)
        session_manager.session = None
        session_manager.last_login_time = None
        return jsonify({"success": True, "message": "登录状态已重置"})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# 警情案件相关 REST 接口
# ---------------------------------------------------------------------------

@app.route("/api/case_stats")
def api_case_stats():
    """
    获取案件统计数据。
    支持按案件类型和时间范围过滤。
    """
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    data = case_service.get_case_stats_by_type(case_type, start_time, end_time)
    return jsonify({"success": True, "data": data, "count": len(data)})


@app.route("/api/case_types")
def api_case_types():
    """
    查询案件类型配置，供前端下拉框使用。
    """
    try:
        connection = get_database_connection()
        with connection.cursor() as cursor:
            cursor.execute('SELECT leixing FROM "ywdata"."case_type_config"')
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        connection.close()

        items = [dict(zip(columns, row)) for row in rows]
        return jsonify({"success": True, "data": items})
    except Exception as exc:
        return jsonify({"success": False, "message": f"查询案件类型失败: {exc}"}), 500


@app.route("/api/case_details")
def api_case_details():
    """
    查询案件详情列表。
    返回字段顺序按照 service 层的配置排序。
    """
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = case_service.get_ordered_case_details(case_type, start_time, end_time)
    return jsonify(
        {
            "success": True,
            "field_config": result.get("field_config", []),
            "data": result.get("data", []),
            "count": len(result.get("data", [])),
        }
    )

@app.route("/api/case_ry_details")
def api_case_ry_details():
    """
    查询人员详情列表。
    列顺序与数据库函数返回顺序一致。
    """
    case_type = request.args.get("case_type")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    result = case_service.get_case_ry_data(case_type, start_time, end_time)
    return jsonify(
        {
            "success": True,
            "columns": result.get("columns", []),
            "data": result.get("data", []),
            "count": len(result.get("data", [])),
        }
    )


# ---------------------------------------------------------------------------
# 蓝图注册：将子模块挂载到主应用
# ---------------------------------------------------------------------------
app.register_blueprint(xunfang_bp, url_prefix="/xunfang")
app.register_blueprint(zhizong_bp, url_prefix="/zhizong")
app.register_blueprint(jingqing_anjian_bp, url_prefix="/jingqing_anjian")
app.register_blueprint(weichengnianren_bp, url_prefix="/weichengnianren")
app.register_blueprint(gzrzdd_bp, url_prefix="/gzrzdd")
app.register_blueprint(jszahzyj_bp, url_prefix="/jszahzyj")
app.register_blueprint(mdjfxsyj_bp, url_prefix="/mdjfxsyj")
app.register_blueprint(hqzcsj_bp, url_prefix="/hqzcsj")
app.register_blueprint(zfba_jq_aj_bp, url_prefix="/hqzcsj")
if wcnr_djdo_bp is not None:
    app.register_blueprint(wcnr_djdo_bp, url_prefix="/weichengnianren-djdo")
try:
    from houtai.routes.houtai_routes import houtai_bp  # 后台批量导入模块
    app.register_blueprint(houtai_bp, url_prefix="/houtai")
except Exception:
    pass


# ---------------------------------------------------------------------------
# 开发调试入口
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        print("初始化会话管理器...")
        # session_manager.get_session()
        print("会话管理器初始化完成")
    except Exception as exc:
        print(f"会话管理器初始化失败: {exc}")

    app.run(host="0.0.0.0", port=5003, debug=True)
