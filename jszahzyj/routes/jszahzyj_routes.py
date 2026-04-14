"""
精神障碍患者预警模块路由

功能说明：
- 访问前校验当前登录用户是否拥有"精神障碍"模块权限
- 支持按列管时间、矛盾纠纷录入时间、分局进行筛选
- 支持分页查询
- 支持导出为CSV/Excel格式
"""
from flask import (
    Blueprint,
    Response,
    abort,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
import logging
from werkzeug.exceptions import HTTPException

from gonggong.config.database import get_database_connection
from jszahzyj.service.jszahzyj_service import (
    get_jszahzyj_data,
    export_to_csv,
    export_to_xlsx
)

logger = logging.getLogger(__name__)

# 创建蓝图
jszahzyj_bp = Blueprint(
    "jszahzyj",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


def _parse_positive_int(value: str, default: int) -> int:
    try:
        return max(int(value), 1)
    except (TypeError, ValueError):
        return default


def _render_index_page(**context) -> str:
    defaults = {
        "rows": [],
        "total": 0,
        "page": 1,
        "page_size": 20,
        "total_pages": 1,
        "liguan_start": "",
        "liguan_end": "",
        "maodun_start": "",
        "maodun_end": "",
        "fenju_selected": [],
    }
    defaults.update(context)
    return render_template("jszahzyj.html", **defaults)


@jszahzyj_bp.before_request
def _check_access() -> Response | None:
    """
    访问控制：
    - 需已登录
    - 且在 jcgkzx_permission 中拥有 module = '精神障碍' 的权限
    """
    if not session.get("username"):
        return redirect(url_for("login"))

    conn = None
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "精神障碍"),
            )
            row = cur.fetchone()

        if not row:
            abort(403)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("权限检查失败: %s", e)
        abort(500)
    finally:
        if conn is not None:
            conn.close()


@jszahzyj_bp.route("/")
def index() -> str:
    """
    精神障碍患者预警模块首页

    支持的查询参数：
    - liguan_start: 列管时间开始
    - liguan_end: 列管时间结束
    - maodun_start: 矛盾纠纷录入时间开始
    - maodun_end: 矛盾纠纷录入时间结束
    - fenju: 分局（多选，可传递多个）
    - page: 页码（默认1）
    - page_size: 每页记录数（默认20）
    """
    try:
        liguan_start = request.args.get("liguan_start", "").strip() or None
        liguan_end = request.args.get("liguan_end", "").strip() or None
        maodun_start = request.args.get("maodun_start", "").strip() or None
        maodun_end = request.args.get("maodun_end", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        page = _parse_positive_int(request.args.get("page", "1"), 1)
        page_size = _parse_positive_int(request.args.get("page_size", "20"), 20)

        rows, total = get_jszahzyj_data(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
            page=page,
            page_size=page_size
        )

        total_pages = (total + page_size - 1) // page_size if total > 0 else 1

        return _render_index_page(
            rows=rows,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            liguan_start=liguan_start or "",
            liguan_end=liguan_end or "",
            maodun_start=maodun_start or "",
            maodun_end=maodun_end or "",
            fenju_selected=fenju_list or []
        )

    except Exception as e:
        logger.error("查询数据失败: %s", e)
        return _render_index_page(total_pages=0, error_message=f"查询失败: {str(e)}")


@jszahzyj_bp.route("/export")
def export() -> Response:
    """
    导出数据

    支持的查询参数：
    - format: 导出格式（csv 或 xlsx）
    - liguan_start: 列管时间开始
    - liguan_end: 列管时间结束
    - maodun_start: 矛盾纠纷录入时间开始
    - maodun_end: 矛盾纠纷录入时间结束
    - fenju: 分局（多选）
    """
    try:
        export_format = request.args.get("format", "csv").lower()
        liguan_start = request.args.get("liguan_start", "").strip() or None
        liguan_end = request.args.get("liguan_end", "").strip() or None
        maodun_start = request.args.get("maodun_start", "").strip() or None
        maodun_end = request.args.get("maodun_end", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        if export_format == "xlsx":
            return export_to_xlsx(
                liguan_start=liguan_start,
                liguan_end=liguan_end,
                maodun_start=maodun_start,
                maodun_end=maodun_end,
                fenju_list=fenju_list
            )
        else:
            return export_to_csv(
                liguan_start=liguan_start,
                liguan_end=liguan_end,
                maodun_start=maodun_start,
                maodun_end=maodun_end,
                fenju_list=fenju_list
            )

    except Exception as e:
        logger.error("导出数据失败: %s", e)
        abort(500, description=f"导出失败: {str(e)}")


# 注册“精神病人警情案件统计”子功能路由（与 jszahzyj_bp 同一个蓝图）
from jszahzyj.routes import jsbrjqajtj_routes_impl  # noqa: E402,F401
from jszahzyj.routes import jszahz_topic_routes_impl  # noqa: E402,F401
from jszahzyj.routes import jszahz_topic_relation_routes_impl  # noqa: E402,F401
