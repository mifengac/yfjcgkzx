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
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
import logging

from gonggong.config.database import get_database_connection
from jszahzyj.service.jszahzyj_service import (
    get_jszahzyj_data,
    export_to_csv,
    export_to_xlsx
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# 创建蓝图
jszahzyj_bp = Blueprint("jszahzyj", __name__, template_folder="../templates")


@jszahzyj_bp.before_request
def _check_access() -> None:
    """
    访问控制：
    - 需已登录
    - 且在 jcgkzx_permission 中拥有 module = '精神障碍' 的权限
    """
    if not session.get("username"):
        return redirect(url_for("login"))

    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "精神障碍"),
            )
            row = cur.fetchone()
        conn.close()

        if not row:
            abort(403)
    except Exception as e:
        logging.error(f"权限检查失败: {e}")
        abort(500)


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
        # 获取查询参数（所有参数都是可选的）
        liguan_start = request.args.get("liguan_start", "").strip() or None
        liguan_end = request.args.get("liguan_end", "").strip() or None
        maodun_start = request.args.get("maodun_start", "").strip() or None
        maodun_end = request.args.get("maodun_end", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        # 分页参数
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

        # 查询数据（不再检查是否有查询条件，允许无条件查询）
        rows, total = get_jszahzyj_data(
            liguan_start=liguan_start,
            liguan_end=liguan_end,
            maodun_start=maodun_start,
            maodun_end=maodun_end,
            fenju_list=fenju_list,
            page=page,
            page_size=page_size
        )

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size if total > 0 else 1

        return render_template(
            "jszahzyj.html",
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
        logging.error(f"查询数据失败: {e}")
        return render_template(
            "jszahzyj.html",
            rows=[],
            total=0,
            page=1,
            page_size=20,
            total_pages=0,
            error_message=f"查询失败: {str(e)}"
        )


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
        # 获取查询参数（所有参数都是可选的）
        export_format = request.args.get("format", "csv").lower()
        liguan_start = request.args.get("liguan_start", "").strip() or None
        liguan_end = request.args.get("liguan_end", "").strip() or None
        maodun_start = request.args.get("maodun_start", "").strip() or None
        maodun_end = request.args.get("maodun_end", "").strip() or None
        fenju_list = request.args.getlist("fenju") or None

        # 根据格式导出（不再验证必填参数，允许无条件导出）
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
        logging.error(f"导出数据失败: {e}")
        abort(500, description=f"导出失败: {str(e)}")
