"""
矛盾纠纷重复报警统计：路由层

Blueprint: mdjfxsyj_cfbj  （挂载在 /mdjfxsyj/cfbj）
权限校验：与主模块相同，module = '矛盾纠纷'

路由列表：
  GET /cfbj/api/summary      → JSON，按分局分组统计
  GET /cfbj/api/detail       → JSON，明细数据
  GET /cfbj/export           → 文件下载（csv/xlsx）
  GET /cfbj/modal_detail     → HTML 页（弹出框 iframe）
"""

from __future__ import annotations

import logging
from typing import List, Optional

from flask import (
    Blueprint,
    Response,
    abort,
    jsonify,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)

from gonggong.config.database import get_database_connection
from mdjfxsyj.service.mdjfxsyj_cfbj_service import (
    get_cfbj_detail,
    get_cfbj_summary,
    make_export_response,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

mdjfxsyj_cfbj_bp = Blueprint(
    "mdjfxsyj_cfbj",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


# --------------------------------------------------------------------------
# 权限校验（与主模块相同）
# --------------------------------------------------------------------------

@mdjfxsyj_cfbj_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "矛盾纠纷"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception as exc:
        logging.error("权限检查失败: %s", exc)
        abort(500)


# --------------------------------------------------------------------------
# 参数解析工具
# --------------------------------------------------------------------------

def _parse_min_cs() -> Optional[int]:
    val = (request.args.get("min_cs") or "").strip()
    if not val:
        return None
    try:
        n = int(val)
        if 1 <= n <= 100:
            return n
    except ValueError:
        pass
    return None


def _parse_fenju_list() -> Optional[List[str]]:
    vals = request.args.getlist("fenju")
    out = [v.strip() for v in vals if v.strip()]
    return out or None


# --------------------------------------------------------------------------
# API：分组统计
# --------------------------------------------------------------------------

@mdjfxsyj_cfbj_bp.get("/api/summary")
def api_summary():
    try:
        rows, s, e = get_cfbj_summary(
            start_time=request.args.get("start_time") or None,
            end_time=request.args.get("end_time") or None,
            huanbi_start=request.args.get("huanbi_start") or None,
            huanbi_end=request.args.get("huanbi_end") or None,
            fenju_list=_parse_fenju_list(),
            min_cs=_parse_min_cs(),
        )
        return jsonify({"success": True, "data": rows, "start_time": s, "end_time": e})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logging.exception("重复报警统计查询失败")
        return jsonify({"success": False, "message": str(exc)}), 500


# --------------------------------------------------------------------------
# API：明细数据
# --------------------------------------------------------------------------

@mdjfxsyj_cfbj_bp.get("/api/detail")
def api_detail():
    try:
        rows, s, e = get_cfbj_detail(
            start_time=request.args.get("start_time") or None,
            end_time=request.args.get("end_time") or None,
            fenju_list=_parse_fenju_list(),
            min_cs=_parse_min_cs(),
        )
        return jsonify({"success": True, "data": rows, "start_time": s, "end_time": e})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logging.exception("重复报警明细查询失败")
        return jsonify({"success": False, "message": str(exc)}), 500


# --------------------------------------------------------------------------
# API：刷新物化视图
# --------------------------------------------------------------------------

@mdjfxsyj_cfbj_bp.post("/api/refresh_data")
def refresh_data():
    view_name = "v_b_jq_xjzd2025"
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(f"REFRESH MATERIALIZED VIEW ywdata.{view_name};")
        conn.commit()
        conn.close()
        return jsonify(
            {
                "success": True,
                "message": f"已刷新 ywdata.{view_name} 物化视图",
                "view": f"ywdata.{view_name}",
            }
        )
    except Exception as exc:
        logging.exception("刷新物化视图失败")
        return jsonify({"success": False, "message": f"刷新失败: {exc}"}), 500


# --------------------------------------------------------------------------
# 导出（统计表 / 明细表 共用，弹出框内导出也走此路由）
# --------------------------------------------------------------------------

@mdjfxsyj_cfbj_bp.get("/export")
def export_data() -> Response:
    try:
        fmt = (request.args.get("fmt") or "csv").lower()
        if fmt not in ("csv", "xlsx"):
            fmt = "csv"

        data_type = (request.args.get("data_type") or "summary").strip()
        # data_type: summary | detail
        fenju_exact = (request.args.get("fenju_exact") or "").strip() or None
        detail_type = (request.args.get("detail_type") or "").strip() or None
        # detail_type: '总数' | '重复数'（弹出框使用）

        if data_type == "detail" or fenju_exact:
            rows, _, _ = get_cfbj_detail(
                start_time=request.args.get("start_time") or None,
                end_time=request.args.get("end_time") or None,
                fenju_list=_parse_fenju_list(),
                min_cs=_parse_min_cs(),
                fenju_exact=fenju_exact,
                detail_type=detail_type,
            )
            if fenju_exact and detail_type:
                prefix = f"{fenju_exact}_矛盾纠纷重复报警{detail_type}"
            else:
                prefix = "矛盾纠纷重复报警详情"
        else:
            rows, _, _ = get_cfbj_summary(
                start_time=request.args.get("start_time") or None,
                end_time=request.args.get("end_time") or None,
                fenju_list=_parse_fenju_list(),
                min_cs=_parse_min_cs(),
            )
            prefix = "矛盾纠纷重复报警统计"

        return make_export_response(rows, fmt=fmt, prefix=prefix)
    except ValueError as exc:
        abort(400, description=str(exc))
    except Exception as exc:
        logging.exception("导出失败")
        abort(500, description=f"导出失败: {exc}")


# --------------------------------------------------------------------------
# 弹出框 iframe 页面：显示某分局的 总数/重复数 明细
# --------------------------------------------------------------------------

_MODAL_DETAIL_HTML = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<style>
  body { margin:0; font-size:13px; color:#1f2937; }
  .toolbar { display:flex; justify-content:space-between; align-items:center;
             padding:10px 14px; border-bottom:1px solid #e6e8ef; background:#fbfcff; }
  .toolbar .title { font-weight:800; }
  .toolbar .info  { color:#666; font-size:12px; }
  .dropdown { position:relative; display:inline-block; }
  .dropdown-menu { display:none; position:absolute; right:0; top:34px; background:#fff;
                   border:1px solid #e6e8ef; border-radius:8px; min-width:120px;
                   box-shadow:0 6px 16px rgba(0,0,0,.12); overflow:hidden; z-index:20; }
  .dropdown-menu a { display:block; padding:8px 12px; color:#111; text-decoration:none; font-size:13px; }
  .dropdown-menu a:hover { background:#f3f5fb; }
  .dropdown.open .dropdown-menu { display:block; }
  .btn { height:32px; border:0; border-radius:8px; background:#1976d2; color:#fff;
         padding:0 12px; font-weight:700; cursor:pointer; font-size:13px; }
  .table-wrap { overflow:auto; max-height:calc(100vh - 56px); }
  table { border-collapse:collapse; width:100%; white-space:nowrap; }
  th,td { border-bottom:1px solid #eef0f6; padding:8px 12px; text-align:left; }
  th { position:sticky; top:0; background:#fbfcff; z-index:2; font-weight:700; }
  .no-data { padding:24px; color:#888; text-align:center; }
  .err { color:#b42318; padding:12px 14px; }
</style>
</head>
<body>
<div class="toolbar">
  <span class="title">{{ title }}</span>
  <div style="display:flex;align-items:center;gap:10px;">
    <span class="info">共 {{ rows|length }} 条</span>
    <div class="dropdown" id="modalExportDd">
      <button class="btn" onclick="document.getElementById('modalExportDd').classList.toggle('open')">导出</button>
      <div class="dropdown-menu">
        <a href="#" onclick="modalExport('xlsx');return false;">导出 xlsx</a>
        <a href="#" onclick="modalExport('csv');return false;">导出 csv</a>
      </div>
    </div>
  </div>
</div>
{% if rows %}
<div class="table-wrap">
  <table>
    <thead>
      <tr>{% for k in rows[0].keys() %}<th>{{ k }}</th>{% endfor %}</tr>
    </thead>
    <tbody>
      {% for row in rows %}
      <tr>{% for v in row.values() %}<td>{{ v if v is not none else '' }}</td>{% endfor %}</tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% elif error %}
<div class="err">{{ error }}</div>
{% else %}
<div class="no-data">暂无数据</div>
{% endif %}
<script>
  var EXPORT_URL = {{ export_url|tojson }};
  var EXPORT_PARAMS = {{ export_params|tojson }};
  function modalExport(fmt) {
    document.getElementById('modalExportDd').classList.remove('open');
    var p = Object.assign({}, EXPORT_PARAMS, { fmt: fmt });
    var qs = Object.entries(p).map(function(e){
      return encodeURIComponent(e[0]) + '=' + encodeURIComponent(e[1] || '');
    }).join('&');
    window.open(EXPORT_URL + '?' + qs, '_blank');
  }
  document.addEventListener('click', function(e){
    var dd = document.getElementById('modalExportDd');
    if (dd && !dd.contains(e.target)) dd.classList.remove('open');
  });
</script>
</body>
</html>
"""


@mdjfxsyj_cfbj_bp.get("/modal_detail")
def modal_detail():
    """供弹出框 iframe 调用，显示某分局总数/重复数明细。"""
    fenju_exact = (request.args.get("fenju_exact") or "").strip() or None
    detail_type = (request.args.get("detail_type") or "总数").strip()
    start_time = request.args.get("start_time") or None
    end_time = request.args.get("end_time") or None
    min_cs_raw = (request.args.get("min_cs") or "").strip()
    try:
        min_cs = int(min_cs_raw) if min_cs_raw else None
    except ValueError:
        min_cs = None

    title = f"{fenju_exact or '全部'}  —  {detail_type}明细"
    error: Optional[str] = None
    rows = []
    export_params: dict = {
        "start_time": start_time or "",
        "end_time": end_time or "",
        "fenju_exact": fenju_exact or "",
        "detail_type": detail_type,
        "data_type": "detail",
    }
    if min_cs is not None:
        export_params["min_cs"] = str(min_cs)

    try:
        rows, _, _ = get_cfbj_detail(
            start_time=start_time,
            end_time=end_time,
            fenju_list=None,
            min_cs=min_cs,
            fenju_exact=fenju_exact,
            detail_type=detail_type,
        )
    except Exception as exc:
        logging.exception("弹出框明细查询失败")
        error = str(exc)

    return render_template_string(
        _MODAL_DETAIL_HTML,
        rows=rows,
        title=title,
        error=error,
        export_url=url_for("mdjfxsyj_cfbj.export_data"),
        export_params=export_params,
    )
