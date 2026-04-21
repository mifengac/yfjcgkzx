from __future__ import annotations

import logging
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, jsonify, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection
from mdjfxsyj.service.mdjfxsyj_mdjfjqfx_export import (
    build_all_details_export,
    build_detail_export,
    build_summary_export,
)
from mdjfxsyj.service.mdjfxsyj_mdjfjqfx_service import (
    get_detail_payload,
    get_options,
    get_summary_payload,
    normalize_group_by,
    normalize_repeat_min,
)


logger = logging.getLogger(__name__)


mdjfxsyj_mdjfjqfx_bp = Blueprint(
    "mdjfxsyj_mdjfjqfx",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)


@mdjfxsyj_mdjfjqfx_bp.before_request
def _check_access():
    if not session.get("username"):
        return redirect(url_for("login"))
    conn = None
    row = None
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "矛盾纠纷"),
            )
            row = cur.fetchone()
    except Exception:
        logger.exception("矛盾纠纷警情统计权限检查失败")
        abort(500)
    finally:
        if conn:
            conn.close()
    if not row:
        abort(403)


def _list_arg(name: str) -> List[str]:
    values = request.args.getlist(name)
    if len(values) == 1 and "," in values[0]:
        values = values[0].split(",")
    return [value.strip() for value in values if value.strip()]


def _common_kwargs() -> Dict[str, Any]:
    return {
        "start_time": request.args.get("start_time", "").strip() or None,
        "end_time": request.args.get("end_time", "").strip() or None,
        "ssfjdm_list": _list_arg("ssfjdm"),
        "group_by": normalize_group_by(request.args.get("group_by")),
        "repeat_min": normalize_repeat_min(request.args.get("repeat_min")),
    }


@mdjfxsyj_mdjfjqfx_bp.get("/")
def index() -> str:
    return render_template("mdjfxsyj_mdjfjqfx.html")


@mdjfxsyj_mdjfjqfx_bp.get("/api/options")
def api_options():
    try:
        return jsonify({"success": True, "data": get_options()})
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计选项加载失败")
        return jsonify({"success": False, "message": str(exc)}), 500


@mdjfxsyj_mdjfjqfx_bp.get("/api/summary")
def api_summary():
    try:
        payload = get_summary_payload(**_common_kwargs())
        return jsonify({"success": True, "data": payload})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计汇总查询失败")
        return jsonify({"success": False, "message": str(exc)}), 500


@mdjfxsyj_mdjfjqfx_bp.get("/api/detail")
def api_detail():
    try:
        page = max(int(request.args.get("page", "1") or 1), 1)
        page_size = int(request.args.get("page_size", "20") or 20)
        payload = get_detail_payload(
            **_common_kwargs(),
            dimension=(request.args.get("dimension") or "original_total").strip(),
            group_code=(request.args.get("group_code") or "__TOTAL__").strip(),
            ori_code=(request.args.get("ori_code") or "").strip(),
            confirm_code=(request.args.get("confirm_code") or "").strip(),
            page=page,
            page_size=page_size,
        )
        return jsonify({"success": True, "data": payload})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计详情查询失败")
        return jsonify({"success": False, "message": str(exc)}), 500


@mdjfxsyj_mdjfjqfx_bp.get("/export/summary")
def export_summary() -> Response:
    try:
        return build_summary_export(**_common_kwargs())
    except ValueError as exc:
        abort(400, description=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计汇总导出失败")
        abort(500, description=f"导出失败: {exc}")


@mdjfxsyj_mdjfjqfx_bp.get("/export/details")
def export_details() -> Response:
    try:
        return build_all_details_export(**_common_kwargs())
    except ValueError as exc:
        abort(400, description=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计全量详情导出失败")
        abort(500, description=f"导出失败: {exc}")


@mdjfxsyj_mdjfjqfx_bp.get("/export/detail")
def export_detail() -> Response:
    try:
        return build_detail_export(
            **_common_kwargs(),
            dimension=(request.args.get("dimension") or "original_total").strip(),
            group_code=(request.args.get("group_code") or "__TOTAL__").strip(),
            ori_code=(request.args.get("ori_code") or "").strip(),
            confirm_code=(request.args.get("confirm_code") or "").strip(),
            page=1,
        )
    except ValueError as exc:
        abort(400, description=str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("矛盾纠纷警情统计当前详情导出失败")
        abort(500, description=f"导出失败: {exc}")
