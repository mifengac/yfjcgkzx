from __future__ import annotations

from flask import Blueprint, abort, redirect, render_template, request, session, url_for

from gonggong.config.database import get_database_connection


weichengnianren_bp = Blueprint(
    "weichengnianren",
    __name__,
    template_folder="../templates",
    static_folder="../static",
)

VALID_TABS = {"wcnr9lbq", "fzxxlxxshf"}


@weichengnianren_bp.before_request
def _check_access() -> None:
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        conn = get_database_connection()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT 1 FROM "ywdata"."jcgkzx_permission" WHERE username=%s AND module=%s',
                (session["username"], "鏈垚骞翠汉"),
            )
            row = cur.fetchone()
        conn.close()
        if not row:
            abort(403)
    except Exception:
        abort(500)


@weichengnianren_bp.route("/")
def index() -> str:
    active_tab = (request.args.get("tab") or "wcnr9lbq").strip().lower()
    if active_tab not in VALID_TABS:
        active_tab = "wcnr9lbq"
    return render_template("weichengnianren_index.html", active_tab=active_tab)


from weichengnianren.routes import wcnr_fzxxlxxshf_routes  # noqa: E402,F401
