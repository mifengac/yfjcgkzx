from __future__ import annotations

from flask import Blueprint, redirect, url_for


weichengnianren_bp = Blueprint("weichengnianren", __name__)


@weichengnianren_bp.route("/")
def index() -> str:
    return redirect(url_for("wcnr_9lbq.index"))

