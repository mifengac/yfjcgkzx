from __future__ import annotations

from flask import Response, render_template, request

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp
from jszahzyj.service.jszahz_topic_relation_service import build_relation_page_payload


@jszahzyj_bp.route("/jszahzztk/relation_page", methods=["GET"])
def jszahzztk_relation_page() -> Response:
    relation_type = str(request.args.get("relation_type") or "").strip()
    zjhm = str(request.args.get("zjhm") or "").strip()
    xm = str(request.args.get("xm") or "").strip()
    try:
        payload = build_relation_page_payload(
            relation_type=relation_type,
            zjhm=zjhm,
            xm=xm,
        )
    except ValueError as exc:
        return Response(str(exc), status=400)
    except Exception as exc:
        return Response(str(exc), status=500)

    return render_template(
        "jszahz_topic_relation.html",
        title=payload["title"],
        xm=payload["xm"],
        zjhm=payload["zjhm"],
        records=payload["records"],
        message=payload["message"],
    )
