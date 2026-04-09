from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List

from flask import Response, jsonify, render_template, request, send_file, session

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp
from jszahzyj.service.jszahz_topic_relation_service import (
    RELATION_COLUMN_TYPES,
    build_relation_count_payload,
)
from jszahzyj.service.jszahz_topic_service import (
    defaults_payload,
    export_detail_xlsx,
    export_summary_xlsx,
    import_jszahz_topic_excel,
    query_detail_payload,
    query_summary_payload,
)


def _parse_csv_values(value: str) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


@jszahzyj_bp.route("/api/jszahzztk/defaults", methods=["GET"])
def api_jszahzztk_defaults() -> Response:
    try:
        return jsonify(defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/api/jszahzztk/upload", methods=["POST"])
def api_jszahzztk_upload() -> Response:
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"success": False, "message": "请先选择 Excel 文件"}), 400
    try:
        payload = import_jszahz_topic_excel(
            file_obj=file.stream,
            filename=file.filename,
            created_by=str(session.get("username") or ""),
        )
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/api/jszahzztk/query", methods=["POST"])
def api_jszahzztk_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        return jsonify(
            query_summary_payload(
                start_time=str(payload.get("start_time") or "").strip(),
                end_time=str(payload.get("end_time") or "").strip(),
                branch_codes=payload.get("branch_codes") or [],
                person_types=payload.get("person_types") or [],
                risk_labels=payload.get("risk_labels") or [],
            )
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/download/jszahzztk", methods=["GET"])
def download_jszahzztk() -> Response:
    try:
        data, filename = export_summary_xlsx(
            start_time=str(request.args.get("start_time") or "").strip(),
            end_time=str(request.args.get("end_time") or "").strip(),
            branch_codes=_parse_csv_values(str(request.args.get("branch_codes") or "")),
            person_types=_parse_csv_values(str(request.args.get("person_types") or "")),
            risk_labels=_parse_csv_values(str(request.args.get("risk_labels") or "")),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    buffer = BytesIO(data)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@jszahzyj_bp.route("/jszahzztk/detail_page", methods=["GET"])
def jszahzztk_detail_page() -> Response:
    branch_code = str(request.args.get("branch_code") or "").strip()
    branch_name = str(request.args.get("branch_name") or "").strip() or "汇总"
    start_time = str(request.args.get("start_time") or "").strip()
    end_time = str(request.args.get("end_time") or "").strip()
    person_types = _parse_csv_values(str(request.args.get("person_types") or ""))
    risk_labels = _parse_csv_values(str(request.args.get("risk_labels") or ""))
    try:
        payload = query_detail_payload(
            branch_code=branch_code,
            start_time=start_time,
            end_time=end_time,
            person_types=person_types,
            risk_labels=risk_labels,
            include_relation_counts=False,
        )
    except ValueError as exc:
        return Response(str(exc), status=400)
    except Exception as exc:
        return Response(str(exc), status=500)

    return render_template(
        "jszahz_topic_detail.html",
        title=f"{branch_name} - 精神患者主题库明细",
        branch_code=branch_code or "__ALL__",
        branch_name=branch_name,
        filters=payload["filters"],
        records=payload["records"],
        message=payload["message"],
        relation_column_types=RELATION_COLUMN_TYPES,
    )


@jszahzyj_bp.route("/api/jszahzztk/detail_relation_counts", methods=["POST"])
def api_jszahzztk_detail_relation_counts() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        zjhms = payload.get("zjhms") or []
        return jsonify(
            {
                "success": True,
                "counts": build_relation_count_payload(list(zjhms)),
            }
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/download/jszahzztk/detail", methods=["GET"])
def download_jszahzztk_detail() -> Response:
    branch_code = str(request.args.get("branch_code") or "").strip() or "__ALL__"
    try:
        data, filename = export_detail_xlsx(
            branch_code=branch_code,
            start_time=str(request.args.get("start_time") or "").strip(),
            end_time=str(request.args.get("end_time") or "").strip(),
            person_types=_parse_csv_values(str(request.args.get("person_types") or "")),
            risk_labels=_parse_csv_values(str(request.args.get("risk_labels") or "")),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500

    buffer = BytesIO(data)
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
