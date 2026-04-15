from __future__ import annotations

import json
from io import BytesIO
from typing import Any, Dict, List

from flask import Response, jsonify, render_template, request, send_file, session, stream_with_context

from jszahzyj.routes.jszahzyj_routes import jszahzyj_bp
from jszahzyj.service.jszahz_topic_relation_service import (
    RELATION_COLUMN_TYPES,
    build_relation_count_payload,
)
from jszahzyj.service.jszahz_topic_service import (
    UPLOAD_API_VERSION,
    defaults_payload,
    export_detail_xlsx,
    export_summary_xlsx,
    import_jszahz_base_excel_stream,
    import_jszahz_tag_excel_stream,
    import_jszahz_topic_excel_stream,
    query_detail_page_payload,
    query_summary_payload,
)


def _parse_csv_values(value: str) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _parse_request_values(name: str) -> List[str]:
    values = request.args.getlist(name)
    if len(values) == 1 and "," in str(values[0]):
        return _parse_csv_values(str(values[0]))
    return [str(item).strip() for item in values if str(item).strip()]


def _parse_bool_value(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in ("0", "false", "off", "no"):
        return False
    if text in ("1", "true", "on", "yes"):
        return True
    return default


def _stream_upload_response(file, *, import_func) -> Response:
    if not file or not file.filename:
        return jsonify({"success": False, "message": "请先选择 Excel 文件"}), 400

    file_bytes = file.stream.read()
    filename = file.filename
    created_by = str(session.get("username") or "")

    def generate():
        try:
            for event in import_func(
                file_bytes=file_bytes,
                filename=filename,
                created_by=created_by,
            ):
                yield json.dumps(event, ensure_ascii=False) + "\n"
        except ValueError as exc:
            yield json.dumps(
                {"success": False, "message": str(exc), "api_version": UPLOAD_API_VERSION},
                ensure_ascii=False,
            ) + "\n"
        except Exception as exc:
            yield json.dumps(
                {"success": False, "message": str(exc), "api_version": UPLOAD_API_VERSION},
                ensure_ascii=False,
            ) + "\n"

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "X-JSZAHZ-Upload-Version": UPLOAD_API_VERSION,
        },
    )


@jszahzyj_bp.route("/api/jszahzztk/defaults", methods=["GET"])
def api_jszahzztk_defaults() -> Response:
    try:
        return jsonify(defaults_payload())
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/api/jszahzztk/upload", methods=["POST"])
def api_jszahzztk_upload() -> Response:
    file = request.files.get("file")
    return _stream_upload_response(file, import_func=import_jszahz_topic_excel_stream)


@jszahzyj_bp.route("/api/jszahzztk/upload_base", methods=["POST"])
def api_jszahzztk_upload_base() -> Response:
    return _stream_upload_response(request.files.get("file"), import_func=import_jszahz_base_excel_stream)


@jszahzyj_bp.route("/api/jszahzztk/upload_tags", methods=["POST"])
def api_jszahzztk_upload_tags() -> Response:
    return _stream_upload_response(request.files.get("file"), import_func=import_jszahz_tag_excel_stream)


@jszahzyj_bp.route("/api/jszahzztk/query", methods=["POST"])
def api_jszahzztk_query() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        return jsonify(
            query_summary_payload(
                branch_codes=payload.get("branch_codes") or [],
                person_types=payload.get("person_types") or [],
                risk_labels=payload.get("risk_labels") or [],
                managed_only=payload.get("managed_only"),
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
            branch_codes=_parse_csv_values(str(request.args.get("branch_codes") or "")),
            person_types=_parse_csv_values(str(request.args.get("person_types") or "")),
            risk_labels=_parse_csv_values(str(request.args.get("risk_labels") or "")),
            managed_only=_parse_bool_value(request.args.get("managed_only"), default=True),
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
    person_types = _parse_csv_values(str(request.args.get("person_types") or ""))
    risk_labels = _parse_csv_values(str(request.args.get("risk_labels") or ""))
    relation_types = _parse_request_values("relation_types")
    try:
        payload = query_detail_page_payload(
            branch_code=branch_code,
            person_types=person_types,
            risk_labels=risk_labels,
            managed_only=_parse_bool_value(request.args.get("managed_only"), default=True),
            relation_types=relation_types,
            page=request.args.get("page", "1"),
            page_size=request.args.get("page_size", "20"),
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
        count=payload["count"],
        page=payload["page"],
        page_size=payload["page_size"],
        page_size_value=payload["page_size_value"],
        page_size_label=payload["page_size_label"],
        page_record_count=payload["page_record_count"],
        total_pages=payload["total_pages"],
        page_numbers=payload["page_numbers"],
        has_previous=payload["has_previous"],
        has_next=payload["has_next"],
        previous_page=payload["previous_page"],
        next_page=payload["next_page"],
        page_size_options=payload["page_size_options"],
        relation_type_options=payload["relation_type_options"],
        selected_relation_types=payload["selected_relation_types"],
        visible_relation_columns=payload["visible_relation_columns"],
        relation_column_types=RELATION_COLUMN_TYPES,
    )


@jszahzyj_bp.route("/api/jszahzztk/detail_relation_counts", methods=["POST"])
def api_jszahzztk_detail_relation_counts() -> Response:
    payload: Dict[str, Any] = request.json or {}
    try:
        zjhms = payload.get("zjhms") or []
        counts = build_relation_count_payload(
            list(zjhms),
            relation_types=payload.get("relation_types") or [],
        )
        return jsonify(
            {
                "success": True,
                "counts": counts,
            }
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 500


@jszahzyj_bp.route("/download/jszahzztk/detail", methods=["GET"])
def download_jszahzztk_detail() -> Response:
    branch_code = str(request.args.get("branch_code") or "").strip() or "__ALL__"
    branch_name = str(request.args.get("branch_name") or "").strip() or "汇总"
    try:
        data, filename = export_detail_xlsx(
            branch_code=branch_code,
            branch_name=branch_name,
            person_types=_parse_csv_values(str(request.args.get("person_types") or "")),
            risk_labels=_parse_csv_values(str(request.args.get("risk_labels") or "")),
            managed_only=_parse_bool_value(request.args.get("managed_only"), default=True),
            relation_types=_parse_request_values("relation_types"),
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
