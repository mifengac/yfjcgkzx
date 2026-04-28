from __future__ import annotations

from flask import Response, jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.gambling_analysis_export_service import (
    build_gambling_analysis_export_filename,
    generate_gambling_analysis_export,
)
from jingqing_fenxi.service.gambling_report_code_convert_service import (
    build_code_convert_filename,
    convert_markdown_station_codes_to_docx,
)
from jingqing_fenxi.service.gambling_topic_service import (
    build_export_filename,
    generate_gambling_topic_excel,
    run_gambling_topic_analysis,
)


def _get_dimensions_from_request() -> list[str]:
    if request.method == "GET":
        return request.args.getlist("dimensions[]") or request.args.getlist("dimensions")
    return request.form.getlist("dimensions[]") or request.form.getlist("dimensions")


def _collect_params() -> dict[str, str]:
    source = request.args if request.method == "GET" else request.form
    return {
        "beginDate": source.get("beginDate", ""),
        "endDate": source.get("endDate", ""),
        "m2mStartTime": source.get("m2mStartTime", ""),
        "m2mEndTime": source.get("m2mEndTime", ""),
        "timeBucketHours": source.get("timeBucketHours", ""),
        "deptTopN": source.get("deptTopN", ""),
        "repeatPhoneMinCount": source.get("repeatPhoneMinCount", ""),
        "repeatAddrRadiusMeters": source.get("repeatAddrRadiusMeters", ""),
        "desensitized": source.get("desensitized", "1"),
    }


@jingqing_fenxi_bp.route("/api/gambling-topic/analyze", methods=["POST"])
def api_gambling_topic_analyze() -> Response:
    params = _collect_params()
    dimensions = _get_dimensions_from_request()
    try:
        results, analysis_base, _all_data, analysis_options, _meta = run_gambling_topic_analysis(
            params,
            dimensions,
        )
        return jsonify(
            {
                "code": 0,
                "data": results,
                "analysisBase": analysis_base,
                "analysisOptions": analysis_options,
            }
        )
    except ValueError as exc:
        return jsonify({"code": 1, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"code": 1, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/gambling-topic", methods=["GET"])
def download_gambling_topic() -> Response:
    params = _collect_params()
    dimensions = _get_dimensions_from_request()
    try:
        results, _analysis_base, all_data, analysis_options, meta = run_gambling_topic_analysis(
            params,
            dimensions,
            include_detail_rows=True,
        )
        export_file = generate_gambling_topic_excel(
            results,
            all_data,
            dimensions,
            begin_date=meta["beginDate"],
            end_date=meta["endDate"],
            analysis_options=analysis_options,
        )
        return send_file(
            export_file,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=build_export_filename(meta["beginDate"], meta["endDate"]),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/gambling-topic/analysis-data", methods=["GET"])
def download_gambling_analysis_data() -> Response:
    params = _collect_params()
    try:
        export_file = generate_gambling_analysis_export(params)
        begin_date = params.get("beginDate", "")
        end_date = params.get("endDate", "")
        desensitized = str(params.get("desensitized", "1")).lower() not in {"0", "false", "no", "off"}
        return send_file(
            export_file,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=build_gambling_analysis_export_filename(begin_date, end_date, desensitized=desensitized),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"success": False, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/gambling-topic/code-convert", methods=["POST"])
def download_gambling_code_convert() -> Response:
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"success": False, "message": "请上传 markdown 文件"}), 400
    if not upload.filename.lower().endswith(".md"):
        return jsonify({"success": False, "message": "只支持上传 .md 格式文件"}), 400

    try:
        content = upload.read()
        export_file = convert_markdown_station_codes_to_docx(content, upload.filename)
        return send_file(
            export_file,
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            as_attachment=True,
            download_name=build_code_convert_filename(upload.filename),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"success": False, "message": str(exc)}), 500
