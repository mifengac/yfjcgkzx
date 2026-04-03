from __future__ import annotations

import uuid

from flask import Response, jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.fight_topic_service import (
    build_export_filename,
    generate_fight_topic_excel,
    run_fight_topic_analysis,
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
    }


@jingqing_fenxi_bp.route("/api/fight-topic/analyze", methods=["POST"])
def api_fight_topic_analyze() -> Response:
    params = _collect_params()
    dimensions = _get_dimensions_from_request()
    trace_id = uuid.uuid4().hex[:12]
    try:
        results, analysis_base, _all_data, analysis_options, _meta = run_fight_topic_analysis(
            params,
            dimensions,
            trace_id=trace_id,
        )
        return jsonify(
            {
                "code": 0,
                "data": results,
                "analysisBase": analysis_base,
                "analysisOptions": analysis_options,
                "trace_id": trace_id,
            }
        )
    except ValueError as exc:
        return jsonify({"code": 1, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"code": 1, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/fight-topic", methods=["GET"])
def download_fight_topic() -> Response:
    params = _collect_params()
    dimensions = _get_dimensions_from_request()
    trace_id = uuid.uuid4().hex[:12]
    try:
        results, _analysis_base, all_data, analysis_options, meta = run_fight_topic_analysis(
            params,
            dimensions,
            trace_id=trace_id,
            include_detail_rows=True,
        )
        export_file = generate_fight_topic_excel(
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
