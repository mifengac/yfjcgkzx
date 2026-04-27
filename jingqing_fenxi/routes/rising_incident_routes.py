from __future__ import annotations

from flask import Response, jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.rising_incident_service import (
    build_export_filename,
    generate_rising_incident_excel,
    run_rising_incident_analysis,
)


def _get_list(source, name: str) -> list[str]:
    values = source.getlist(name)
    if not values and name.endswith("[]"):
        values = source.getlist(name[:-2])
    if not values and not name.endswith("[]"):
        values = source.getlist(name + "[]")
    return [str(item).strip() for item in values if str(item).strip()]


def _collect_params() -> dict:
    source = request.args if request.method == "GET" else request.form
    return {
        "beginDate": source.get("beginDate", ""),
        "endDate": source.get("endDate", ""),
        "caseTypeSource": source.get("caseTypeSource", "nature"),
        "caseTypeIds": _get_list(source, "caseTypeIds[]"),
        "newOriCharaSubclassNo": source.get("newOriCharaSubclassNo", ""),
        "newOriCharaSubclass": source.get("newOriCharaSubclass", ""),
        "periodType": source.get("periodType", "business_week"),
        "minPeriods": source.get("minPeriods", "3"),
        "currentOnly": source.get("currentOnly", "1"),
    }


@jingqing_fenxi_bp.route("/api/rising-incident/analyze", methods=["POST"])
def api_rising_incident_analyze() -> Response:
    try:
        result = run_rising_incident_analysis(_collect_params())
        return jsonify({"code": 0, "data": result})
    except ValueError as exc:
        return jsonify({"code": 1, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"code": 1, "message": str(exc)}), 500


@jingqing_fenxi_bp.route("/download/rising-incident", methods=["GET"])
def download_rising_incident() -> Response:
    try:
        result = run_rising_incident_analysis(_collect_params())
        export_file = generate_rising_incident_excel(result)
        return send_file(
            export_file,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=build_export_filename(result.get("meta") or {}),
        )
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"success": False, "message": str(exc)}), 500
