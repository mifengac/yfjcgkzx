import logging
import uuid

from flask import jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.analysis_tab_service import (
    build_debug_srr_payload,
    generate_excel_report,
    get_srr_debug_result,
    get_tree_view_data,
    run_analysis,
)


logger = logging.getLogger(__name__)


@jingqing_fenxi_bp.route('/treeData', methods=['GET'])
def tree_data():
    return jsonify(get_tree_view_data())


@jingqing_fenxi_bp.route('/debug_srr', methods=['GET'])
def debug_srr():
    payload = build_debug_srr_payload()
    result = get_srr_debug_result(payload)
    return jsonify({"payload_sample": {k: v for k, v in list(payload.items())[:5]}, "result": result})


@jingqing_fenxi_bp.route('/analyze', methods=['POST'])
def analyze():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    trace_id = uuid.uuid4().hex[:12]

    results, analysis_base, _, analysis_options = run_analysis(form, dimensions, trace_id=trace_id)
    return jsonify({
        "code": 0,
        "data": results,
        "analysisBase": analysis_base,
        "analysisOptions": analysis_options,
        "trace_id": trace_id,
    })


@jingqing_fenxi_bp.route('/export', methods=['POST'])
def export_report():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    trace_id = uuid.uuid4().hex[:12]

    results, _, all_data, analysis_options = run_analysis(form, dimensions, trace_id=trace_id)
    excel_file = generate_excel_report(
        results,
        all_data,
        dimensions,
        analysis_options=analysis_options,
        begin_date=str(form.get('beginDate', '') or '').replace('T', ' '),
        end_date=str(form.get('endDate', '') or '').replace('T', ' '),
    )
    return send_file(
        excel_file,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="警情分析报表.xlsx",
    )
