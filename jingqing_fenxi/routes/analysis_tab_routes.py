from flask import jsonify, request, send_file

from jingqing_fenxi.routes.jingqing_fenxi_routes import jingqing_fenxi_bp
from jingqing_fenxi.service.analysis_tab_service import (
    generate_excel_report,
    get_tree_view_data,
    run_analysis,
)


@jingqing_fenxi_bp.route('/treeData', methods=['GET'])
def tree_data():
    return jsonify(get_tree_view_data())


@jingqing_fenxi_bp.route('/analyze', methods=['POST'])
def analyze():
    form = request.form
    dimensions = form.getlist('dimensions[]')

    results, analysis_base, _, analysis_options = run_analysis(form, dimensions)
    return jsonify({
        "code": 0,
        "data": results,
        "analysisBase": analysis_base,
        "analysisOptions": analysis_options,
    })


@jingqing_fenxi_bp.route('/export', methods=['POST'])
def export_report():
    form = request.form
    dimensions = form.getlist('dimensions[]')

    results, _, all_data, analysis_options = run_analysis(form, dimensions)
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
