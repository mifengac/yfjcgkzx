from flask import Blueprint, render_template, request, jsonify, send_file
import urllib.parse

from jingqing_fenxi.service.jingqing_api_client import api_client
from jingqing_fenxi.service.jingqing_fenxi_service import (
    fetch_all_case_list, fetch_srr_list,
    calc_time_period, calc_duty_dept,
    calc_repeat_phone, calc_50m_cluster,
    generate_excel_report
)

jingqing_fenxi_bp = Blueprint('jingqing_fenxi', __name__, template_folder='../templates', static_folder='../static')

@jingqing_fenxi_bp.route('/', methods=['GET'])
def index():
    return render_template("jingqing_fenxi_index.html")

@jingqing_fenxi_bp.route('/treeData', methods=['GET'])
def tree_data():
    """Proxy to get tree data"""
    data = api_client.get_tree_view_data()
    return jsonify(data)

def _build_base_payload(form):
    return {
        "beginDate": form.get("beginDate", ""),
        "endDate": form.get("endDate", ""),
        "newOriCharaSubclassNo": form.get("newOriCharaSubclassNo", ""),
        "newOriCharaSubclass": form.get("newOriCharaSubclass", ""),
        # default fixed parameters
        "newCaseSource": "全部",
        "dutyDeptName": "全部",
        "newCharaSubclass": "全部",
        "caseMark": "全部",
        "pageSize": "15",
        "pageNum": "1",
        "orderByColumn": "callTime",
        "isAsc": "desc",
    }

def _build_srr_payload(form):
    return {
        "params[startTime]": form.get("beginDate", ""),
        "params[endTime]": form.get("endDate", ""),
        "params[y2yStartTime]": form.get("y2yStartTime", ""),
        "params[y2yEndTime]": form.get("y2yEndTime", ""),
        "params[m2mStartTime]": form.get("m2mStartTime", ""),
        "params[m2mEndTime]": form.get("m2mEndTime", ""),
        "charaNo": form.get("newOriCharaSubclassNo", ""),
        "chara": form.get("newOriCharaSubclass", ""),
        "groupField": "duty_dept_no",
        "charaType": "chara_ori",
        "charaLevel": "1",
        "caseLevel": "",
        "dutyDeptNo": "",
        "dutyDeptName": "全部",
        "newRecvType": "",
        "newRecvTypeName": "全部",
        "newCaseSourceNo": "",
        "newCaseSource": "全部",
        "params[searchAnd]": "",
        "params[searchOr]": "",
        "params[searchNot]": "",
        "caseContents": "on",
        "replies": "on",
        "pageNum": "NaN",
        "orderByColumn": "",
        "isAsc": "asc"
    }

def _run_analysis(form, dimensions_selected):
    results = {}
    all_data = []

    # Requires case data for any of the local dimension calculations
    requires_case_data = any(d in dimensions_selected for d in ["time", "dept", "phone", "cluster"])
    
    if requires_case_data:
        base_payload = _build_base_payload(form)
        all_data = fetch_all_case_list(base_payload)
        
        if "time" in dimensions_selected:
            results["time"] = calc_time_period(all_data)
        if "dept" in dimensions_selected:
            results["dept"] = calc_duty_dept(all_data)
        if "phone" in dimensions_selected:
            results["phone"] = calc_repeat_phone(all_data)
        if "cluster" in dimensions_selected:
            results["cluster"] = calc_50m_cluster(all_data)
            
    if "srr" in dimensions_selected:
        srr_payload = _build_srr_payload(form)
        srr_res = fetch_srr_list(srr_payload)
        import logging
        logging.getLogger(__name__).info("SRR analyze result: code=%s rows=%s",
                                         srr_res.get('code'), len(srr_res.get('rows', [])))
        results["srr"] = srr_res.get("rows", [])
        
    return results, all_data

@jingqing_fenxi_bp.route('/debug_srr', methods=['GET'])
def debug_srr():
    """Debug endpoint: call SRR API with hardcoded params, return raw result"""
    import datetime
    now = datetime.datetime.now()
    start = (now - datetime.timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    end = now.strftime('%Y-%m-%d 23:59:59')
    m2m_start = (now - datetime.timedelta(days=14)).strftime('%Y-%m-%d 00:00:00')
    m2m_end = (now - datetime.timedelta(days=7)).strftime('%Y-%m-%d 23:59:59')
    y2y_start = (now.replace(year=now.year-1) - datetime.timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    y2y_end = now.replace(year=now.year-1).strftime('%Y-%m-%d 23:59:59')
    test_payload = {
        "params[startTime]": start,
        "params[endTime]": end,
        "groupField": "duty_dept_no",
        "caseLevel": "",
        "charaNo": "",
        "chara": "",
        "charaType": "chara_ori",
        "charaLevel": "1",
        "params[y2yStartTime]": y2y_start,
        "params[y2yEndTime]": y2y_end,
        "dutyDeptNo": "",
        "dutyDeptName": "全部",
        "newRecvType": "",
        "newRecvTypeName": "全部",
        "newCaseSourceNo": "",
        "newCaseSource": "全部",
        "params[m2mStartTime]": m2m_start,
        "params[m2mEndTime]": m2m_end,
        "params[searchAnd]": "",
        "params[searchOr]": "",
        "params[searchNot]": "",
        "caseContents": "on",
        "replies": "on",
        "pageNum": "NaN",
        "orderByColumn": "",
        "isAsc": "asc"
    }
    result = api_client.get_srr_list(test_payload)
    return jsonify({"payload_sample": {k: v for k, v in list(test_payload.items())[:5]}, "result": result})

@jingqing_fenxi_bp.route('/analyze', methods=['POST'])
def analyze():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    
    results, _ = _run_analysis(form, dimensions)
    return jsonify({"code": 0, "data": results})

@jingqing_fenxi_bp.route('/export', methods=['POST'])
def export_report():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    
    results, all_data = _run_analysis(form, dimensions)
    
    excel_file = generate_excel_report(results, all_data, dimensions)
    
    encoded_name = urllib.parse.quote("警情分析报表.xlsx")
    return send_file(
        excel_file,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="警情分析报表.xlsx"
    )
