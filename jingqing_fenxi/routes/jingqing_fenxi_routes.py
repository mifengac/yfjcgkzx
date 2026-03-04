from flask import Blueprint, render_template, request, jsonify, send_file
import urllib.parse
import logging
import uuid
import re

from jingqing_fenxi.service.jingqing_api_client import api_client
from jingqing_fenxi.service.jingqing_fenxi_service import (
    fetch_all_case_list, fetch_srr_list,
    calc_time_period, calc_duty_dept,
    calc_repeat_phone, calc_50m_cluster,
    generate_excel_report
)

jingqing_fenxi_bp = Blueprint('jingqing_fenxi', __name__, template_folder='../templates', static_folder='../static')
logger = logging.getLogger(__name__)

@jingqing_fenxi_bp.route('/', methods=['GET'])
def index():
    return render_template("jingqing_fenxi_index.html")

@jingqing_fenxi_bp.route('/treeData', methods=['GET'])
def tree_data():
    """Proxy to get tree data"""
    data = api_client.get_tree_view_data()
    return jsonify(data)

def _build_base_payload(form):
    begin_date = _normalize_datetime(form.get("beginDate", ""))
    end_date = _normalize_datetime(form.get("endDate", ""))
    return {
        "beginDate": begin_date,
        "endDate": end_date,
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

def _normalize_csv(value):
    tokens = [t.strip() for t in str(value or "").split(",") if t and t.strip()]
    seen = set()
    out = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return ",".join(out)

def _normalize_datetime(value):
    val = str(value or "").strip().replace("T", " ")
    if not val:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        return val + " 00:00:00"
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$", val):
        return val + ":00"
    return val

def _build_chara_no_from_case_type_ids(case_type_ids):
    ids = [str(x).strip() for x in (case_type_ids or []) if str(x).strip()]
    if not ids:
        return ""
    id_set = set(ids)
    tree_data = api_client.get_tree_view_data() or []
    tags = []
    seen = set()
    for node in tree_data:
        pid = str(node.get("pId") or "").strip()
        tag = str(node.get("tag") or "").strip()
        if pid and pid in id_set and tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return ",".join(tags)

def _build_srr_payload(form):
    selected_ids = form.getlist("caseTypeIds[]")
    chara_no = _build_chara_no_from_case_type_ids(selected_ids)
    if not chara_no:
        chara_no = _normalize_csv(form.get("newOriCharaSubclassNo", ""))
    chara_name = _normalize_csv(form.get("newOriCharaSubclass", ""))
    start_time = _normalize_datetime(form.get("beginDate", ""))
    end_time = _normalize_datetime(form.get("endDate", ""))
    y2y_start_time = _normalize_datetime(form.get("y2yStartTime", ""))
    y2y_end_time = _normalize_datetime(form.get("y2yEndTime", ""))
    m2m_start_time = _normalize_datetime(form.get("m2mStartTime", ""))
    m2m_end_time = _normalize_datetime(form.get("m2mEndTime", ""))
    return {
        "params[startTime]": start_time,
        "params[endTime]": end_time,
        "params[y2yStartTime]": y2y_start_time,
        "params[y2yEndTime]": y2y_end_time,
        "params[m2mStartTime]": m2m_start_time,
        "params[m2mEndTime]": m2m_end_time,
        "charaNo": chara_no,
        "chara": chara_name,
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

def _run_analysis(form, dimensions_selected, trace_id=None):
    results = {}
    all_data = []
    trace = trace_id or "-"

    chara_no = _normalize_csv(form.get("newOriCharaSubclassNo", ""))
    logger.info(
        "[trace:%s] analyze form begin=%s end=%s dims=%s charaNoLen=%s charaNoHead=%s",
        trace,
        form.get("beginDate", ""),
        form.get("endDate", ""),
        list(dimensions_selected or []),
        len(chara_no),
        chara_no[:100],
    )

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
        logger.info(
            "[trace:%s] srr payload charaNoLen=%s y2yStart=%s y2yEnd=%s m2mStart=%s m2mEnd=%s",
            trace,
            len(str(srr_payload.get("charaNo", ""))),
            srr_payload.get("params[y2yStartTime]", ""),
            srr_payload.get("params[y2yEndTime]", ""),
            srr_payload.get("params[m2mStartTime]", ""),
            srr_payload.get("params[m2mEndTime]", ""),
        )
        srr_res = fetch_srr_list(srr_payload, trace_id=trace)
        logger.info(
            "[trace:%s] SRR analyze result: code=%s total=%s rows=%s",
            trace,
            srr_res.get('code'),
            srr_res.get('total'),
            len(srr_res.get('rows', []))
        )
        if srr_res.get("code") == 0:
            results["srr"] = srr_res.get("rows", [])
        else:
            results["srr"] = []
            results["srr_error"] = {
                "upstream_code": srr_res.get("code", -1),
                "message": srr_res.get("msg") or "上游接口异常",
                "trace_id": trace,
            }

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
    trace_id = uuid.uuid4().hex[:12]
    
    results, _ = _run_analysis(form, dimensions, trace_id=trace_id)
    return jsonify({"code": 0, "data": results, "trace_id": trace_id})

@jingqing_fenxi_bp.route('/export', methods=['POST'])
def export_report():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    trace_id = uuid.uuid4().hex[:12]
    
    results, all_data = _run_analysis(form, dimensions, trace_id=trace_id)
    
    excel_file = generate_excel_report(results, all_data, dimensions)
    
    encoded_name = urllib.parse.quote("警情分析报表.xlsx")
    return send_file(
        excel_file,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="警情分析报表.xlsx"
    )
