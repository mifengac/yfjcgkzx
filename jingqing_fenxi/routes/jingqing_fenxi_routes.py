from flask import Blueprint, render_template, request, jsonify, send_file
import urllib.parse
import logging
import uuid
import re
from datetime import datetime

from jingqing_fenxi.service.jingqing_api_client import api_client
from jingqing_fenxi.service.jingqing_fenxi_service import (
    fetch_all_case_list, fetch_srr_list,
    calc_time_period, calc_duty_dept,
    calc_repeat_phone, calc_repeat_address,
    generate_excel_report, calc_time_hourly_counts
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

def _parse_int(value, default=None):
    try:
        return int(str(value).strip())
    except Exception:
        return default

def _clamp(value, minimum, maximum):
    return max(minimum, min(value, maximum))

def _build_analysis_options(form):
    valid_time_buckets = [1, 2, 3, 4, 6, 8, 12]

    time_bucket_hours = _parse_int(form.get("timeBucketHours"), 3)
    if time_bucket_hours not in valid_time_buckets:
        time_bucket_hours = 3

    dept_top_n_raw = str(form.get("deptTopN", "all")).strip().lower()
    if dept_top_n_raw in ("", "all", "0"):
        dept_top_n = None
    else:
        dept_top_n = _parse_int(dept_top_n_raw, None)
        if not dept_top_n or dept_top_n < 1:
            dept_top_n = None

    repeat_phone_min_count = _parse_int(form.get("repeatPhoneMinCount"), 2)
    if repeat_phone_min_count is None:
        repeat_phone_min_count = 2
    repeat_phone_min_count = _clamp(repeat_phone_min_count, 2, 10)

    repeat_addr_radius_meters = _parse_int(form.get("repeatAddrRadiusMeters"), 50)
    if repeat_addr_radius_meters is None:
        repeat_addr_radius_meters = 50
    repeat_addr_radius_meters = _clamp(repeat_addr_radius_meters, 50, 500)
    repeat_addr_radius_meters = int(round(repeat_addr_radius_meters / 50.0) * 50)
    repeat_addr_radius_meters = _clamp(repeat_addr_radius_meters, 50, 500)

    return {
        "timeBucketHours": time_bucket_hours,
        "deptTopN": dept_top_n,
        "repeatPhoneMinCount": repeat_phone_min_count,
        "repeatAddrRadiusMeters": repeat_addr_radius_meters,
    }

def _parse_datetime(value):
    val = _normalize_datetime(value)
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def _format_datetime(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _shift_year_safe(dt, years):
    target_year = dt.year + years
    try:
        return dt.replace(year=target_year)
    except ValueError:
        # Leap day fallback: 2024-02-29 -> 2023-02-28
        return dt.replace(year=target_year, month=2, day=28)

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
    start_dt = _parse_datetime(start_time)
    end_dt = _parse_datetime(end_time)

    y2y_start_time = _normalize_datetime(form.get("y2yStartTime", ""))
    y2y_end_time = _normalize_datetime(form.get("y2yEndTime", ""))
    m2m_start_time = _normalize_datetime(form.get("m2mStartTime", ""))
    m2m_end_time = _normalize_datetime(form.get("m2mEndTime", ""))

    # Y2Y default: one year before begin/end
    if (not _parse_datetime(y2y_start_time)) and start_dt:
        y2y_start_time = _format_datetime(_shift_year_safe(start_dt, -1))
    if (not _parse_datetime(y2y_end_time)) and end_dt:
        y2y_end_time = _format_datetime(_shift_year_safe(end_dt, -1))

    # M2M default: [begin - (end-begin), begin]
    if start_dt and end_dt:
        duration = end_dt - start_dt
        default_m2m_end = start_dt
        default_m2m_start = default_m2m_end - duration
        if not _parse_datetime(m2m_end_time):
            m2m_end_time = _format_datetime(default_m2m_end)
        if not _parse_datetime(m2m_start_time):
            m2m_start_time = _format_datetime(default_m2m_start)

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
    analysis_base = {}
    all_data = []
    trace = trace_id or "-"
    analysis_options = _build_analysis_options(form)

    chara_no = _normalize_csv(form.get("newOriCharaSubclassNo", ""))
    logger.info(
        "[trace:%s] analyze form begin=%s end=%s dims=%s charaNoLen=%s charaNoHead=%s options=%s",
        trace,
        form.get("beginDate", ""),
        form.get("endDate", ""),
        list(dimensions_selected or []),
        len(chara_no),
        chara_no[:100],
        analysis_options,
    )

    # Requires case data for any of the local dimension calculations
    requires_case_data = any(d in dimensions_selected for d in ["time", "dept", "phone", "cluster"])
    
    if requires_case_data:
        base_payload = _build_base_payload(form)
        all_data = fetch_all_case_list(base_payload)
        
        if "time" in dimensions_selected:
            analysis_base["timeHourly"] = calc_time_hourly_counts(all_data)
            results["time"] = calc_time_period(
                all_data, bucket_hours=analysis_options["timeBucketHours"]
            )
        if "dept" in dimensions_selected:
            analysis_base["deptAll"] = calc_duty_dept(all_data, top_n=None)
            results["dept"] = calc_duty_dept(
                all_data, top_n=analysis_options["deptTopN"]
            )
        if "phone" in dimensions_selected:
            analysis_base["phoneAll"] = calc_repeat_phone(all_data, min_count=2)
            results["phone"] = calc_repeat_phone(
                all_data, min_count=analysis_options["repeatPhoneMinCount"]
            )
        if "cluster" in dimensions_selected:
            results["cluster"] = calc_repeat_address(
                all_data, radius_meters=analysis_options["repeatAddrRadiusMeters"]
            )
            
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

    return results, analysis_base, all_data, analysis_options

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
    
    results, analysis_base, _, analysis_options = _run_analysis(form, dimensions, trace_id=trace_id)
    return jsonify({
        "code": 0,
        "data": results,
        "analysisBase": analysis_base,
        "analysisOptions": analysis_options,
        "trace_id": trace_id
    })

@jingqing_fenxi_bp.route('/export', methods=['POST'])
def export_report():
    form = request.form
    dimensions = form.getlist('dimensions[]')
    trace_id = uuid.uuid4().hex[:12]
    
    results, _, all_data, analysis_options = _run_analysis(form, dimensions, trace_id=trace_id)
    
    excel_file = generate_excel_report(
        results, all_data, dimensions, analysis_options=analysis_options
    )
    
    encoded_name = urllib.parse.quote("警情分析报表.xlsx")
    return send_file(
        excel_file,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="警情分析报表.xlsx"
    )
