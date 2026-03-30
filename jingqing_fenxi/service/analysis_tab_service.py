import logging
import re
from datetime import datetime, timedelta

from gonggong.service.upstream_jingqing_client import api_client
from jingqing_fenxi.service.jingqing_fenxi_service import (
    calc_duty_dept,
    calc_repeat_address,
    calc_repeat_phone,
    calc_time_hourly_counts,
    calc_time_period,
    fetch_all_case_list,
    fetch_srr_list,
    generate_excel_report,
)


logger = logging.getLogger(__name__)


def get_tree_view_data():
    return api_client.get_tree_view_data()


def _normalize_csv(value):
    tokens = [token.strip() for token in str(value or '').split(',') if token and token.strip()]
    seen = set()
    out = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return ','.join(out)


def _normalize_datetime(value):
    val = str(value or '').strip().replace('T', ' ')
    if not val:
        return ''
    if re.match(r'^\d{4}-\d{2}-\d{2}$', val):
        return val + ' 00:00:00'
    if re.match(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$', val):
        return val + ':00'
    return val


def _parse_int(value, default=None):
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _clamp(value, minimum, maximum):
    return max(minimum, min(value, maximum))


def _build_base_payload(form):
    return {
        'beginDate': _normalize_datetime(form.get('beginDate', '')),
        'endDate': _normalize_datetime(form.get('endDate', '')),
        'newOriCharaSubclassNo': form.get('newOriCharaSubclassNo', ''),
        'newOriCharaSubclass': form.get('newOriCharaSubclass', ''),
        'newCaseSource': '全部',
        'dutyDeptName': '全部',
        'newCharaSubclass': '全部',
        'caseMark': '全部',
        'pageSize': '15',
        'pageNum': '1',
        'orderByColumn': 'callTime',
        'isAsc': 'desc',
    }


def _build_analysis_options(form):
    valid_time_buckets = [1, 2, 3, 4, 6, 8, 12]

    time_bucket_hours = _parse_int(form.get('timeBucketHours'), 3)
    if time_bucket_hours not in valid_time_buckets:
        time_bucket_hours = 3

    dept_top_n_raw = str(form.get('deptTopN', 'all')).strip().lower()
    if dept_top_n_raw in ('', 'all', '0'):
        dept_top_n = None
    else:
        dept_top_n = _parse_int(dept_top_n_raw, None)
        if not dept_top_n or dept_top_n < 1:
            dept_top_n = None

    repeat_phone_min_count = _parse_int(form.get('repeatPhoneMinCount'), 2)
    if repeat_phone_min_count is None:
        repeat_phone_min_count = 2
    repeat_phone_min_count = _clamp(repeat_phone_min_count, 2, 10)

    repeat_addr_radius_meters = _parse_int(form.get('repeatAddrRadiusMeters'), 50)
    if repeat_addr_radius_meters is None:
        repeat_addr_radius_meters = 50
    repeat_addr_radius_meters = _clamp(repeat_addr_radius_meters, 50, 500)
    repeat_addr_radius_meters = int(round(repeat_addr_radius_meters / 50.0) * 50)
    repeat_addr_radius_meters = _clamp(repeat_addr_radius_meters, 50, 500)

    return {
        'timeBucketHours': time_bucket_hours,
        'deptTopN': dept_top_n,
        'repeatPhoneMinCount': repeat_phone_min_count,
        'repeatAddrRadiusMeters': repeat_addr_radius_meters,
    }


def _parse_datetime(value):
    normalized = _normalize_datetime(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def _format_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def _shift_year_safe(dt, years):
    target_year = dt.year + years
    try:
        return dt.replace(year=target_year)
    except ValueError:
        return dt.replace(year=target_year, month=2, day=28)


def _build_chara_no_from_case_type_ids(case_type_ids):
    ids = [str(item).strip() for item in (case_type_ids or []) if str(item).strip()]
    if not ids:
        return ''

    id_set = set(ids)
    tags = []
    seen = set()
    for node in get_tree_view_data() or []:
        parent_id = str(node.get('pId') or '').strip()
        tag = str(node.get('tag') or '').strip()
        if parent_id and parent_id in id_set and tag and tag not in seen:
            seen.add(tag)
            tags.append(tag)
    return ','.join(tags)


def _build_srr_payload(form):
    selected_ids = form.getlist('caseTypeIds[]')
    chara_no = _build_chara_no_from_case_type_ids(selected_ids)
    if not chara_no:
        chara_no = _normalize_csv(form.get('newOriCharaSubclassNo', ''))

    start_time = _normalize_datetime(form.get('beginDate', ''))
    end_time = _normalize_datetime(form.get('endDate', ''))
    start_dt = _parse_datetime(start_time)
    end_dt = _parse_datetime(end_time)

    y2y_start_time = _normalize_datetime(form.get('y2yStartTime', ''))
    y2y_end_time = _normalize_datetime(form.get('y2yEndTime', ''))
    m2m_start_time = _normalize_datetime(form.get('m2mStartTime', ''))
    m2m_end_time = _normalize_datetime(form.get('m2mEndTime', ''))

    if (not _parse_datetime(y2y_start_time)) and start_dt:
        y2y_start_time = _format_datetime(_shift_year_safe(start_dt, -1))
    if (not _parse_datetime(y2y_end_time)) and end_dt:
        y2y_end_time = _format_datetime(_shift_year_safe(end_dt, -1))

    if start_dt and end_dt:
        duration = end_dt - start_dt
        default_m2m_end = start_dt
        default_m2m_start = default_m2m_end - duration
        if not _parse_datetime(m2m_end_time):
            m2m_end_time = _format_datetime(default_m2m_end)
        if not _parse_datetime(m2m_start_time):
            m2m_start_time = _format_datetime(default_m2m_start)

    return {
        'params[startTime]': start_time,
        'params[endTime]': end_time,
        'params[y2yStartTime]': y2y_start_time,
        'params[y2yEndTime]': y2y_end_time,
        'params[m2mStartTime]': m2m_start_time,
        'params[m2mEndTime]': m2m_end_time,
        'charaNo': chara_no,
        'chara': _normalize_csv(form.get('newOriCharaSubclass', '')),
        'groupField': 'duty_dept_no',
        'charaType': 'chara_ori',
        'charaLevel': '1',
        'caseLevel': '',
        'dutyDeptNo': '',
        'dutyDeptName': '全部',
        'newRecvType': '',
        'newRecvTypeName': '全部',
        'newCaseSourceNo': '',
        'newCaseSource': '全部',
        'params[searchAnd]': '',
        'params[searchOr]': '',
        'params[searchNot]': '',
        'caseContents': 'on',
        'replies': 'on',
        'pageNum': 'NaN',
        'orderByColumn': '',
        'isAsc': 'asc',
    }


def build_debug_srr_payload():
    now = datetime.now()
    start = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    end = now.strftime('%Y-%m-%d 23:59:59')
    m2m_start = (now - timedelta(days=14)).strftime('%Y-%m-%d 00:00:00')
    m2m_end = (now - timedelta(days=7)).strftime('%Y-%m-%d 23:59:59')
    y2y_start = (now.replace(year=now.year - 1) - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    y2y_end = now.replace(year=now.year - 1).strftime('%Y-%m-%d 23:59:59')
    return {
        'params[startTime]': start,
        'params[endTime]': end,
        'groupField': 'duty_dept_no',
        'caseLevel': '',
        'charaNo': '',
        'chara': '',
        'charaType': 'chara_ori',
        'charaLevel': '1',
        'params[y2yStartTime]': y2y_start,
        'params[y2yEndTime]': y2y_end,
        'dutyDeptNo': '',
        'dutyDeptName': '全部',
        'newRecvType': '',
        'newRecvTypeName': '全部',
        'newCaseSourceNo': '',
        'newCaseSource': '全部',
        'params[m2mStartTime]': m2m_start,
        'params[m2mEndTime]': m2m_end,
        'params[searchAnd]': '',
        'params[searchOr]': '',
        'params[searchNot]': '',
        'caseContents': 'on',
        'replies': 'on',
        'pageNum': 'NaN',
        'orderByColumn': '',
        'isAsc': 'asc',
    }


def get_srr_debug_result(payload):
    return api_client.get_srr_list(payload)


def run_analysis(form, dimensions_selected, trace_id=None):
    results = {}
    analysis_base = {}
    all_data = []
    trace = trace_id or '-'
    analysis_options = _build_analysis_options(form)

    chara_no = _normalize_csv(form.get('newOriCharaSubclassNo', ''))
    logger.info(
        '[trace:%s] analyze form begin=%s end=%s dims=%s charaNoLen=%s charaNoHead=%s options=%s',
        trace,
        form.get('beginDate', ''),
        form.get('endDate', ''),
        list(dimensions_selected or []),
        len(chara_no),
        chara_no[:100],
        analysis_options,
    )

    requires_case_data = any(dim in dimensions_selected for dim in ['time', 'dept', 'phone', 'cluster'])
    if requires_case_data:
        all_data = fetch_all_case_list(_build_base_payload(form))
        if 'time' in dimensions_selected:
            analysis_base['timeHourly'] = calc_time_hourly_counts(all_data)
            results['time'] = calc_time_period(all_data, bucket_hours=analysis_options['timeBucketHours'])
        if 'dept' in dimensions_selected:
            analysis_base['deptAll'] = calc_duty_dept(all_data, top_n=None)
            results['dept'] = calc_duty_dept(all_data, top_n=analysis_options['deptTopN'])
        if 'phone' in dimensions_selected:
            analysis_base['phoneAll'] = calc_repeat_phone(all_data, min_count=2)
            results['phone'] = calc_repeat_phone(all_data, min_count=analysis_options['repeatPhoneMinCount'])
        if 'cluster' in dimensions_selected:
            results['cluster'] = calc_repeat_address(all_data, radius_meters=analysis_options['repeatAddrRadiusMeters'])

    if 'srr' in dimensions_selected:
        srr_payload = _build_srr_payload(form)
        logger.info(
            '[trace:%s] srr payload charaNoLen=%s y2yStart=%s y2yEnd=%s m2mStart=%s m2mEnd=%s',
            trace,
            len(str(srr_payload.get('charaNo', ''))),
            srr_payload.get('params[y2yStartTime]', ''),
            srr_payload.get('params[y2yEndTime]', ''),
            srr_payload.get('params[m2mStartTime]', ''),
            srr_payload.get('params[m2mEndTime]', ''),
        )
        srr_res = fetch_srr_list(srr_payload, trace_id=trace)
        logger.info(
            '[trace:%s] SRR analyze result: code=%s total=%s rows=%s',
            trace,
            srr_res.get('code'),
            srr_res.get('total'),
            len(srr_res.get('rows', [])),
        )
        if srr_res.get('code') == 0:
            results['srr'] = srr_res.get('rows', [])
        else:
            results['srr'] = []
            results['srr_error'] = {
                'upstream_code': srr_res.get('code', -1),
                'message': srr_res.get('msg') or '上游接口异常',
                'trace_id': trace,
            }

    return results, analysis_base, all_data, analysis_options
