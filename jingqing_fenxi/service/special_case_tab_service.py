from __future__ import annotations

import csv
import io
import logging
import re
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import openpyxl
from openpyxl import Workbook

from jingqing_fenxi.service.jingqing_fenxi_service import fetch_all_case_list


logger = logging.getLogger(__name__)


BRANCH_CMD_ID_MAP = {
    '市局': '445300000000',
    '云城': '445302000000',
    '云安': '445303000000',
    '罗定': '445381000000',
    '新兴': '445321000000',
    '郁南': '445322000000',
}

SPECIAL_CASE_UPSTREAM_PAGE_SIZE = 5000

EXPORT_HEADERS = [
    ('caseNo', '接警号'),
    ('callTime', '报警时间'),
    ('cmdId', '分局编码'),
    ('dutyDeptName', '管辖单位'),
    ('caseLevelName', '警情级别'),
    ('occurAddress', '涉案地址'),
    ('callerName', '报警人'),
    ('callerPhone', '报警人电话'),
    ('caseContents', '简要案情'),
    ('replies', '反馈内容'),
]


def default_time_range() -> Tuple[str, str]:
    now = datetime.now()
    end_dt = datetime(now.year, now.month, now.day, 23, 59, 59)
    start_dt = datetime(now.year, now.month, now.day, 0, 0, 0)
    return start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S')


def normalize_datetime_text(value: str) -> str:
    text = str(value or '').strip().replace('T', ' ')
    if not text:
        return ''
    if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
        return text + ' 00:00:00'
    if re.match(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$', text):
        return text + ':00'
    if not re.match(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$', text):
        raise ValueError('时间格式不正确')
    return text


def normalize_branch_selection(branches: Sequence[str] | None) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for branch in branches or []:
        name = str(branch or '').strip()
        if not name or name not in BRANCH_CMD_ID_MAP or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return normalized


def branch_options() -> List[Dict[str, str]]:
    return [
        {'value': label, 'label': label, 'cmd_id': cmd_id}
        for label, cmd_id in BRANCH_CMD_ID_MAP.items()
    ]


def build_special_case_payload(begin_date: str, end_date: str) -> Dict[str, Any]:
    return {
        'params[colArray]': '',
        'beginDate': begin_date,
        'endDate': end_date,
        'newCaseSourceNo': '',
        'newCaseSource': '全部',
        'dutyDeptNo': '',
        'dutyDeptName': '全部',
        'newCharaSubclassNo': '',
        'newCharaSubclass': '全部',
        'newOriCharaSubclassNo': '',
        'newOriCharaSubclass': '全部',
        'caseNo': '',
        'callerName': '',
        'callerPhone': '',
        'phoneAddress': '',
        'callerIdentity': '',
        'operatorNo': '',
        'operatorName': '',
        'params[isInvalidCase]': '',
        'occurAddress': '',
        'caseMarkNo': '',
        'caseMark': '全部',
        'params[repetitionCase]': '',
        'params[originalDuplicateCase]': '',
        'params[startTimePeriod]': '',
        'params[endTimePeriod]': '',
        'caseContents': '',
        'replies': '',
        'params[sinceRecord]': '',
        'dossierResult': '',
        'params[isVideo]': '',
        'params[isConversation]': '',
        'pageSize': SPECIAL_CASE_UPSTREAM_PAGE_SIZE,
        'pageNum': 1,
        'orderByColumn': 'callTime',
        'isAsc': 'desc',
    }


def fetch_all_special_case_rows(begin_date: str, end_date: str) -> List[Dict[str, Any]]:
    return fetch_all_case_list(
        build_special_case_payload(begin_date, end_date),
        max_page_size=SPECIAL_CASE_UPSTREAM_PAGE_SIZE,
    )


def count_keyword_matches(rows: Iterable[Dict[str, Any]], keywords: Sequence[str]) -> int:
    count = 0
    for row in rows or []:
        case_contents = str(row.get('caseContents') or '')
        replies = str(row.get('replies') or '')
        combined = case_contents + '\n' + replies
        if any(keyword in combined for keyword in keywords):
            count += 1
    return count


def collect_cmd_id_samples(rows: Iterable[Dict[str, Any]], limit: int = 8) -> List[str]:
    samples: List[str] = []
    seen = set()
    for row in rows or []:
        cmd_id = str(row.get('cmdId') or '').strip()
        if not cmd_id or cmd_id in seen:
            continue
        seen.add(cmd_id)
        samples.append(cmd_id)
        if len(samples) >= limit:
            break
    return samples


def collect_keyword_hit_samples(rows: Iterable[Dict[str, Any]], keywords: Sequence[str], limit: int = 5) -> List[Dict[str, str]]:
    samples: List[Dict[str, str]] = []
    for row in rows or []:
        case_contents = str(row.get('caseContents') or '')
        replies = str(row.get('replies') or '')
        combined = (case_contents + '\n' + replies).replace('\r', ' ')
        matched_keyword = ''
        for keyword in keywords:
            if keyword in combined:
                matched_keyword = keyword
                break
        if not matched_keyword:
            continue
        snippet = combined[:120]
        samples.append({
            'case_no': str(row.get('caseNo') or ''),
            'cmd_id': str(row.get('cmdId') or ''),
            'keyword': matched_keyword,
            'snippet': snippet,
        })
        if len(samples) >= limit:
            break
    return samples


def filter_special_case_rows(rows: Iterable[Dict[str, Any]], keywords: Sequence[str], selected_branches: Sequence[str] | None = None) -> List[Dict[str, Any]]:
    branch_names = normalize_branch_selection(selected_branches)
    branch_cmd_ids = {BRANCH_CMD_ID_MAP[name] for name in branch_names}
    filtered: List[Dict[str, Any]] = []
    for row in rows or []:
        case_contents = str(row.get('caseContents') or '')
        replies = str(row.get('replies') or '')
        combined = case_contents + '\n' + replies
        if not any(keyword in combined for keyword in keywords):
            continue
        if branch_cmd_ids:
            cmd_id = str(row.get('cmdId') or '').strip()
            if cmd_id not in branch_cmd_ids:
                continue
        filtered.append(row)
    return filtered


def paginate_rows(rows: Sequence[Dict[str, Any]], page_num: int = 1, page_size: int = 15) -> Dict[str, Any]:
    page_num = max(1, int(page_num or 1))
    page_size = max(1, min(int(page_size or 15), 200))
    total = len(rows or [])
    start = (page_num - 1) * page_size
    end = start + page_size
    return {
        'total': total,
        'page_num': page_num,
        'page_size': page_size,
        'rows': list(rows[start:end]),
    }


def build_defaults_payload(case_type: str, label: str) -> Dict[str, Any]:
    start_time, end_time = default_time_range()
    return {
        'success': True,
        'case_type': case_type,
        'label': label,
        'start_time': start_time,
        'end_time': end_time,
        'branches': branch_options(),
    }


def query_special_case_records(case_type: str, label: str, keywords: Sequence[str], start_time: str, end_time: str, branches: Sequence[str] | None, page_num: int, page_size: int) -> Dict[str, Any]:
    trace_id = uuid.uuid4().hex[:10]
    begin_date = normalize_datetime_text(start_time) if start_time else default_time_range()[0]
    end_date = normalize_datetime_text(end_time) if end_time else default_time_range()[1]
    rows = fetch_all_special_case_rows(begin_date, end_date)
    normalized_branches = normalize_branch_selection(branches)
    keyword_match_count = count_keyword_matches(rows, keywords)
    filtered_rows = filter_special_case_rows(rows, keywords, selected_branches=branches)
    paged = paginate_rows(filtered_rows, page_num=page_num, page_size=page_size)
    debug_info = {
        'trace_id': trace_id,
        'requested_branches': list(branches or []),
        'normalized_branches': normalized_branches,
        'branch_cmd_ids': [BRANCH_CMD_ID_MAP[name] for name in normalized_branches],
        'upstream_row_count': len(rows),
        'keyword_match_count': keyword_match_count,
        'branch_filtered_count': len(filtered_rows),
        'page_row_count': len(paged['rows']),
        'sample_cmd_ids': collect_cmd_id_samples(rows),
        'keyword_hit_samples': collect_keyword_hit_samples(rows, keywords),
    }
    logger.info(
        '[trace:%s][%s] special case query start=%s end=%s req_branches=%s normalized_branches=%s upstream=%s keyword_match=%s branch_filtered=%s page_num=%s page_size=%s page_rows=%s sample_cmd_ids=%s',
        trace_id,
        case_type,
        begin_date,
        end_date,
        list(branches or []),
        normalized_branches,
        len(rows),
        keyword_match_count,
        len(filtered_rows),
        paged['page_num'],
        paged['page_size'],
        len(paged['rows']),
        debug_info['sample_cmd_ids'],
    )
    return {
        'success': True,
        'case_type': case_type,
        'label': label,
        'start_time': begin_date,
        'end_time': end_date,
        'branches': normalized_branches,
        'debug': debug_info,
        **paged,
    }


def generate_special_case_excel(rows: Sequence[Dict[str, Any]], title: str) -> io.BytesIO:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = title[:31]
    for col_idx, (_, header) in enumerate(EXPORT_HEADERS, 1):
        worksheet.cell(row=1, column=col_idx, value=header).font = openpyxl.styles.Font(bold=True)
    for row_idx, row in enumerate(rows or [], 2):
        for col_idx, (field, _) in enumerate(EXPORT_HEADERS, 1):
            worksheet.cell(row=row_idx, column=col_idx, value=row.get(field, ''))
    out = io.BytesIO()
    workbook.save(out)
    out.seek(0)
    return out


def generate_special_case_csv(rows: Sequence[Dict[str, Any]]) -> io.BytesIO:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([header for _, header in EXPORT_HEADERS])
    for row in rows or []:
        writer.writerow([row.get(field, '') for field, _ in EXPORT_HEADERS])
    return io.BytesIO(output.getvalue().encode('utf-8-sig'))


def export_special_case_records(case_type: str, label: str, keywords: Sequence[str], export_format: str, start_time: str, end_time: str, branches: Sequence[str] | None):
    result = query_special_case_records(
        case_type=case_type,
        label=label,
        keywords=keywords,
        start_time=start_time,
        end_time=end_time,
        branches=branches,
        page_num=1,
        page_size=200000,
    )
    rows = result['rows']
    if export_format == 'csv':
        return generate_special_case_csv(rows), 'text/csv; charset=utf-8', f'{label}.csv'
    return generate_special_case_excel(rows, label), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', f'{label}.xlsx'
