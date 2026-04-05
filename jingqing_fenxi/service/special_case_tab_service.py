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


CUSTOM_CASE_MONITOR_LABEL = "自定义警情监测"

BRANCH_CMD_ID_MAP = {
    "市局": "445300000000",
    "云城": "445302000000",
    "云安": "445303000000",
    "罗定": "445381000000",
    "新兴": "445321000000",
    "郁南": "445322000000",
}

SPECIAL_CASE_UPSTREAM_PAGE_SIZE = 5000

EXPORT_HEADERS = [
    ("caseNo", "接警号"),
    ("callTime", "报警时间"),
    ("cmdId", "分局编码"),
    ("dutyDeptName", "管辖单位"),
    ("caseLevelName", "警情级别"),
    ("occurAddress", "涉案地址"),
    ("callerName", "报警人"),
    ("callerPhone", "报警人电话"),
    ("caseContents", "简要案情"),
    ("replies", "反馈内容"),
]

RULE_FIELD_OPTIONS = [
    {"value": "combined_text", "label": "报警内容+反馈内容"},
    {"value": "caseContents", "label": "报警内容"},
    {"value": "replies", "label": "反馈内容"},
    {"value": "occurAddress", "label": "涉案地址"},
    {"value": "dutyDeptName", "label": "管辖单位"},
    {"value": "callerName", "label": "报警人"},
    {"value": "callerPhone", "label": "报警人电话"},
    {"value": "cmdId", "label": "分局编码"},
    {"value": "caseLevelName", "label": "警情级别"},
]

RULE_OPERATOR_OPTIONS = [
    {"value": "contains_any", "label": "包含任一值"},
    {"value": "contains_all", "label": "包含全部值"},
    {"value": "not_contains_any", "label": "不包含任一值"},
    {"value": "equals", "label": "完全等于"},
    {"value": "in_list", "label": "属于值列表"},
]

ALLOWED_RULE_FIELDS = {item["value"] for item in RULE_FIELD_OPTIONS}
ALLOWED_RULE_OPERATORS = {item["value"] for item in RULE_OPERATOR_OPTIONS}
HIT_KEYWORD_HEADER = ("hitKeywords", "命中关键字")
_FILENAME_SAFE_PATTERN = re.compile(r'[\\/:*?"<>|\r\n\t]+')
_SHEET_TITLE_SAFE_PATTERN = re.compile(r"[\[\]\*?:/\\]")


def default_time_range() -> Tuple[str, str]:
    now = datetime.now()
    end_dt = datetime(now.year, now.month, now.day, 23, 59, 59)
    start_dt = datetime(now.year, now.month, now.day, 0, 0, 0)
    return start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")


def field_options() -> List[Dict[str, str]]:
    return [dict(item) for item in RULE_FIELD_OPTIONS]


def operator_options() -> List[Dict[str, str]]:
    return [dict(item) for item in RULE_OPERATOR_OPTIONS]


def sanitize_filename_component(value: Any) -> str:
    text = str(value or "").strip()
    text = _FILENAME_SAFE_PATTERN.sub("_", text)
    text = re.sub(r"\s+", " ", text).strip(" ._")
    return text or "export"


def sanitize_sheet_title(value: Any) -> str:
    text = str(value or "").strip()
    text = _SHEET_TITLE_SAFE_PATTERN.sub("_", text)
    text = re.sub(r"\s+", " ", text).strip(" '")
    return (text or "Sheet")[:31]


def build_export_filename(scheme_name: str, start_time: str, end_time: str, export_format: str) -> str:
    fmt = str(export_format or "xlsx").strip().lower().lstrip(".") or "xlsx"
    start_text = normalize_datetime_text(start_time) if start_time else default_time_range()[0]
    end_text = normalize_datetime_text(end_time) if end_time else default_time_range()[1]
    start_date = start_text.split(" ", 1)[0]
    end_date = end_text.split(" ", 1)[0]
    return f"{sanitize_filename_component(scheme_name)}_{start_date}_{end_date}.{fmt}"


def build_defaults_payload() -> Dict[str, Any]:
    start_time, end_time = default_time_range()
    return {
        "success": True,
        "start_time": start_time,
        "end_time": end_time,
        "branches": branch_options(),
    }


def normalize_datetime_text(value: str) -> str:
    text = str(value or "").strip().replace("T", " ")
    if not text:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return text + " 00:00:00"
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$", text):
        return text + ":00"
    if not re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$", text):
        raise ValueError("时间格式不正确")
    return text


def normalize_branch_selection(branches: Sequence[str] | None) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for branch in branches or []:
        name = str(branch or "").strip()
        if not name or name not in BRANCH_CMD_ID_MAP or name in seen:
            continue
        seen.add(name)
        normalized.append(name)
    return normalized


def branch_options() -> List[Dict[str, str]]:
    return [
        {"value": label, "label": label, "cmd_id": cmd_id}
        for label, cmd_id in BRANCH_CMD_ID_MAP.items()
    ]


def build_special_case_payload(begin_date: str, end_date: str) -> Dict[str, Any]:
    return {
        "params[colArray]": "",
        "beginDate": begin_date,
        "endDate": end_date,
        "newCaseSourceNo": "",
        "newCaseSource": "全部",
        "dutyDeptNo": "",
        "dutyDeptName": "全部",
        "newCharaSubclassNo": "",
        "newCharaSubclass": "全部",
        "newOriCharaSubclassNo": "",
        "newOriCharaSubclass": "全部",
        "caseNo": "",
        "callerName": "",
        "callerPhone": "",
        "phoneAddress": "",
        "callerIdentity": "",
        "operatorNo": "",
        "operatorName": "",
        "params[isInvalidCase]": "",
        "occurAddress": "",
        "caseMarkNo": "",
        "caseMark": "全部",
        "params[repetitionCase]": "",
        "params[originalDuplicateCase]": "",
        "params[startTimePeriod]": "",
        "params[endTimePeriod]": "",
        "caseContents": "",
        "replies": "",
        "params[sinceRecord]": "",
        "dossierResult": "",
        "params[isVideo]": "",
        "params[isConversation]": "",
        "pageSize": SPECIAL_CASE_UPSTREAM_PAGE_SIZE,
        "pageNum": 1,
        "orderByColumn": "callTime",
        "isAsc": "desc",
    }


def fetch_all_special_case_rows(begin_date: str, end_date: str) -> List[Dict[str, Any]]:
    return fetch_all_case_list(
        build_special_case_payload(begin_date, end_date),
        max_page_size=SPECIAL_CASE_UPSTREAM_PAGE_SIZE,
    )


def normalize_rule_values(values: Any) -> List[str]:
    result: List[str] = []
    seen = set()

    def add_token(raw_value: Any) -> None:
        for item in re.split(r"[\r\n,]+", str(raw_value or "")):
            token = item.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            result.append(token)

    if isinstance(values, (list, tuple)):
        for value in values:
            add_token(value)
    else:
        add_token(values)
    return result


def validate_scheme_rules(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for index, rule in enumerate(rules or [], start=1):
        field_name = str(rule.get("field_name") or "").strip()
        operator = str(rule.get("operator") or "").strip()
        rule_values = normalize_rule_values(rule.get("rule_values"))
        if field_name not in ALLOWED_RULE_FIELDS:
            raise ValueError(f"规则字段不支持：{field_name}")
        if operator not in ALLOWED_RULE_OPERATORS:
            raise ValueError(f"规则操作符不支持：{operator}")
        if not rule_values:
            raise ValueError("规则值列表不能为空")
        normalized.append(
            {
                "field_name": field_name,
                "operator": operator,
                "rule_values": rule_values,
                "sort_order": int(rule.get("sort_order") or index),
                "is_enabled": bool(rule.get("is_enabled", True)),
            }
        )
    if not normalized:
        raise ValueError("至少需要配置一条规则")
    if not any(rule["is_enabled"] for rule in normalized):
        raise ValueError("至少需要一条启用规则")
    return normalized


def _row_field_text(row: Dict[str, Any], field_name: str) -> str:
    if field_name == "combined_text":
        return (str(row.get("caseContents") or "") + "\n" + str(row.get("replies") or "")).strip()
    return str(row.get(field_name) or "").strip()


def _rule_hit_values(row: Dict[str, Any], rule: Dict[str, Any]) -> List[str]:
    target = _row_field_text(row, rule["field_name"])
    values = normalize_rule_values(rule.get("rule_values"))
    operator = str(rule.get("operator") or "").strip()
    if operator == "contains_any":
        return [value for value in values if value in target]
    if operator == "contains_all":
        return values if all(value in target for value in values) else []
    if operator == "not_contains_any":
        return []
    if operator == "equals":
        return [value for value in values if target == value]
    if operator == "in_list":
        return [target] if target in set(values) else []
    return []


def collect_rule_hit_keywords(row: Dict[str, Any], rules: Sequence[Dict[str, Any]]) -> List[str]:
    keywords: List[str] = []
    seen = set()
    for rule in rules or []:
        if not rule.get("is_enabled", True):
            continue
        for value in _rule_hit_values(row, rule):
            if value in seen:
                continue
            seen.add(value)
            keywords.append(value)
    return keywords


def _rule_matches_row(row: Dict[str, Any], rule: Dict[str, Any]) -> bool:
    target = _row_field_text(row, rule["field_name"])
    values = normalize_rule_values(rule.get("rule_values"))
    if rule["operator"] == "contains_any":
        return any(value in target for value in values)
    if rule["operator"] == "contains_all":
        return all(value in target for value in values)
    if rule["operator"] == "not_contains_any":
        return all(value not in target for value in values)
    if rule["operator"] == "equals":
        return any(target == value for value in values)
    if rule["operator"] == "in_list":
        return target in set(values)
    return False


def filter_rows_by_rules(rows: Iterable[Dict[str, Any]], rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enabled_rules = [rule for rule in rules if rule.get("is_enabled", True)]
    if not enabled_rules:
        return []
    filtered: List[Dict[str, Any]] = []
    for row in rows or []:
        if all(_rule_matches_row(row, rule) for rule in enabled_rules):
            filtered.append(row)
    return filtered


def filter_rows_by_branches(rows: Iterable[Dict[str, Any]], selected_branches: Sequence[str] | None = None) -> List[Dict[str, Any]]:
    branch_names = normalize_branch_selection(selected_branches)
    branch_cmd_ids = {BRANCH_CMD_ID_MAP[name] for name in branch_names}
    if not branch_cmd_ids:
        return list(rows or [])

    filtered: List[Dict[str, Any]] = []
    for row in rows or []:
        cmd_id = str(row.get("cmdId") or "").strip()
        if cmd_id in branch_cmd_ids:
            filtered.append(row)
    return filtered


def paginate_rows(rows: Sequence[Dict[str, Any]], page_num: int = 1, page_size: int = 15) -> Dict[str, Any]:
    page_num = max(1, int(page_num or 1))
    page_size = max(1, min(int(page_size or 15), 200))
    total = len(rows or [])
    start = (page_num - 1) * page_size
    end = start + page_size
    return {
        "total": total,
        "page_num": page_num,
        "page_size": page_size,
        "rows": list(rows[start:end]),
    }


def collect_cmd_id_samples(rows: Iterable[Dict[str, Any]], limit: int = 8) -> List[str]:
    samples: List[str] = []
    seen = set()
    for row in rows or []:
        cmd_id = str(row.get("cmdId") or "").strip()
        if not cmd_id or cmd_id in seen:
            continue
        seen.add(cmd_id)
        samples.append(cmd_id)
        if len(samples) >= limit:
            break
    return samples


def collect_rule_hit_samples(rows: Iterable[Dict[str, Any]], rules: Sequence[Dict[str, Any]], limit: int = 5) -> List[Dict[str, str]]:
    samples: List[Dict[str, str]] = []
    enabled_rules = [rule for rule in rules if rule.get("is_enabled", True)]
    for row in rows or []:
        combined_text = _row_field_text(row, "combined_text").replace("\r", " ")
        samples.append(
            {
                "case_no": str(row.get("caseNo") or ""),
                "cmd_id": str(row.get("cmdId") or ""),
                "rules": " | ".join(f"{rule['field_name']}:{rule['operator']}" for rule in enabled_rules),
                "snippet": combined_text[:120],
            }
        )
        if len(samples) >= limit:
            break
    return samples


def query_special_case_records(
    *,
    label: str,
    scheme_id: int,
    scheme_name: str,
    rules: Sequence[Dict[str, Any]],
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    page_num: int,
    page_size: int,
) -> Dict[str, Any]:
    trace_id = uuid.uuid4().hex[:10]
    begin_date = normalize_datetime_text(start_time) if start_time else default_time_range()[0]
    end_date = normalize_datetime_text(end_time) if end_time else default_time_range()[1]
    rows = fetch_all_special_case_rows(begin_date, end_date)
    normalized_branches = normalize_branch_selection(branches)
    rule_filtered_rows = filter_rows_by_rules(rows, rules)
    branch_filtered_rows = filter_rows_by_branches(rule_filtered_rows, selected_branches=branches)
    paged = paginate_rows(branch_filtered_rows, page_num=page_num, page_size=page_size)
    debug_info = {
        "trace_id": trace_id,
        "requested_branches": list(branches or []),
        "normalized_branches": normalized_branches,
        "branch_cmd_ids": [BRANCH_CMD_ID_MAP[name] for name in normalized_branches],
        "upstream_row_count": len(rows),
        "rule_match_count": len(rule_filtered_rows),
        "branch_filtered_count": len(branch_filtered_rows),
        "page_row_count": len(paged["rows"]),
        "sample_cmd_ids": collect_cmd_id_samples(rows),
        "rule_hit_samples": collect_rule_hit_samples(rule_filtered_rows, rules),
    }
    logger.info(
        "[trace:%s][custom-case-monitor] scheme_id=%s scheme_name=%s start=%s end=%s rules=%s req_branches=%s normalized_branches=%s upstream=%s rule_match=%s branch_filtered=%s page_num=%s page_size=%s page_rows=%s sample_cmd_ids=%s",
        trace_id,
        scheme_id,
        scheme_name,
        begin_date,
        end_date,
        len([rule for rule in rules if rule.get("is_enabled", True)]),
        list(branches or []),
        normalized_branches,
        len(rows),
        len(rule_filtered_rows),
        len(branch_filtered_rows),
        paged["page_num"],
        paged["page_size"],
        len(paged["rows"]),
        debug_info["sample_cmd_ids"],
    )
    return {
        "success": True,
        "label": label,
        "scheme_id": scheme_id,
        "scheme_name": scheme_name,
        "start_time": begin_date,
        "end_time": end_date,
        "branches": normalized_branches,
        "debug": debug_info,
        **paged,
    }


def generate_special_case_excel(
    rows: Sequence[Dict[str, Any]],
    title: str,
    headers: Sequence[Tuple[str, str]] | None = None,
) -> io.BytesIO:
    headers = list(headers or EXPORT_HEADERS)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = sanitize_sheet_title(title)
    for col_idx, (_, header) in enumerate(headers, 1):
        worksheet.cell(row=1, column=col_idx, value=header).font = openpyxl.styles.Font(bold=True)
    for row_idx, row in enumerate(rows or [], 2):
        for col_idx, (field, _) in enumerate(headers, 1):
            worksheet.cell(row=row_idx, column=col_idx, value=row.get(field, ""))
    out = io.BytesIO()
    workbook.save(out)
    out.seek(0)
    return out


def generate_special_case_csv(
    rows: Sequence[Dict[str, Any]],
    headers: Sequence[Tuple[str, str]] | None = None,
) -> io.BytesIO:
    headers = list(headers or EXPORT_HEADERS)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([header for _, header in headers])
    for row in rows or []:
        writer.writerow([row.get(field, "") for field, _ in headers])
    return io.BytesIO(output.getvalue().encode("utf-8-sig"))


def export_special_case_records(
    *,
    label: str,
    scheme_name: str,
    rules: Sequence[Dict[str, Any]],
    export_format: str,
    start_time: str,
    end_time: str,
    branches: Sequence[str] | None,
    include_hit_keywords: bool = False,
    download_name: str | None = None,
):
    result = query_special_case_records(
        label=label,
        scheme_id=0,
        scheme_name=scheme_name,
        rules=rules,
        start_time=start_time,
        end_time=end_time,
        branches=branches,
        page_num=1,
        page_size=200000,
    )
    rows = result["rows"]
    if export_format == "csv":
        return (
            generate_special_case_csv(rows),
            "text/csv; charset=utf-8",
            download_name or f"{sanitize_filename_component(scheme_name)}.csv",
        )
    headers = list(EXPORT_HEADERS)
    export_rows = list(rows)
    if include_hit_keywords:
        headers.append(HIT_KEYWORD_HEADER)
        export_rows = [
            {**row, HIT_KEYWORD_HEADER[0]: "、".join(collect_rule_hit_keywords(row, rules))}
            for row in rows
        ]
    return (
        generate_special_case_excel(export_rows, scheme_name, headers=headers),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        download_name or f"{sanitize_filename_component(scheme_name)}.xlsx",
    )
