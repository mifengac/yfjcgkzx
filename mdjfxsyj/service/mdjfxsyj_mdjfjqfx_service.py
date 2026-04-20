from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from gonggong.service.upstream_jingqing_client import api_client
from mdjfxsyj.dao.mdjfxsyj_mdjfjqfx_dao import (
    query_branch_options,
    query_case_conversion_map,
    query_org_mappings,
)

PAGE_SIZE = 1000
MAX_PAGES = 200
DETAIL_COLUMNS = [
    "警情编号",
    "报警时间",
    "报警电话",
    "报警电话次数",
    "原始性质",
    "确认性质",
    "报警地址",
    "报警内容",
    "处警情况",
    "所属分局",
    "所属派出所",
    "是否转案",
    "案件编号",
]


def _parse_dt(value: str) -> datetime:
    text = str(value or "").strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return datetime.combine(parsed.date(), time.min) if fmt == "%Y-%m-%d" else parsed
        except ValueError:
            continue
    raise ValueError("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS")


def default_range() -> Tuple[datetime, datetime]:
    end_dt = datetime.combine(datetime.now().date(), time.min)
    return end_dt - timedelta(days=8), end_dt


def normalize_range(start_time: Optional[str], end_time: Optional[str]) -> Tuple[datetime, datetime, str, str]:
    if start_time and end_time:
        start_dt, end_dt = _parse_dt(start_time), _parse_dt(end_time)
    else:
        start_dt, end_dt = default_range()
    if start_dt > end_dt:
        raise ValueError("开始时间不能晚于结束时间")
    return start_dt, end_dt, start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")


def normalize_group_by(value: Optional[str]) -> str:
    return value if value in {"fenju", "paichusuo"} else "fenju"


def normalize_repeat_min(value: Any) -> int:
    try:
        repeat_min = int(value or 2)
    except (TypeError, ValueError):
        raise ValueError("重复次数阈值必须是 2-10 的整数") from None
    if repeat_min < 2 or repeat_min > 10:
        raise ValueError("重复次数阈值必须是 2-10 的整数")
    return repeat_min


def _fetch_nature_tree() -> Sequence[Mapping[str, Any]]:
    getter = getattr(api_client, "get_nature_tree_new_view_data", None)
    if callable(getter):
        payload = getter()
        return payload if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)) else []

    # Keep this module resilient when a running dev server still holds an older api_client instance.
    response = api_client.request_with_retry("GET", "/dsjfx/nature/treeNewViewData", timeout=15)
    if response and response.status_code == 200:
        payload = response.json()
        return payload if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)) else []
    return []


def get_mdj_category_code_csv(tree_data: Optional[Sequence[Mapping[str, Any]]] = None) -> str:
    payload = tree_data if tree_data is not None else _fetch_nature_tree()
    if not isinstance(payload, Sequence):
        raise RuntimeError("警情性质树接口返回格式异常")
    codes: List[str] = ["08"]
    seen = {"08"}
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("id") or "").strip()
        parent = str(item.get("pId") or item.get("pid") or "").strip()
        if code and parent == "08" and code not in seen:
            seen.add(code)
            codes.append(code)
    return ",".join(codes)


def _build_case_payload(start_text: str, end_text: str, code_csv: str, page_num: int) -> Dict[str, str]:
    return {
        "beginDate": start_text,
        "endDate": end_text,
        "newOriCharaSubclassNo": code_csv,
        "newCaseSourceNo": "",
        "newCaseSource": "全部",
        "dutyDeptNo": "",
        "dutyDeptName": "全部",
        "newCharaSubclassNo": "",
        "newCharaSubclass": "全部",
        "caseNo": "",
        "callerName": "",
        "callerPhone": "",
        "occurAddress": "",
        "caseContents": "",
        "replies": "",
        "pageSize": str(PAGE_SIZE),
        "pageNum": str(page_num),
        "orderByColumn": "callTime",
        "isAsc": "desc",
    }


def fetch_raw_mdj_cases(start_text: str, end_text: str) -> List[Mapping[str, Any]]:
    code_csv = get_mdj_category_code_csv()
    rows: List[Mapping[str, Any]] = []
    total: Optional[int] = None
    page_num = 1
    while True:
        result = api_client.get_case_list(_build_case_payload(start_text, end_text, code_csv, page_num))
        if not isinstance(result, Mapping):
            raise RuntimeError("警情接口返回格式异常")
        if result.get("code") == -1:
            raise RuntimeError("警情接口登录失效或请求超时")
        if result.get("code") not in (None, 0):
            raise RuntimeError(f"警情接口返回异常：code={result.get('code')} msg={result.get('msg', '')}")
        page_rows = result.get("rows") or []
        if not isinstance(page_rows, list):
            raise RuntimeError("警情接口 rows 格式异常")
        if total is None:
            total = int(result.get("total", 0) or 0)
        rows.extend(row for row in page_rows if isinstance(row, Mapping))
        if not page_rows or len(page_rows) < PAGE_SIZE or page_num * PAGE_SIZE >= total:
            break
        if page_num >= MAX_PAGES:
            raise RuntimeError("查询结果过大，请缩短时间范围后重试")
        page_num += 1
    return rows


def _first(row: Mapping[str, Any], *keys: str) -> str:
    lower = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
        value = lower.get(key.lower())
        if value not in (None, ""):
            return str(value)
    return ""


def clean_phone(value: Any) -> str:
    text = re.sub(r"[\s,，;；\-—_()（）]+", "", str(value or ""))
    digits = re.sub(r"\D", "", text)
    if len(digits) < 7 or len(set(digits)) <= 1:
        return ""
    return digits


def _normalize_case(row: Mapping[str, Any], branch_map: Mapping[str, str], station_map: Mapping[str, str]) -> Dict[str, Any]:
    case_no = _first(row, "caseNo", "caseno")
    cmd_id = _first(row, "cmdId", "cmdid")
    duty_no = _first(row, "dutyDeptNo", "dutydeptno")
    ori_code = _first(row, "newOriCharaSubclass", "neworicharasubclass", "newOriCharaSubclassNo")
    confirm_code = _first(row, "newCharaSubclass", "newcharasubclass", "newCharaSubclassNo")
    return {
        "case_no": case_no,
        "call_time": _first(row, "callTime", "calltime"),
        "caller_phone": _first(row, "callerPhone", "callerphone"),
        "phone_key": clean_phone(_first(row, "callerPhone", "callerphone")),
        "cmd_id": cmd_id,
        "cmd_name": _first(row, "cmdName", "cmdname"),
        "duty_dept_no": duty_no,
        "duty_dept_name": _first(row, "dutyDeptName", "dutydeptname"),
        "ori_code": ori_code,
        "ori_name": _first(row, "newOriCharaSubclassName", "neworicharasubclassname") or ori_code,
        "confirm_code": confirm_code,
        "confirm_name": _first(row, "newCharaSubclassName", "newcharasubclassname") or confirm_code,
        "occur_address": _first(row, "occurAddress", "occuraddress"),
        "case_contents": _first(row, "caseContents", "casecontents"),
        "replies": _first(row, "replies"),
        "branch_code": cmd_id,
        "branch_name": branch_map.get(cmd_id) or _first(row, "cmdName", "cmdname") or cmd_id or "未匹配分局",
        "station_code": duty_no,
        "station_name": station_map.get(duty_no) or _first(row, "dutyDeptName", "dutydeptname") or duty_no or "未匹配派出所",
        "is_both_mdj": ori_code[:2] == "08" and confirm_code[:2] == "08",
    }


def _load_cases(start_text: str, end_text: str, ssfjdm_list: Sequence[str]) -> List[Dict[str, Any]]:
    branch_map, station_map = query_org_mappings()
    selected = {str(value).strip() for value in ssfjdm_list if str(value).strip()}
    rows = [_normalize_case(row, branch_map, station_map) for row in fetch_raw_mdj_cases(start_text, end_text)]
    if selected:
        rows = [row for row in rows if row["branch_code"] in selected]
    conversion_map = query_case_conversion_map(row["case_no"] for row in rows)
    phone_counts = Counter(row["phone_key"] for row in rows if row["phone_key"])
    for row in rows:
        case_ids = conversion_map.get(row["case_no"], [])
        row["case_ids"] = case_ids
        row["is_converted"] = bool(case_ids)
        row["phone_count"] = phone_counts.get(row["phone_key"], 0) if row["phone_key"] else 0
    return rows


def _group_meta(row: Mapping[str, Any], group_by: str) -> Tuple[str, str]:
    if group_by == "paichusuo":
        return str(row.get("station_code") or ""), str(row.get("station_name") or "未匹配派出所")
    return str(row.get("branch_code") or ""), str(row.get("branch_name") or "未匹配分局")


def _ratio(part: int, total: int) -> str:
    return "" if not total else f"{part / total * 100:.2f}%"


def _overall_rows(rows: Sequence[Dict[str, Any]], group_by: str, repeat_min: int) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_group_meta(row, group_by)].append(row)
    if rows:
        groups[("__TOTAL__", "总计")] = list(rows)
    result: List[Dict[str, Any]] = []
    for (code, name), items in sorted(groups.items(), key=lambda item: (item[0][0] == "__TOTAL__", item[0][1])):
        converted = sum(1 for row in items if row["is_converted"])
        both = sum(1 for row in items if row["is_both_mdj"])
        repeat_rows = [row for row in items if row["phone_count"] >= repeat_min]
        repeat_converted = sum(1 for row in repeat_rows if row["is_converted"])
        result.append(
            {
                "group_code": code,
                "分组": name,
                "原始警情数": len(items),
                "转案数": converted,
                "转案率": _ratio(converted, len(items)),
                "原始确认均纠纷性质": both,
                "重复警情数": len(repeat_rows),
                "重复警情转案数": repeat_converted,
                "重复警情转案率": _ratio(repeat_converted, len(repeat_rows)),
            }
        )
    return result


def _fine_rows(rows: Sequence[Dict[str, Any]], group_by: str) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str, str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["is_both_mdj"]:
            group_code, group_name = _group_meta(row, group_by)
            key = (group_code, group_name, row["ori_code"], row["ori_name"], row["confirm_code"], row["confirm_name"])
            groups[key].append(row)
    result = []
    for (group_code, group_name, ori_code, ori_name, confirm_code, confirm_name), items in sorted(groups.items()):
        converted = sum(1 for row in items if row["is_converted"])
        result.append(
            {
                "group_code": group_code,
                "ori_code": ori_code,
                "confirm_code": confirm_code,
                "分组": group_name,
                "原始细类": ori_name,
                "确认细类": confirm_name,
                "警情数": len(items),
                "转案数": converted,
                "转案率": _ratio(converted, len(items)),
            }
        )
    return result


def _repeat_rows(rows: Sequence[Dict[str, Any]], group_by: str, repeat_min: int) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    repeat_all = [row for row in rows if row["phone_count"] >= repeat_min]
    for row in repeat_all:
        groups[_group_meta(row, group_by)].append(row)
    if repeat_all:
        groups[("__TOTAL__", "总计")] = repeat_all
    result = []
    for (code, name), items in sorted(groups.items(), key=lambda item: (item[0][0] == "__TOTAL__", item[0][1])):
        converted = sum(1 for row in items if row["is_converted"])
        result.append(
            {
                "group_code": code,
                "分组": name,
                "重复阈值": f">= {repeat_min}",
                "重复电话数": len({row["phone_key"] for row in items if row["phone_key"]}),
                "重复警情数": len(items),
                "重复转案数": converted,
                "重复转案率": _ratio(converted, len(items)),
            }
        )
    return result


def get_options() -> Dict[str, Any]:
    return {"branches": query_branch_options()}


def get_summary_payload(
    *, start_time: Optional[str], end_time: Optional[str], ssfjdm_list: Sequence[str], group_by: str, repeat_min: int
) -> Dict[str, Any]:
    _, _, start_text, end_text = normalize_range(start_time, end_time)
    group_by = normalize_group_by(group_by)
    rows = _load_cases(start_text, end_text, ssfjdm_list)
    return {
        "start_time": start_text,
        "end_time": end_text,
        "group_by": group_by,
        "repeat_min": repeat_min,
        "overall": _overall_rows(rows, group_by, repeat_min),
        "fine": _fine_rows(rows, group_by),
        "repeat": _repeat_rows(rows, group_by, repeat_min),
    }


def _filter_detail_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    group_by: str,
    group_code: str,
    dimension: str,
    repeat_min: int,
    ori_code: str = "",
    confirm_code: str = "",
) -> List[Dict[str, Any]]:
    filtered = [row for row in rows if group_code in ("", "__TOTAL__") or _group_meta(row, group_by)[0] == group_code]
    if dimension == "converted":
        filtered = [row for row in filtered if row["is_converted"]]
    elif dimension == "both_mdj":
        filtered = [row for row in filtered if row["is_both_mdj"]]
    elif dimension == "repeat":
        filtered = [row for row in filtered if row["phone_count"] >= repeat_min]
    elif dimension == "repeat_converted":
        filtered = [row for row in filtered if row["phone_count"] >= repeat_min and row["is_converted"]]
    elif dimension == "fine":
        filtered = [row for row in filtered if row["ori_code"] == ori_code and row["confirm_code"] == confirm_code]
    elif dimension == "fine_converted":
        filtered = [
            row
            for row in filtered
            if row["ori_code"] == ori_code and row["confirm_code"] == confirm_code and row["is_converted"]
        ]
    elif dimension != "original_total":
        raise ValueError("未知详情维度")
    return filtered


def _to_detail_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "警情编号": row.get("case_no", ""),
        "报警时间": row.get("call_time", ""),
        "报警电话": row.get("caller_phone", ""),
        "报警电话次数": row.get("phone_count", 0),
        "原始性质": row.get("ori_name", ""),
        "确认性质": row.get("confirm_name", ""),
        "报警地址": row.get("occur_address", ""),
        "报警内容": row.get("case_contents", ""),
        "处警情况": row.get("replies", ""),
        "所属分局": row.get("branch_name", ""),
        "所属派出所": row.get("station_name", ""),
        "是否转案": "是" if row.get("is_converted") else "否",
        "案件编号": "，".join(row.get("case_ids") or []),
    }


def get_detail_payload(
    *,
    start_time: Optional[str],
    end_time: Optional[str],
    ssfjdm_list: Sequence[str],
    group_by: str,
    repeat_min: int,
    dimension: str,
    group_code: str,
    ori_code: str = "",
    confirm_code: str = "",
    page: int = 1,
    page_size: int = 20,
) -> Dict[str, Any]:
    _, _, start_text, end_text = normalize_range(start_time, end_time)
    group_by = normalize_group_by(group_by)
    rows = _filter_detail_rows(
        _load_cases(start_text, end_text, ssfjdm_list),
        group_by=group_by,
        group_code=group_code,
        dimension=dimension,
        repeat_min=repeat_min,
        ori_code=ori_code,
        confirm_code=confirm_code,
    )
    detail_rows = [_to_detail_row(row) for row in rows]
    total = len(detail_rows)
    if page_size <= 0:
        page_rows = detail_rows
        page = 1
    else:
        page = max(page, 1)
        start = (page - 1) * page_size
        page_rows = detail_rows[start : start + page_size]
    return {"start_time": start_text, "end_time": end_text, "total": total, "page": page, "rows": page_rows}
