from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from gonggong.service.upstream_jingqing_client import api_client


CASE_LIST_PAGE_SIZE = 2000
_MINOR_CASE_MARK_NO = "01020201,0102020101,0102020102,0102020103"
_MINOR_CASE_MARK = "未成年人,未成年人（加害方）,未成年人（受害方）,未成年人（其他）"
_LOGIN_TIMEOUT_MESSAGE = "111警情系统登录或取数超时，请检查网络连通性和上游系统状态"
_DEFAULT_MINOR_SUBCLASS_PREFIXES = ("01", "02")


def _stringify(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _first_non_empty(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        return value
    return ""


def _build_minor_case_payload(*, start_time: str, end_time: str, page_num: int) -> Dict[str, str]:
    return {
        "beginDate": start_time,
        "endDate": end_time,
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
        "caseMarkNo": _MINOR_CASE_MARK_NO,
        "caseMark": _MINOR_CASE_MARK,
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
        "pageSize": str(CASE_LIST_PAGE_SIZE),
        "pageNum": str(page_num),
        "orderByColumn": "callTime",
        "isAsc": "desc",
    }


def _parse_total(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(value)
    except Exception:
        return None


def fetch_minor_case_rows(*, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page_num = 1
    total: Optional[int] = None

    while True:
        payload = _build_minor_case_payload(
            start_time=start_time,
            end_time=end_time,
            page_num=page_num,
        )
        result = api_client.get_case_list(payload)
        if not isinstance(result, dict):
            raise RuntimeError("case/list 响应格式异常")

        code = result.get("code")
        if code == -1:
            raise RuntimeError(_LOGIN_TIMEOUT_MESSAGE)
        if code not in (None, 0):
            raise RuntimeError(f"case/list 返回异常，code={code}，msg={result.get('msg', '')}")

        raw_rows = result.get("rows")
        if raw_rows is None:
            raw_rows = []
        if not isinstance(raw_rows, list):
            raise RuntimeError("case/list rows 不是数组")

        if total is None:
            total = _parse_total(result.get("total"))

        for raw_row in raw_rows:
            if not isinstance(raw_row, dict):
                raise RuntimeError("case/list 单条记录格式异常")
            rows.append(dict(raw_row))

        if not raw_rows:
            break
        if len(raw_rows) < CASE_LIST_PAGE_SIZE:
            break
        if total is not None and len(rows) >= total:
            break
        page_num += 1

    return rows


def filter_minor_case_rows_by_subclasses(
    rows: Sequence[Mapping[str, Any]],
    *,
    subclass_codes: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    if subclass_codes is None:
        filtered_default: List[Dict[str, Any]] = []
        for row in rows or []:
            subclass = _stringify(_first_non_empty(row, "newCharaSubclass", "newcharasubclass"))
            if subclass[:2] in _DEFAULT_MINOR_SUBCLASS_PREFIXES:
                filtered_default.append(dict(row))
        return filtered_default

    allowed = {str(code).strip() for code in (subclass_codes or []) if str(code).strip()}
    if not allowed:
        return []

    filtered: List[Dict[str, Any]] = []
    for row in rows or []:
        subclass = _stringify(_first_non_empty(row, "newCharaSubclass", "newcharasubclass"))
        if subclass in allowed:
            filtered.append(dict(row))
    return filtered


def extract_minor_case_region_code(row: Mapping[str, Any]) -> str:
    cmdid = _stringify(_first_non_empty(row, "cmdid", "cmdId"))
    if cmdid:
        return cmdid[:6]
    duty_dept_no = _stringify(_first_non_empty(row, "dutyDeptNo", "dutydeptno"))
    if duty_dept_no:
        return duty_dept_no[:6]
    return ""


def count_minor_case_rows_by_region(rows: Sequence[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows or []:
        region_code = extract_minor_case_region_code(row)
        if not region_code:
            continue
        counts[region_code] = int(counts.get(region_code) or 0) + 1
    return counts


def _map_minor_case_detail_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "报警时间": _first_non_empty(row, "callTime", "calltime"),
        "警情编号": _first_non_empty(row, "caseNo", "caseno"),
        "管辖单位": _first_non_empty(row, "dutyDeptName", "dutydeptname"),
        "分局": _first_non_empty(row, "cmdName", "cmdname"),
        "警情地址": _first_non_empty(row, "occurAddress", "occuraddress"),
        "报警内容": _first_non_empty(row, "caseContents", "casecontents"),
        "处警情况": _first_non_empty(row, "replies"),
        "警情标注": _first_non_empty(row, "caseMark", "casemark"),
        "经度": _first_non_empty(row, "lngOfCriterion", "lngofcriterion"),
        "纬度": _first_non_empty(row, "latOfCriterion", "latofcriterion"),
        "地区": extract_minor_case_region_code(row),
    }


def build_minor_case_detail_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    diqu: str,
    limit: Optional[int],
) -> Tuple[List[Dict[str, Any]], bool]:
    is_all = diqu in ("", "__ALL__", "全市")
    detail_rows: List[Dict[str, Any]] = []
    for row in rows or []:
        region_code = extract_minor_case_region_code(row)
        if not is_all and region_code != diqu:
            continue
        detail_rows.append(_map_minor_case_detail_row(row))

    detail_rows.sort(key=lambda item: _stringify(item.get("报警时间")), reverse=True)

    limit_n = int(limit) if limit and int(limit) > 0 else 0
    truncated = False
    if limit_n and len(detail_rows) > limit_n:
        truncated = True
        detail_rows = detail_rows[:limit_n]
    return detail_rows, truncated
