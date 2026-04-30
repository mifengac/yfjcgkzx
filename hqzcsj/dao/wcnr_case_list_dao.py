from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from gonggong.service.upstream_province_jingqing_client import province_api_client
from gonggong.service.upstream_jingqing_client import api_client


CASE_LIST_PAGE_SIZE = 2000
_MINOR_CASE_MARK_NO = "01020201,0102020101,0102020102,0102020103"
_MINOR_CASE_MARK = "未成年人,未成年人（加害方）,未成年人（受害方）,未成年人（其他）"
_PROVINCE_MINOR_CASE_SOURCE_CODE = (
    "0100,0101,0102,0103,0199,0200,0201,0202,0299,0400,0401,0402,0403,0404,"
    "0405,0499,0500,0600,0601,0602,0603,0604,0800,0801,0802,0900,0901,0902,"
    "0903,0904,0999,9900"
)
_PROVINCE_MINOR_CASE_SOURCE_NAME = (
    "电话报警,110报警,122报警,5G视频报警,其他电话报警,亲临报警,亲临到所,扭送现行,"
    "其他亲临报警,物联报警,终端报警,技防报警,校园报警,公交报警,地铁报警,其他物联报警,"
    "短信报警,网络报警,视频报警,网语报警,自助报警,其他网络报警,异地转警,省内,省外,"
    "其他部门移送,12345推送,119推送,120推送,心理关爱热线,其他部门,其他报警方式"
)
_PROVINCE_MINOR_CHARA_NO = "01,02"
_PROVINCE_MINOR_CHARA = "刑事类警情,行政（治安）类警情"
_CAMPUS_BULLYING_CASE_MARK_NO = "03010108,0604"
_CAMPUS_BULLYING_CASE_MARK = "校园欺凌,校园欺凌"
_LOGIN_TIMEOUT_MESSAGE = "111警情系统登录或取数超时，请检查网络连通性和上游系统状态"
_PROVINCE_LOGIN_TIMEOUT_MESSAGE = "省厅警情系统登录或取数超时，请检查网络连通性和上游系统状态"
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


def _build_province_minor_case_payload(
    *, start_time: str, end_time: str, page_num: int
) -> Dict[str, str]:
    return {
        "params[startTime]": start_time,
        "params[endTime]": end_time,
        "caseSourceCode": _PROVINCE_MINOR_CASE_SOURCE_CODE,
        "caseSourceName": _PROVINCE_MINOR_CASE_SOURCE_NAME,
        "caseNo": "",
        "dutyDeptNo": "",
        "dutyDeptName": "全部",
        "callerPhone": "",
        "occurAddress": "",
        "charaNo": _PROVINCE_MINOR_CHARA_NO,
        "chara": _PROVINCE_MINOR_CHARA,
        "callerPeopleName": "",
        "phoneAddress": "",
        "callerAddress": "",
        "oriCharaNo": "",
        "oriChara": "全部",
        "iniCharaNo": "",
        "iniChara": "全部",
        "fixCharaNo": "",
        "fixChara": "全部",
        "caseLevel": "",
        "operatorName": "",
        "callerPeopleIdcard": "",
        "uploadAreaNo": "",
        "fixCaseSourceCode": "",
        "fixCaseSourceName": "全部",
        "dossierNo": "",
        "caseMarkNo": _MINOR_CASE_MARK_NO,
        "caseMark": _MINOR_CASE_MARK,
        "firstOriCharaNo": "",
        "firstOriChara": "全部",
        "firstCharaNo": "",
        "firstChara": "全部",
        "handleResultNo": "",
        "pageSize": str(CASE_LIST_PAGE_SIZE),
        "pageNum": str(page_num),
        "orderByColumn": "alarmTime",
        "isAsc": "desc",
    }


def _build_campus_bullying_case_payload(*, start_time: str, end_time: str, page_num: int) -> Dict[str, str]:
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
        "caseMarkNo": _CAMPUS_BULLYING_CASE_MARK_NO,
        "caseMark": _CAMPUS_BULLYING_CASE_MARK,
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


def _fetch_case_rows(
    *,
    start_time: str,
    end_time: str,
    payload_builder,
    case_list_getter=None,
    timeout_message: str = _LOGIN_TIMEOUT_MESSAGE,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page_num = 1
    total: Optional[int] = None
    getter = case_list_getter or api_client.get_case_list

    while True:
        payload = payload_builder(
            start_time=start_time,
            end_time=end_time,
            page_num=page_num,
        )
        result = getter(payload)
        if not isinstance(result, dict):
            raise RuntimeError("case/list 响应格式异常")

        code = result.get("code")
        if code == -1:
            raise RuntimeError(timeout_message)
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


def fetch_minor_case_rows(*, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    return _fetch_case_rows(
        start_time=start_time,
        end_time=end_time,
        payload_builder=_build_minor_case_payload,
    )


def fetch_province_minor_case_rows(*, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    return _fetch_case_rows(
        start_time=start_time,
        end_time=end_time,
        payload_builder=_build_province_minor_case_payload,
        case_list_getter=province_api_client.get_case_list,
        timeout_message=_PROVINCE_LOGIN_TIMEOUT_MESSAGE,
    )


def fetch_campus_bullying_case_rows(*, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    return _fetch_case_rows(
        start_time=start_time,
        end_time=end_time,
        payload_builder=_build_campus_bullying_case_payload,
    )


def fetch_cases_by_incident_numbers(conn, incident_numbers: Sequence[str]) -> Dict[str, List[Dict[str, Any]]]:
    numbers = sorted({str(x or "").strip() for x in (incident_numbers or []) if str(x or "").strip()})
    if not numbers:
        return {}

    q = """
        SELECT
            ajxx_jqbh,
            ajxx_ajbh,
            ajxx_ajmc,
            ajxx_lasj
        FROM "ywdata"."zq_zfba_ajxx"
        WHERE ajxx_jqbh = ANY(%s)
          AND NULLIF(BTRIM(COALESCE(ajxx_ajbh, '')), '') IS NOT NULL
        ORDER BY ajxx_jqbh, ajxx_lasj DESC NULLS LAST, ajxx_ajbh DESC
    """
    with conn.cursor() as cur:
        cur.execute(q, (numbers,))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        jqbh = str(row.get("ajxx_jqbh") or "").strip()
        if not jqbh:
            continue
        grouped.setdefault(jqbh, []).append(row)
    return grouped


def filter_minor_case_rows_by_subclasses(
    rows: Sequence[Mapping[str, Any]],
    *,
    subclass_codes: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    if subclass_codes is None:
        filtered_default: List[Dict[str, Any]] = []
        for row in rows or []:
            subclass = _stringify(
                _first_non_empty(row, "newCharaSubclass", "newcharasubclass", "charaNo", "charano")
            )
            if subclass[:2] in _DEFAULT_MINOR_SUBCLASS_PREFIXES:
                filtered_default.append(dict(row))
        return filtered_default

    allowed = {str(code).strip() for code in (subclass_codes or []) if str(code).strip()}
    if not allowed:
        return []

    filtered: List[Dict[str, Any]] = []
    for row in rows or []:
        subclass = _stringify(
            _first_non_empty(row, "newCharaSubclass", "newcharasubclass", "charaNo", "charano")
        )
        if subclass in allowed:
            filtered.append(dict(row))
    return filtered


def extract_minor_case_region_code(row: Mapping[str, Any]) -> str:
    cmdid = _stringify(_first_non_empty(row, "cmdid", "cmdId"))
    if cmdid:
        return cmdid[:6]
    area_no = _stringify(_first_non_empty(row, "areaNo", "areano"))
    if area_no:
        return area_no[:6]
    duty_dept_no = _stringify(_first_non_empty(row, "dutyDeptNo", "dutydeptno"))
    if duty_dept_no:
        return duty_dept_no[:6]
    upload_area_no = _stringify(_first_non_empty(row, "uploadAreaNo", "uploadareano"))
    if upload_area_no:
        if len(upload_area_no) == 4:
            return f"{upload_area_no}00"
        return upload_area_no[:6]
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
        "报警时间": _first_non_empty(row, "alarmTime", "alarmtime", "callTime", "calltime"),
        "警情编号": _first_non_empty(row, "caseNo", "caseno"),
        "管辖单位": _first_non_empty(row, "dutyDeptName", "dutydeptname"),
        "分局": _first_non_empty(row, "cmdName", "cmdname", "uploadAreaName", "uploadareaname"),
        "警情地址": _first_non_empty(row, "occurAddress", "occuraddress"),
        "报警内容": _first_non_empty(row, "caseContents", "casecontents"),
        "处警情况": _first_non_empty(row, "replies", "supplementCaseContents", "supplementcasecontents"),
        "警情标注": _first_non_empty(row, "caseMark", "casemark"),
        "经度": _first_non_empty(
            row, "lngOfCriterion", "lngofcriterion", "lngOfLocate", "lngoflocate", "lngOfCall", "lngofcall"
        ),
        "纬度": _first_non_empty(
            row, "latOfCriterion", "latofcriterion", "latOfLocate", "latoflocate", "latOfCall", "latofcall"
        ),
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
