from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List

from jszahzyj.jszahz_topic_constants import PERSON_TYPE_ORDER


def _normalize_list(values: Iterable[str] | None) -> List[str]:
    return [str(item).strip() for item in (values or []) if str(item).strip()]


def _normalize_datetime_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value or "").strip()
    return text.replace("T", " ") if text else ""


def _detail_sort_key(row: Dict[str, Any]):
    raw_time = str(row.get("列管时间") or "")
    sort_time = raw_time or "0000-00-00 00:00:00"
    return (
        str(row.get("分局") or "未匹配分局"),
        -int(sort_time.replace("-", "").replace(":", "").replace(" ", "") or 0),
        str(row.get("姓名") or ""),
        str(row.get("身份证号") or ""),
    )


def build_person_records(
    *,
    base_rows: List[Dict[str, Any]],
    live_rows: List[Dict[str, Any]],
    tag_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    people: Dict[str, Dict[str, Any]] = {}

    for item in base_rows or []:
        zjhm = str(item.get("zjhm") or "").strip().upper()
        if not zjhm:
            continue
        people[zjhm] = {
            "姓名": str(item.get("xm") or "").strip(),
            "身份证号": zjhm,
            "列管时间": "",
            "列管单位": "",
            "分局代码": str(item.get("ssfjdm") or "").strip() or "__UNMATCHED__",
            "分局": str(item.get("ssfj") or item.get("source_sheet_name") or "").strip() or "未匹配分局",
            "人员风险": "无数据",
            "人员类型": "",
            "_person_type_set": set(),
        }

    for item in live_rows or []:
        zjhm = str(item.get("zjhm") or "").strip().upper()
        if not zjhm:
            continue
        record = people.setdefault(
            zjhm,
            {
                "姓名": "",
                "身份证号": zjhm,
                "列管时间": "",
                "列管单位": "",
                "分局代码": "__UNMATCHED__",
                "分局": "未匹配分局",
                "人员风险": "无数据",
                "人员类型": "",
                "_person_type_set": set(),
            },
        )
        if not record["姓名"]:
            record["姓名"] = str(item.get("xm") or "").strip()
        record["列管时间"] = _normalize_datetime_value(item.get("lgsj"))
        record["列管单位"] = str(item.get("lgdw") or "").strip()
        record["人员风险"] = str(item.get("fxdj_label") or "无数据").strip() or "无数据"
        if record["分局代码"] == "__UNMATCHED__":
            record["分局代码"] = str(item.get("ssfjdm") or "").strip() or "__UNMATCHED__"
        if record["分局"] == "未匹配分局":
            record["分局"] = str(item.get("ssfj") or "").strip() or "未匹配分局"

    for item in tag_rows or []:
        zjhm = str(item.get("zjhm") or "").strip().upper()
        person_type = str(item.get("person_type") or "").strip()
        if not zjhm or not person_type:
            continue
        record = people.setdefault(
            zjhm,
            {
                "姓名": "",
                "身份证号": zjhm,
                "列管时间": "",
                "列管单位": "",
                "分局代码": "__UNMATCHED__",
                "分局": "未匹配分局",
                "人员风险": "无数据",
                "人员类型": "",
                "_person_type_set": set(),
            },
        )
        record["_person_type_set"].add(person_type)

    records: List[Dict[str, Any]] = []
    for record in people.values():
        person_types = sorted(
            record["_person_type_set"],
            key=lambda item: (PERSON_TYPE_ORDER.get(item, 99), item),
        )
        record["人员类型"] = ",".join(person_types)
        records.append(record)

    return records


def filter_person_records(
    records: List[Dict[str, Any]],
    *,
    branch_codes: Iterable[str] | None,
    person_types: Iterable[str] | None,
    risk_labels: Iterable[str] | None,
) -> List[Dict[str, Any]]:
    branch_set = set(_normalize_list(branch_codes))
    person_type_set = set(_normalize_list(person_types))
    risk_set = set(_normalize_list(risk_labels))

    filtered: List[Dict[str, Any]] = []
    for item in records or []:
        if branch_set and str(item.get("分局代码") or "") not in branch_set:
            continue
        if risk_set and str(item.get("人员风险") or "") not in risk_set:
            continue
        current_types = set(item.get("_person_type_set") or set())
        if person_type_set and not current_types.intersection(person_type_set):
            continue
        filtered.append(item)
    return filtered


def build_summary_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple, int] = defaultdict(int)
    for item in records or []:
        key = (
            str(item.get("分局代码") or "__UNMATCHED__"),
            str(item.get("分局") or "未匹配分局"),
        )
        grouped[key] += 1

    rows = [
        {
            "分局代码": code,
            "分局名称": name,
            "去重患者数": count,
        }
        for (code, name), count in grouped.items()
    ]
    rows.sort(key=lambda item: (-int(item["去重患者数"] or 0), str(item["分局名称"] or "")))
    return rows


def build_detail_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    detail_rows = []
    for item in records or []:
        detail_rows.append(
            {
                "姓名": item.get("姓名") or "",
                "身份证号": item.get("身份证号") or "",
                "列管时间": item.get("列管时间") or "",
                "列管单位": item.get("列管单位") or "",
                "分局": item.get("分局") or "未匹配分局",
                "人员风险": item.get("人员风险") or "无数据",
                "人员类型": item.get("人员类型") or "",
            }
        )
    detail_rows.sort(key=_detail_sort_key)
    return detail_rows