from __future__ import annotations

from typing import Any, Dict, Iterable, List

from jszahzyj.dao import jszahz_topic_relation_dao


RELATION_TYPES: Dict[str, Dict[str, str]] = {
    "case": {
        "column": "关联案件",
        "sheet": "关联案件",
        "short_label": "案件",
        "title": "关联案件明细",
        "empty_message": "未查询到该人员的关联案件数据",
    },
    "alarm": {
        "column": "关联警情",
        "sheet": "关联警情",
        "short_label": "警情",
        "title": "关联警情明细",
        "empty_message": "未查询到该人员的关联警情数据",
    },
    "vehicle": {
        "column": "关联机动车",
        "sheet": "关联机动车",
        "short_label": "机动车",
        "title": "关联机动车明细",
        "empty_message": "未查询到该人员的关联机动车数据",
    },
    "video": {
        "column": "关联视频云",
        "sheet": "关联视频云",
        "short_label": "视频云",
        "title": "关联视频云明细",
        "empty_message": "未查询到该人员的关联视频云数据",
    },
    "clinic": {
        "column": "关联门诊",
        "sheet": "关联门诊",
        "short_label": "门诊",
        "title": "关联门诊明细",
        "empty_message": "未查询到该人员的关联门诊数据",
    },
    "racing": {
        "column": "关联飙车炸街",
        "sheet": "关联飙车炸街",
        "short_label": "飙车炸街",
        "title": "关联飙车炸街明细",
        "empty_message": "未查询到该人员的关联飙车炸街数据",
    },
    "traffic": {
        "column": "关联交通违法",
        "sheet": "关联交通违法",
        "short_label": "交通违法",
        "title": "关联交通违法明细",
        "empty_message": "未查询到该人员的关联交通违法数据",
    },
}

RELATION_COLUMN_TYPES: Dict[str, str] = {
    config["column"]: relation_type for relation_type, config in RELATION_TYPES.items()
}

RELATION_QUERY_FUNC_NAMES = {
    "case": "query_case_rows",
    "alarm": "query_alarm_rows",
    "vehicle": "query_vehicle_rows",
    "video": "query_video_rows",
    "clinic": "query_clinic_rows",
    "racing": "query_racing_rows",
    "traffic": "query_traffic_rows",
}


def _normalize_zjhm(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_unique_zjhms(values: List[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for value in values or []:
        zjhm = _normalize_zjhm(value)
        if not zjhm or zjhm in seen:
            continue
        seen.add(zjhm)
        normalized.append(zjhm)
    return normalized


def normalize_relation_types(relation_types: Iterable[str] | None) -> List[str]:
    raw_values = list(relation_types or [])
    if not raw_values:
        return list(RELATION_TYPES.keys())

    selected: List[str] = []
    seen = set()
    invalid: List[str] = []
    for value in raw_values:
        relation_type = str(value or "").strip()
        if not relation_type or relation_type in seen:
            continue
        if relation_type not in RELATION_TYPES:
            invalid.append(relation_type)
            continue
        seen.add(relation_type)
        selected.append(relation_type)

    if invalid:
        raise ValueError(f"不支持的关联类型: {', '.join(invalid)}")
    return selected or list(RELATION_TYPES.keys())


def list_relation_type_options(relation_types: Iterable[str] | None = None) -> List[Dict[str, Any]]:
    selected = set(normalize_relation_types(relation_types))
    return [
        {
            "value": relation_type,
            "label": config["column"],
            "checked": relation_type in selected,
        }
        for relation_type, config in RELATION_TYPES.items()
    ]


def get_relation_columns(relation_types: Iterable[str] | None = None) -> List[str]:
    return [
        RELATION_TYPES[relation_type]["column"]
        for relation_type in normalize_relation_types(relation_types)
    ]


def get_relation_type_config(relation_type: str) -> Dict[str, str]:
    config = RELATION_TYPES.get(str(relation_type or "").strip())
    if not config:
        raise ValueError("不支持的关联类型")
    return config


def _get_relation_query_func(relation_type: str):
    return getattr(jszahz_topic_relation_dao, RELATION_QUERY_FUNC_NAMES[relation_type])


def initialize_relation_placeholders(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    initialized: List[Dict[str, Any]] = []
    for row in records or []:
        item = dict(row)
        for config in RELATION_TYPES.values():
            item.setdefault(config["column"], None)
        initialized.append(item)
    return initialized


def build_relation_count_payload(
    zjhms: List[str],
    *,
    relation_types: Iterable[str] | None = None,
) -> Dict[str, Dict[str, int]]:
    normalized = _normalize_unique_zjhms(zjhms)
    selected_relation_types = normalize_relation_types(relation_types)
    count_maps = jszahz_topic_relation_dao.query_relation_count_maps(
        normalized,
        relation_types=selected_relation_types,
    )
    return {
        relation_type: {key: int(value) for key, value in (value_map or {}).items()}
        for relation_type, value_map in count_maps.items()
    }


def attach_relation_counts(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rows = initialize_relation_placeholders(records)
    zjhms: List[str] = []

    for item in normalized_rows:
        zjhm = _normalize_zjhm(item.get("身份证号"))
        if zjhm:
            zjhms.append(zjhm)

    count_maps = build_relation_count_payload(zjhms)
    for item in normalized_rows:
        zjhm = _normalize_zjhm(item.get("身份证号"))
        for relation_type, config in RELATION_TYPES.items():
            item[config["column"]] = int(count_maps.get(relation_type, {}).get(zjhm, 0))
    return normalized_rows


def build_relation_page_payload(*, relation_type: str, zjhm: str, xm: str) -> Dict[str, Any]:
    normalized_zjhm = _normalize_zjhm(zjhm)
    if not normalized_zjhm:
        raise ValueError("身份证号不能为空")

    config = get_relation_type_config(relation_type)
    records = _get_relation_query_func(relation_type)(normalized_zjhm)
    return {
        "title": config["title"],
        "relation_type": relation_type,
        "zjhm": normalized_zjhm,
        "xm": str(xm or "").strip(),
        "records": records,
        "message": "" if records else config["empty_message"],
    }
