from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List

from jszahzyj.dao import jszahz_topic_relation_dao

logger = logging.getLogger(__name__)


RELATION_TYPES: Dict[str, Dict[str, str]] = {
    "case": {
        "column": "关联案件",
        "title": "关联案件明细",
        "empty_message": "未查询到该人员的关联案件数据",
    },
    "alarm": {
        "column": "关联警情",
        "title": "关联警情明细",
        "empty_message": "未查询到该人员的关联警情数据",
    },
    "vehicle": {
        "column": "关联机动车",
        "title": "关联机动车明细",
        "empty_message": "未查询到该人员的关联机动车数据",
    },
    "video": {
        "column": "关联视频云",
        "title": "关联视频云明细",
        "empty_message": "未查询到该人员的关联视频云数据",
    },
    "clinic": {
        "column": "关联门诊",
        "title": "关联门诊明细",
        "empty_message": "未查询到该人员的关联门诊数据",
    },
    "racing": {
        "column": "关联飙车炸街",
        "title": "关联飙车炸街明细",
        "empty_message": "未查询到该人员的关联飙车炸街数据",
    },
}

RELATION_COLUMN_TYPES: Dict[str, str] = {
    config["column"]: relation_type for relation_type, config in RELATION_TYPES.items()
}


def _normalize_zjhm(value: Any) -> str:
    return str(value or "").strip().upper()


def get_relation_type_config(relation_type: str) -> Dict[str, str]:
    config = RELATION_TYPES.get(str(relation_type or "").strip())
    if not config:
        raise ValueError("不支持的关联类型")
    return config


def _get_relation_query_func(relation_type: str):
    return {
        "case": jszahz_topic_relation_dao.query_case_rows,
        "alarm": jszahz_topic_relation_dao.query_alarm_rows,
        "vehicle": jszahz_topic_relation_dao.query_vehicle_rows,
        "video": jszahz_topic_relation_dao.query_video_rows,
        "clinic": jszahz_topic_relation_dao.query_clinic_rows,
        "racing": jszahz_topic_relation_dao.query_racing_rows,
    }[relation_type]


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
    debug_token: str = "",
) -> Dict[str, Dict[str, int]]:
    started_at = perf_counter()
    normalized: List[str] = []
    seen = set()
    for value in zjhms or []:
        zjhm = _normalize_zjhm(value)
        if not zjhm or zjhm in seen:
            continue
        seen.add(zjhm)
        normalized.append(zjhm)
    logger.warning(
        "[JSZAHZ_BACKEND][%s] service.build_relation_count_payload:start raw_count=%s normalized_count=%s",
        debug_token or "no-token",
        len(zjhms or []),
        len(normalized),
    )
    count_maps = jszahz_topic_relation_dao.query_relation_count_maps(normalized)
    result = {
        relation_type: {key: int(value) for key, value in (value_map or {}).items()}
        for relation_type, value_map in count_maps.items()
    }
    logger.warning(
        "[JSZAHZ_BACKEND][%s] service.build_relation_count_payload:done duration=%.3fs case=%s alarm=%s vehicle=%s video=%s clinic=%s racing=%s",
        debug_token or "no-token",
        perf_counter() - started_at,
        len(result.get("case") or {}),
        len(result.get("alarm") or {}),
        len(result.get("vehicle") or {}),
        len(result.get("video") or {}),
        len(result.get("clinic") or {}),
        len(result.get("racing") or {}),
    )
    return result


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
