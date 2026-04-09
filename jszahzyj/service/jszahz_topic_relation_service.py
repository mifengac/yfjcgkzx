from __future__ import annotations

from typing import Any, Dict, List

from jszahzyj.dao import jszahz_topic_relation_dao


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


def append_relation_columns(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enhanced_rows: List[Dict[str, Any]] = []
    for row in records or []:
        item = dict(row)
        for config in RELATION_TYPES.values():
            item.setdefault(config["column"], None)
        enhanced_rows.append(item)
    return enhanced_rows


def build_relation_page_payload(*, relation_type: str, zjhm: str, xm: str) -> Dict[str, Any]:
    normalized_zjhm = _normalize_zjhm(zjhm)
    if not normalized_zjhm:
        raise ValueError("身份证号不能为空")

    config = get_relation_type_config(relation_type)
    query_func = {
        "case": jszahz_topic_relation_dao.query_case_rows,
        "alarm": jszahz_topic_relation_dao.query_alarm_rows,
        "vehicle": jszahz_topic_relation_dao.query_vehicle_rows,
        "video": jszahz_topic_relation_dao.query_video_rows,
        "clinic": jszahz_topic_relation_dao.query_clinic_rows,
    }[relation_type]
    records = query_func(normalized_zjhm)
    return {
        "title": config["title"],
        "relation_type": relation_type,
        "zjhm": normalized_zjhm,
        "xm": str(xm or "").strip(),
        "records": records,
        "message": "" if records else config["empty_message"],
    }
