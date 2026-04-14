from __future__ import annotations


TOPIC_BRANCH_SHEET_CODE_MAP = {
    "云城": "445302000000",
    "云安": "445303000000",
    "罗定": "445381000000",
    "新兴": "445321000000",
    "郁南": "445322000000",
}

TOPIC_BRANCH_SHEETS = tuple(TOPIC_BRANCH_SHEET_CODE_MAP.keys())
TOPIC_BRANCH_CODES = tuple(TOPIC_BRANCH_SHEET_CODE_MAP.values())

PERSON_TYPE_OPTIONS = [
    "不规律服药",
    "弱监护",
    "无监护",
    "既往有严重自杀或伤人行为",
    "列为重点关注人员",
]

PERSON_TYPE_ORDER = {
    label: index
    for index, label in enumerate(PERSON_TYPE_OPTIONS, start=1)
}

RISK_OPTIONS = [
    "0级患者",
    "1级患者",
    "2级患者",
    "3级患者",
    "4级患者",
    "5级患者",
    "无数据",
]