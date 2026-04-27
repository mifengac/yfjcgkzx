from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Sequence, Tuple


GAMBLING_WAY_RULES: List[Tuple[str, List[str]]] = [
    ("麻将", ["麻将", "打麻将", "麻将档", "麻将台", "麻将机", "麻雀"]),
    ("三公", ["三公", "赌三公", "三公牌", "三张牌"]),
    ("番摊/翻摊", ["番摊", "翻摊", "番摊赌博", "翻摊赌博"]),
    ("斗牛", ["斗牛", "牛牛", "玩牛牛", "斗牛赌博"]),
    ("扑克", ["扑克", "扑克牌", "打扑克", "炸金花", "德州扑克", "斗地主", "梭哈", "十三水", "跑得快"]),
    ("牌九", ["牌九", "推牌九", "骨牌"]),
    ("纸牌", ["纸牌", "打纸牌", "纸牌赌博", "牌局"]),
    ("六合彩", ["六合彩", "六和彩", "地下六合彩", "买码", "报码", "特码", "私彩"]),
    ("网络赌博", ["网络赌博", "网上赌博", "手机赌博", "赌博网站", "赌博APP", "赌博平台", "线上赌博", "网赌", "百家乐", "赌球"]),
    ("老虎机", ["老虎机", "赌博机", "电子游戏机", "电玩赌博", "捕鱼机", "打鱼机"]),
]

WILDERNESS_KEYWORDS = [
    "山腰",
    "山顶",
    "山脚",
    "山上",
    "山林",
    "树林",
    "林地",
    "林区",
    "竹林",
    "果林",
    "荒山",
    "山坡",
    "山边",
    "山坳",
    "山沟",
    "山谷",
    "半山",
    "山路",
    "林间",
    "林边",
    "野外",
    "郊外",
    "荒地",
]

GAMBLING_VENUE_KEYWORDS = [
    "棋牌室",
    "棋牌馆",
    "棋牌店",
    "棋牌会所",
    "棋牌娱乐",
    "棋牌",
    "麻将馆",
    "麻将室",
    "麻将档",
    "麻将店",
    "麻将房",
    "麻将铺",
    "麻将机房",
    "麻雀馆",
    "麻雀档",
    "麻雀室",
    "小卖部",
    "士多店",
    "便利店",
    "杂货店",
    "副食店",
    "食杂店",
    "日杂店",
    "烟酒店",
    "商店",
    "小店",
    "超市",
]

GAMBLING_WAY_TITLE = "赌博方式"
GAMBLING_WILDERNESS_TITLE = "涉山林野外赌博"
GAMBLING_VENUE_TITLE = "棋牌室/麻将馆/小卖部"
GAMBLING_VENUE_SHEET_TITLE = "棋牌麻将小卖部"
FIELD_LABELS = {
    "caseContents": "报警内容",
    "replies": "处警情况",
    "occurAddress": "警情地址",
}


def _region_name(row: Mapping[str, Any]) -> str:
    return str(row.get("cmdName") or row.get("cmdId") or "未知地区").strip() or "未知地区"


def _cmd_id(row: Mapping[str, Any]) -> str:
    return str(row.get("cmdId") or row.get("cmdName") or "未知编码").strip() or "未知编码"


def _find_keywords(text: Any, keywords: Sequence[str]) -> List[str]:
    content = str(text or "")
    hits: List[str] = []
    seen = set()
    for keyword in keywords:
        if keyword and keyword in content and keyword not in seen:
            if any(keyword in hit for hit in hits):
                continue
            hits = [hit for hit in hits if hit not in keyword]
            seen.add(keyword)
            hits.append(keyword)
    return hits


def _match_gambling_way(text: Any) -> List[Tuple[str, List[str]]]:
    matched: List[Tuple[str, List[str]]] = []
    for label, keywords in GAMBLING_WAY_RULES:
        hits = _find_keywords(text, keywords)
        if hits:
            matched.append((label, hits))
    return matched


def summarize_gambling_way_by_region(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    labels = [label for label, _keywords in GAMBLING_WAY_RULES]
    region_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    details: List[Dict[str, Any]] = []

    for row in rows:
        matches = _match_gambling_way(row.get("caseContents"))
        if not matches:
            row["gamblingWayLabels"] = ""
            row["gamblingWayKeywords"] = ""
            continue
        region = _region_name(row)
        labels_hit: List[str] = []
        keywords_hit: List[str] = []
        for label, keywords in matches:
            region_counts[region][label] += 1
            labels_hit.append(label)
            keywords_hit.extend(keywords)
        row["gamblingWayLabels"] = "、".join(labels_hit)
        row["gamblingWayKeywords"] = "、".join(dict.fromkeys(keywords_hit))
        details.append(row)

    table_rows = []
    for region, counts in region_counts.items():
        total = sum(counts.values())
        table_rows.append(
            {
                "cmdName": region,
                "counts": {label: counts.get(label, 0) for label in labels},
                "total": total,
            }
        )
    table_rows.sort(key=lambda item: (-item["total"], item["cmdName"]))
    return {"columns": labels, "rows": table_rows, "details": details}


def summarize_wilderness_by_region(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    region_counts: Dict[str, int] = defaultdict(int)
    details: List[Dict[str, Any]] = []
    for row in rows:
        hits = _find_keywords(row.get("replies"), WILDERNESS_KEYWORDS)
        if not hits:
            row["gamblingWildernessKeywords"] = ""
            continue
        region = _region_name(row)
        region_counts[region] += 1
        row["gamblingWildernessKeywords"] = "、".join(hits)
        details.append(row)

    table_rows = [
        {"cmdName": region, "total": count}
        for region, count in sorted(region_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {"rows": table_rows, "details": details}


def summarize_venue_by_cmd_id(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    region_counts: Dict[str, Dict[str, Any]] = {}
    details: List[Dict[str, Any]] = []

    for row in rows:
        field_hits: List[str] = []
        keyword_hits: List[str] = []
        for field_name in ("caseContents", "replies", "occurAddress"):
            hits = _find_keywords(row.get(field_name), GAMBLING_VENUE_KEYWORDS)
            if not hits:
                continue
            field_hits.append(FIELD_LABELS[field_name])
            keyword_hits.extend(hits)
        if not keyword_hits:
            row["gamblingVenueFields"] = ""
            row["gamblingVenueKeywords"] = ""
            continue

        cmd_id = _cmd_id(row)
        item = region_counts.setdefault(cmd_id, {"cmdId": cmd_id, "cmdName": _region_name(row), "total": 0})
        item["total"] += 1
        if item.get("cmdName") == "未知地区" and row.get("cmdName"):
            item["cmdName"] = _region_name(row)
        row["gamblingVenueFields"] = "、".join(dict.fromkeys(field_hits))
        row["gamblingVenueKeywords"] = "、".join(dict.fromkeys(keyword_hits))
        details.append(row)

    table_rows = sorted(region_counts.values(), key=lambda item: (-item["total"], item["cmdId"]))
    return {"rows": table_rows, "details": details}
