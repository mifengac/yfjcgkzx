from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Mapping, Sequence, Tuple


EFFECTIVE_KEYWORDS = [
    "立案",
    "受理行政",
    "治安案件",
    "查处",
    "查获",
    "传唤",
    "口头传唤",
    "书面传唤",
    "行政拘留",
    "处罚",
    "罚款",
    "刑拘",
    "拘留",
    "抓获",
    "带回",
    "移交",
    "扣押",
    "收缴",
    "供认不讳",
]
LEFT_SCENE_KEYWORDS = [
    "已离开",
    "已散",
    "散去",
    "跑了",
    "逃跑",
    "人已走",
    "未抓获",
    "未发现现行",
    "现场无人",
    "未发现",
    "没有发现",
    "暂未发现",
    "未见",
    "未营业",
]
NOISE_KEYWORDS = ["噪音", "扰民", "太吵", "声音", "影响休息", "麻将声", "打牌声", "吵闹"]
FALSE_KEYWORDS = ["误报", "虚假", "无此人", "核实无", "无赌博", "不是赌博", "没有赌博", "未发现报警情况"]
COMPETITION_KEYWORDS = ["同行", "竞争", "抢生意", "恶意举报", "报复", "矛盾", "纠纷", "投诉"]
ONLINE_KEYWORDS = ["网络", "网上", "手机", "APP", "平台", "网赌", "微信", "赌博网站", "下注", "二维码", "转账"]

GAMBLING_WAY_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("麻将", ("麻将", "转转麻将", "打麻将", "麻将桌", "麻将馆", "麻将机")),
    ("扑克", ("扑克", "扑克牌", "纸牌", "打牌", "炸金花", "德州扑克", "斗地主", "十三水")),
    ("六合彩", ("六合彩", "地下六合彩", "买码", "报码", "特码", "私彩")),
    ("斗牛", ("斗牛", "牛牛")),
    ("三公", ("三公", "赌三公")),
    ("牌九", ("牌九", "推牌九", "骨牌")),
    ("翻摊/番摊", ("翻摊", "番摊")),
    ("鱼虾蟹", ("鱼虾蟹",)),
    ("网络赌博", ("网络赌博", "网上赌博", "手机赌博", "赌博APP", "赌博平台", "网赌", "百家乐", "足球赌博", "赌博网站")),
    ("赌博机", ("老虎机", "赌博机", "电子游戏机", "捕鱼机", "打鱼机")),
)

VENUE_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("棋牌室/麻将馆", ("棋牌室", "棋牌馆", "棋牌", "麻将馆", "麻将室", "麻将房", "麻将机房")),
    ("小卖部/商铺", ("小卖部", "士多店", "便利店", "杂货店", "日杂店", "烟酒店", "商店", "商行", "店铺", "超市")),
    ("宾馆公寓/住宿", ("公寓", "宾馆", "酒店", "旅店", "民宿", "网约房")),
    ("出租屋/民宅", ("出租屋", "民宅", "居民楼", "出租房", "住宅", "家中")),
    ("村居/祠堂", ("农村", "村委", "村庄", "村里", "村口", "自然村", "祠堂")),
    ("工地/厂房", ("工地", "建筑", "工棚", "厂房", "工厂")),
    ("娱乐服务场所", ("酒吧", "KTV", "会所", "娱乐场所", "茶楼", "茶室")),
    ("山林野外", ("山腰", "山顶", "山脚", "山上", "树林", "林地", "竹林", "野外", "荒地")),
    ("户外空地/塘边", ("空地", "塘边", "鱼塘", "路边", "河边")),
    ("网络空间", ("网络", "网上", "手机", "APP", "平台", "微信", "赌博网站", "下注")),
)

EVIDENCE_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("赌资赌具", ("赌资", "赌具", "筹码", "现金", "收缴", "扣押", "查扣")),
    ("牌具工具", ("麻将牌", "麻将机", "扑克牌", "纸牌", "鱼虾蟹纸板", "赌博工具")),
    ("赌博痕迹", ("赌博痕迹", "胶凳", "桌椅", "纸板")),
    ("抽水营利", ("抽水", "抽头", "获利", "几万元", "资金大")),
    ("电子网络", ("微信", "手机", "赌博网站", "赌博APP", "平台", "下注", "转账")),
)

REPORT_RISK_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("有人看风/盯梢", ("看风", "放风", "盯梢", "望风")),
    ("现场有监控", ("监控", "门口装有监控")),
    ("多人聚赌", ("多人", "聚众", "十多人", "十几个人", "10几个人", "20-30人", "15人")),
    ("大额赌资/抽水", ("赌资大", "资金大", "几万元", "抽水")),
    ("匿名或要求保密", ("匿名", "保密", "不愿透露")),
)

QUALITY_RULES: Sequence[Tuple[str, Sequence[str]]] = (
    ("位置不详", ("位置不详", "不清楚具体位置", "无法获取准确", "未告知具体", "具体位置不详")),
    ("情况不清", ("不清楚", "具体情况不清楚", "情况不清", "赌资不详", "人数不详")),
    ("匿名保密", ("匿名", "要求保密", "设置报警人信息保密", "不愿透露")),
    ("线索较完整", ("方式：", "地点：", "嫌疑人信息", "现场情况", "赌资", "参与人数", "看风", "盯梢")),
)


def contains_any(text: str, keywords: Iterable[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def classify_effective(text: str) -> str:
    if contains_any(text, EFFECTIVE_KEYWORDS):
        return "有效违法警情"
    if contains_any(text, LEFT_SCENE_KEYWORDS):
        return "疑似违法但未抓现行"
    if contains_any(text, FALSE_KEYWORDS):
        return "核实无违法行为"
    if not text.strip():
        return "无反馈待核查"
    return "其他未分类"


def classify_problem_signal(text: str) -> str:
    if contains_any(text, NOISE_KEYWORDS):
        return "噪音扰民/非赌诉求"
    if contains_any(text, COMPETITION_KEYWORDS):
        return "同行竞争/纠纷类"
    if contains_any(text, ONLINE_KEYWORDS):
        return "网络/手机赌博"
    if contains_any(text, FALSE_KEYWORDS):
        return "误报/虚假/核实无"
    if contains_any(text, EFFECTIVE_KEYWORDS):
        return "查处转化"
    if contains_any(text, LEFT_SCENE_KEYWORDS):
        return "到场已散/未抓现行"
    return "其他/待核查"


def match_label(text: str, rules: Sequence[Tuple[str, Sequence[str]]]) -> str:
    labels = [label for label, keywords in rules if contains_any(text, keywords)]
    return "、".join(labels) if labels else "未识别"


def build_text_features(row: Mapping[str, Any]) -> Dict[str, Any]:
    content = _first(row, "caseContents", "content", "alarmContent")
    replies = _first(row, "replies", "reply", "feedback")
    address = _first(row, "occurAddress", "occurAddressNorm", "address")
    content_text = str(content or "")
    reply_text = str(replies or "")
    reply_core_text = _clean_reply_for_analysis(reply_text)
    reply_for_result = reply_core_text or reply_text
    all_text = " ".join([content_text, reply_core_text, str(address or "")])

    return {
        "effective_class": classify_effective(all_text),
        "problem_signal": classify_problem_signal(all_text),
        "gambling_way": match_label(all_text, GAMBLING_WAY_RULES),
        "venue_type": match_label(all_text, VENUE_RULES),
        "disposal_result": _classify_disposal_result(reply_for_result),
        "disposal_evidence": _classify_disposal_evidence(reply_for_result),
        "evidence_signal": match_label(all_text, EVIDENCE_RULES),
        "report_risk_signal": match_label(content_text, REPORT_RISK_RULES),
        "report_quality_signal": _classify_report_quality(content_text),
        "watchout_signal": _watchout_signal(content_text),
        "gambling_scale_signal": _scale_signal(content_text),
        "profit_signal": _profit_signal(all_text),
        "consistency_signal": _consistency_signal(content_text, reply_for_result),
        "content_len": len(content_text),
        "reply_len": len(reply_text),
    }


def _clean_reply_for_analysis(reply_text: str) -> str:
    markers = (
        "【过程反馈】",
        "【结警反馈】",
        "【到场反馈】",
        "补充【处理结果说明】",
        "处理结果说明",
        "出警处置情况说明",
    )
    drop_markers = (
        "警情送达",
        "警务通",
        "自动发在线移动终端",
        "视频通话",
        "发起视频",
        "派警至管辖单位",
        "签收警情",
        "保存警情",
        "与报警人的通话结束",
        "选择管辖单位",
    )
    lines = []
    for raw_line in str(reply_text or "").splitlines():
        line = re.sub(r"^\[[^\]]+\]\s*", "", raw_line).strip()
        if not line:
            continue
        if any(marker in line for marker in markers):
            lines.append(line)
            continue
        if any(marker in line for marker in drop_markers):
            continue
        if any(keyword in line for keyword in EFFECTIVE_KEYWORDS + LEFT_SCENE_KEYWORDS + list(FALSE_KEYWORDS)):
            lines.append(line)
    return "\n".join(lines)


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _classify_disposal_result(reply_text: str) -> str:
    if contains_any(reply_text, ("受理行政", "治安案件", "立案")):
        return "受理案件"
    if contains_any(reply_text, ("查获", "查处", "传唤", "抓获", "带回")):
        return "查获传唤"
    if contains_any(reply_text, ("处罚", "罚款", "拘留")):
        return "处罚拘留"
    if contains_any(reply_text, ("已散", "散去", "已离开", "现场无人")):
        return "到场已散"
    if contains_any(reply_text, ("未发现", "没有发现", "暂未发现", "未见", "未营业")):
        return "到场未发现"
    if contains_any(reply_text, ("摸排", "巡逻", "继续跟进", "核查", "线索研判")):
        return "摸排跟进"
    if contains_any(reply_text, ("派警", "警情送达", "签收警情")):
        return "已派警待反馈"
    if not reply_text.strip():
        return "无反馈"
    return "其他处置"


def _classify_disposal_evidence(reply_text: str) -> str:
    if contains_any(reply_text, ("查扣赌资", "赌资赌具", "收缴", "扣押", "筹码", "现金")):
        return "查扣赌资赌具"
    if contains_any(reply_text, ("麻将牌", "麻将机", "扑克牌", "纸牌", "鱼虾蟹纸板", "赌具")):
        return "发现赌具"
    if contains_any(reply_text, ("赌博痕迹", "胶凳", "纸板", "桌椅")):
        return "发现疑似痕迹"
    if contains_any(reply_text, ("无发现有现金", "没有发现其他人员和赌具", "没有发现可疑", "未发现")):
        return "未发现证据"
    return "未提及证据"


def _classify_report_quality(content_text: str) -> str:
    if not content_text.strip():
        return "报警内容为空"
    labels = [label for label, keywords in QUALITY_RULES if contains_any(content_text, keywords)]
    if "线索较完整" in labels and len(labels) == 1:
        return "线索较完整"
    if labels:
        return "、".join(labels)
    return "普通线索"


def _watchout_signal(content_text: str) -> str:
    if contains_any(content_text, ("看风", "放风", "盯梢", "望风")):
        return "有看风盯梢"
    if contains_any(content_text, ("无人看风", "没有人盯梢", "无人放风")):
        return "明确无看风盯梢"
    return "未提及"


def _scale_signal(content_text: str) -> str:
    text = str(content_text or "")
    if re.search(r"\d+\s*-\s*\d+\s*人", text):
        return "明确人数区间"
    if re.search(r"\d+\s*(几|多)?\s*人", text) or contains_any(text, ("十几个人", "十多人", "10几个人")):
        return "明确人数"
    if contains_any(text, ("多人", "人员多", "聚众")):
        return "多人聚集"
    return "未提及"


def _profit_signal(text: str) -> str:
    if contains_any(text, ("抽水", "抽头")):
        return "抽水营利"
    if contains_any(text, ("几万元", "资金大", "赌资大")):
        return "大额赌资"
    if contains_any(text, ("赌资", "现金", "筹码")):
        return "提及赌资"
    return "未提及"


def _consistency_signal(content_text: str, reply_text: str) -> str:
    report_has_gambling = contains_any(content_text, ("赌博", "打麻将", "三公", "聚赌", "赌资", "打牌", "抽水"))
    if contains_any(reply_text, ("查获", "查处", "受理行政", "治安案件", "传唤", "处罚", "罚款")):
        return "举报查实或转案件"
    if report_has_gambling and contains_any(reply_text, ("未发现", "没有发现", "暂未发现", "未见", "已散", "散去")):
        return "举报后未抓现行"
    if report_has_gambling and contains_any(reply_text, ("摸排", "跟进", "巡逻", "核查")):
        return "举报线索待跟进"
    if report_has_gambling and contains_any(reply_text, ("派警", "签收警情", "警情送达")):
        return "已派警未闭环"
    return "其他"
