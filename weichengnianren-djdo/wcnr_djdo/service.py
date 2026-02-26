from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Literal

from gonggong.config.database import get_database_connection
from hqzcsj.dao import jzqk_tongji_dao

from . import dao
from .constants import map_region_name


Region = Literal["云城", "云安", "罗定", "新兴", "郁南", "全市"]
REGIONS: List[Region] = ["云城", "云安", "罗定", "新兴", "郁南", "全市"]
REGIONS_JZQK: List[str] = ["云城", "云安", "罗定", "新兴", "郁南", "市局", "其他", "全市"]


@dataclass(frozen=True)
class MetricResult:
    title: str
    series: List[str]
    chart_rows: List[Dict[str, Any]]
    detail_rows: List[Dict[str, Any]]


def _pct(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return round((n / d) * 100.0, 2)


def _normalize_region(value: Any) -> str:
    mapped = str(map_region_name(value) or "").strip()
    if mapped in ("云城", "云安", "罗定", "新兴", "郁南", "市局"):
        return mapped
    return "其他"


def _region_group(detail_rows: List[Dict[str, Any]], region_order: List[str] | None = None) -> Dict[str, List[Dict[str, Any]]]:
    order = region_order or [str(r) for r in REGIONS]
    groups: Dict[str, List[Dict[str, Any]]] = {r: [] for r in order if r != "全市"}
    for r in detail_rows:
        region = _normalize_region(r.get("地区"))
        if region in groups:
            groups[region].append(r)
    return groups


def _fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _is_yes(value: Any) -> bool:
    return str(value or "").strip() == "是"


def _is_no(value: Any) -> bool:
    return str(value or "").strip() == "否"


def _to_int_or_none(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def _is_jzjy_numerator(row: Dict[str, Any]) -> bool:
    return (
        _is_yes(row.get("是否开具矫治文书"))
        or _is_yes(row.get("是否刑拘"))
        or (_is_yes(row.get("治拘大于4天")) and _is_no(row.get("是否治拘不送")))
        or _is_yes(row.get("是否送校"))
    )


def _is_sx_candidate(row: Dict[str, Any]) -> bool:
    if str(row.get("案件类型") or "").strip() != "刑事":
        return False
    if not _is_no(row.get("是否刑拘")):
        return False
    age = _to_int_or_none(row.get("年龄"))
    return age is not None and age > 12


def _is_sx_numerator(row: Dict[str, Any]) -> bool:
    return _is_sx_candidate(row) and _is_yes(row.get("是否送校"))


def _fetch_jzqk_rows(start_time: datetime, end_time: datetime, case_types: List[str] | None = None) -> List[Dict[str, Any]]:
    leixing_list = [str(t).strip() for t in (case_types or []) if str(t).strip()]
    conn = get_database_connection()
    try:
        rows = jzqk_tongji_dao.fetch_jzqk_data(
            conn,
            start_time=_fmt_dt(start_time),
            end_time=_fmt_dt(end_time),
            leixing_list=leixing_list,
        )
    finally:
        conn.close()

    for row in rows:
        row["地区"] = _normalize_region(row.get("地区"))
    return rows


def metric_jq_za(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "警情转案率"
    detail_rows = dao.query_jq_za_details(start_time, end_time, case_types)
    groups = _region_group(detail_rows)

    all_jq = {str(r.get("警情编号")) for r in detail_rows if r.get("警情编号") is not None}
    all_case = {str(r.get("案件编号")) for r in detail_rows if r.get("案件编号") not in (None, "", "空")}

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS:
        if region == "全市":
            jq = len(all_jq)
            aj = len(all_case)
        else:
            rows = groups.get(region, [])
            jq_set = {str(r.get("警情编号")) for r in rows if r.get("警情编号") is not None}
            aj_set = {str(r.get("案件编号")) for r in rows if r.get("案件编号") not in (None, "", "空")}
            jq = len(jq_set)
            aj = len(aj_set)
        chart_rows.append({"地区": region, "警情": jq, "案件": aj, "转案率": _pct(aj, jq)})

    return MetricResult(title=title, series=["警情", "案件", "转案率"], chart_rows=chart_rows, detail_rows=detail_rows)


def metric_jzjy(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "采取矫治教育措施率"
    detail_rows = _fetch_jzqk_rows(start_time, end_time, case_types)
    groups = _region_group(detail_rows, REGIONS_JZQK)

    all_total = len(detail_rows)
    all_yes = sum(1 for r in detail_rows if _is_jzjy_numerator(r))

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS_JZQK:
        if region == "全市":
            total = all_total
            yes = all_yes
        else:
            rows = groups.get(region, [])
            total = len(rows)
            yes = sum(1 for r in rows if _is_jzjy_numerator(r))
        chart_rows.append(
            {
                "地区": region,
                "应采取矫治教育措施人数": total,
                "已采取矫治教育措施人数": yes,
                "采取矫治教育措施率": _pct(yes, total),
            }
        )
    logging.info("djdo metric_jzjy: total=%s numerator=%s", all_total, all_yes)

    return MetricResult(
        title=title,
        series=["应采取矫治教育措施人数", "已采取矫治教育措施人数", "采取矫治教育措施率"],
        chart_rows=chart_rows,
        detail_rows=detail_rows,
    )


def metric_sx_sx(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "涉刑人员送学率"
    base_rows = _fetch_jzqk_rows(start_time, end_time, case_types)
    detail_rows = [r for r in base_rows if _is_sx_candidate(r)]
    groups = _region_group(detail_rows, REGIONS_JZQK)

    all_total = len(detail_rows)
    all_yes = sum(1 for r in detail_rows if _is_yes(r.get("是否送校")))

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS_JZQK:
        if region == "全市":
            total = all_total
            yes = all_yes
        else:
            rows = groups.get(region, [])
            total = len(rows)
            yes = sum(1 for r in rows if _is_yes(r.get("是否送校")))
        chart_rows.append(
            {"地区": region, "符合涉刑人员送学人数": total, "实际送学人数": yes, "涉刑人员送学率": _pct(yes, total)}
        )
    logging.info("djdo metric_sx_sx: total=%s numerator=%s", all_total, all_yes)

    return MetricResult(
        title=title,
        series=["符合涉刑人员送学人数", "实际送学人数", "涉刑人员送学率"],
        chart_rows=chart_rows,
        detail_rows=detail_rows,
    )


def metric_zljqjh(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "责令加强监护率"
    detail_rows = _fetch_jzqk_rows(start_time, end_time, case_types)
    groups = _region_group(detail_rows, REGIONS_JZQK)

    all_total = len(detail_rows)
    all_yes = sum(1 for r in detail_rows if _is_yes(r.get("是否开具家庭教育指导书")))

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS_JZQK:
        if region == "全市":
            total = all_total
            yes = all_yes
        else:
            rows = groups.get(region, [])
            total = len(rows)
            yes = sum(1 for r in rows if _is_yes(r.get("是否开具家庭教育指导书")))
        chart_rows.append({"地区": region, "应责令加强监护人数": total, "已责令加强监护人数": yes, "责令加强监护率": _pct(yes, total)})
    logging.info("djdo metric_zljqjh: total=%s numerator=%s", all_total, all_yes)

    return MetricResult(
        title=title,
        series=["应责令加强监护人数", "已责令加强监护人数", "责令加强监护率"],
        chart_rows=chart_rows,
        detail_rows=detail_rows,
    )


def metric_cs_fa(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "场所发案率"
    detail_rows = dao.query_cs_fa_details(start_time, end_time, case_types)

    # 地址分类（使用巡防模型）
    try:
        from xunfang.service.jiemiansanlei_service import predict_addresses  # type: ignore

        texts: List[str] = []
        for r in detail_rows:
            addr = (
                r.get("案件发生地址名称")
                or r.get("案件发生地址")
                or r.get("发生地址")
                or r.get("案发地址")
                or ""
            )
            texts.append(str(addr))
        preds = predict_addresses(texts)
        for r, (label, prob) in zip(detail_rows, preds):
            r["分类结果"] = label
            r["置信度"] = round(float(prob or 0.0), 5)
    except Exception:
        for r in detail_rows:
            r["分类结果"] = ""
            r["置信度"] = 0.0

    groups = _region_group(detail_rows)
    all_total = len(detail_rows)
    all_ent = sum(1 for r in detail_rows if str(r.get("分类结果") or "") == "重点管控行业")

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS:
        if region == "全市":
            total = all_total
            ent = all_ent
        else:
            rows = groups.get(region, [])
            total = len(rows)
            ent = sum(1 for r in rows if str(r.get("分类结果") or "") == "重点管控行业")
        chart_rows.append({"地区": region, "娱乐场所案件数": ent, "案件数": total, "场所发案率": _pct(ent, total)})

    return MetricResult(
        title=title,
        series=["娱乐场所案件数", "案件数", "场所发案率"],
        chart_rows=chart_rows,
        detail_rows=detail_rows,
    )


def metric_ng_zf(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    title = "纳管人员再犯率"
    detail_rows = dao.query_ng_zf_details(start_time, end_time, case_types)
    groups = _region_group(detail_rows)

    all_total = len(detail_rows)
    all_yes = sum(1 for r in detail_rows if str(r.get("是否再犯") or "") == "是")

    chart_rows: List[Dict[str, Any]] = []
    for region in REGIONS:
        if region == "全市":
            total = all_total
            yes = all_yes
        else:
            rows = groups.get(region, [])
            total = len(rows)
            yes = sum(1 for r in rows if str(r.get("是否再犯") or "") == "是")
        chart_rows.append({"地区": region, "列管人数": total, "再犯人数": yes, "再犯率": _pct(yes, total)})

    return MetricResult(
        title=title,
        series=["列管人数", "再犯人数", "再犯率"],
        chart_rows=chart_rows,
        detail_rows=detail_rows,
    )


METRICS = {
    "jq_za": metric_jq_za,
    "jzjy": metric_jzjy,
    "sx_sx": metric_sx_sx,
    "zljqjh": metric_zljqjh,
    "cs_fa": metric_cs_fa,
    "ng_zf": metric_ng_zf,
}


def get_metric(metric_key: str, start_time: datetime, end_time: datetime, case_types: List[str] = None) -> MetricResult:
    if metric_key not in METRICS:
        raise KeyError(metric_key)
    return METRICS[metric_key](start_time, end_time, case_types)


def get_responsible_sms_data(start_time: datetime, end_time: datetime, case_types: List[str] = None) -> Dict[str, Any]:
    """
    获取责任人短信数据（三个模块）

    Returns:
        {
            "jzjy": {"title": "采取矫治教育措施率", "items": [...]},
            "sx_sx": {"title": "涉刑人员送学率", "items": [...]},
            "zljqjh": {"title": "责令加强监护率", "items": [...]}
        }
    """
    from . import sms

    current_year = datetime.now().year
    modules: Dict[str, Any] = {}
    jzqk_details = _fetch_jzqk_rows(start_time, end_time, case_types)

    # 1. 采取矫治教育措施率（不满足分子条件）
    jzjy_items = []
    for record in jzqk_details:
        if _is_jzjy_numerator(record):
            continue

        case_name = str(record.get("案件名称") or "")
        person_name = str(record.get("姓名") or "")
        desensitized_case = sms.desensitize_case_name(case_name)

        # 过滤手机号
        phone_json = record.get("联系电话_json")
        mobiles = sms.filter_mobile_phones(phone_json)

        if not mobiles:
            continue

        template = (
            f"{current_year}年未成年人打架斗殴指标监测: "
            f"您办理的{desensitized_case}的{person_name}未开具"
            f"《训诫书》/《责令未成年人遵守特定行为规范通知书》【基础管控中心】"
        )

        jzjy_items.append({
            "案件名称": case_name,
            "案件名称_脱敏": desensitized_case,
            "姓名": person_name,
            "联系电话": mobiles,
            "短信模板": template,
        })

    if jzjy_items:
        modules["jzjy"] = {
            "title": "采取矫治教育措施率",
            "items": jzjy_items,
        }

    # 2. 涉刑人员送学率（候选集合中未送校）
    sx_sx_items = []
    for record in jzqk_details:
        if not _is_sx_candidate(record):
            continue
        if _is_sx_numerator(record):
            continue

        case_name = str(record.get("案件名称") or "")
        person_name = str(record.get("姓名") or "")
        desensitized_case = sms.desensitize_case_name(case_name)

        # 过滤手机号
        phone_json = record.get("联系电话_json")
        mobiles = sms.filter_mobile_phones(phone_json)

        if not mobiles:
            continue

        template = (
            f"{current_year}年未成年人打架斗殴指标监测: "
            f"您办理的{desensitized_case}的{person_name}未送方正学校【基础管控中心】"
        )

        sx_sx_items.append({
            "案件名称": case_name,
            "案件名称_脱敏": desensitized_case,
            "姓名": person_name,
            "联系电话": mobiles,
            "短信模板": template,
        })

    if sx_sx_items:
        modules["sx_sx"] = {
            "title": "涉刑人员送学率",
            "items": sx_sx_items,
        }

    # 3. 责令加强监护率（未开具家庭教育指导书）
    zljqjh_items = []
    for record in jzqk_details:
        if _is_yes(record.get("是否开具家庭教育指导书")):
            continue

        case_name = str(record.get("案件名称") or "")
        person_name = str(record.get("姓名") or "")
        desensitized_case = sms.desensitize_case_name(case_name)

        # 过滤手机号
        phone_json = record.get("联系电话_json")
        mobiles = sms.filter_mobile_phones(phone_json)

        if not mobiles:
            continue

        template = (
            f"{current_year}年未成年人打架斗殴指标监测: "
            f"您办理的{desensitized_case}的{person_name}未开具"
            f"《加强监督教育/责令接受家庭监督指导通知书》【基础管控中心】"
        )

        zljqjh_items.append({
            "案件名称": case_name,
            "案件名称_脱敏": desensitized_case,
            "姓名": person_name,
            "联系电话": mobiles,
            "短信模板": template,
        })

    if zljqjh_items:
        modules["zljqjh"] = {
            "title": "责令加强监护率",
            "items": zljqjh_items,
        }

    return modules
