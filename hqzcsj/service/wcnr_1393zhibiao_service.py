from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import jzqk_tongji_dao
from hqzcsj.dao import wcnr_1393zhibiao_dao
from hqzcsj.service.wcnr_1393_rate_utils import calc_rate_stats_bundle


@dataclass(frozen=True)
class MetricDef:
    metric: str
    label: str
    kind: str  # "count" | "rate"


LABEL_WFZF = "违法犯罪未成年人"
LABEL_JYH_CF = "结业后再犯数(犯罪)"
LABEL_JYH_WFZF = "结业后再犯数(违法犯罪)"
LABEL_CS_BQH = "未成年人场所被侵害发案数"
LABEL_BQH = "未成年人被侵害发案数"
LABEL_YZBL_XZ = "严重不良未成年人矫治教育覆盖率(行政)"
LABEL_YZBL_XS = "严重不良未成年人矫治教育覆盖率(刑事)"
LABEL_SYZMJ_XZ = "适用专门（矫治）教育情形送矫率(行政)"
LABEL_SYZMJ_XS = "适用专门（矫治）教育情形送矫率(刑事)"
LABEL_ZLJQJH_XZ = "责令加强监护率(行政)"
LABEL_ZLJQJH_XS = "责令加强监护率(刑事)"


METRICS: List[MetricDef] = [
    MetricDef("wfzf", LABEL_WFZF, "count"),
    MetricDef("jyh_cf", LABEL_JYH_CF, "count"),
    MetricDef("jyh_wfzf", LABEL_JYH_WFZF, "count"),
    MetricDef("cs_bqh", LABEL_CS_BQH, "count"),
    MetricDef("bqh", LABEL_BQH, "count"),
    MetricDef("yzbl_cover_xz", LABEL_YZBL_XZ, "rate"),
    MetricDef("yzbl_cover_xs", LABEL_YZBL_XS, "rate"),
    MetricDef("syzmj_songjiao_xz", LABEL_SYZMJ_XZ, "rate"),
    MetricDef("syzmj_songjiao_xs", LABEL_SYZMJ_XS, "rate"),
    MetricDef("zljqjh_rate_xz", LABEL_ZLJQJH_XZ, "rate"),
    MetricDef("zljqjh_rate_xs", LABEL_ZLJQJH_XS, "rate"),
]


DIQU_NAME_MAP = {
    "445302": "云城",
    "445303": "云安",
    "445381": "罗定",
    "445321": "新兴",
    "445322": "郁南",
    "445300": "市局",
}
DIQU_ORDER = ["445302", "445303", "445381", "445321", "445322", "445300"]


def default_time_range_for_page() -> Tuple[str, str]:
    """默认时间范围：7天前到当天 00:00:00"""
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today0 - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
    end = today0.strftime("%Y-%m-%d 00:00:00")
    return start, end


def parse_dt(s: str) -> datetime:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    raise ValueError(f"时间格式错误: {s}（期望 YYYY-MM-DD HH:MM:SS）")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_rate(num: int, denom: int) -> str:
    if not denom:
        return "0.00%"
    return f"{(num / denom) * 100:.2f}%"


def map_diqu_name(code: Any) -> str:
    s = str(code or "").strip()
    if not s:
        return "未知"
    return DIQU_NAME_MAP.get(s, s)


def _count_by_diqu(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        code = str(r.get("地区") or "").strip() or "未知"
        out[code] = out.get(code, 0) + 1
    return out


def _append_addr_predictions(rows: List[Dict[str, Any]], *, addr_col: str = "发案地点") -> None:
    try:
        from xunfang.service.jiemiansanlei_service import predict_addresses
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"无法加载地址分类模型（xunfang/5lei_dizhi_model）：{exc}") from exc

    texts = [str((r.get(addr_col) or "")).strip() for r in rows]
    preds = predict_addresses(texts)
    for r, (label, prob) in zip(rows, preds):
        r["分类结果"] = str(label or "").strip()
        try:
            r["置信度"] = f"{float(prob):.5f}"
        except Exception:
            r["置信度"] = "0.00000"


def build_summary(
    *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    meta = {"start_time": fmt_dt(start_dt), "end_time": fmt_dt(end_dt)}
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    conn = get_database_connection()
    try:
        wfzf_by, wfzf_total = wcnr_1393zhibiao_dao.count_wfzf_wcnr_by_diqu(
            conn, start_time=meta["start_time"], end_time=meta["end_time"], leixing_list=leixing_list
        )

        jyh_cf_by, jyh_cf_total = wcnr_1393zhibiao_dao.count_jyh_after_cases_by_diqu(
            conn,
            start_date=start_date,
            end_date=end_date,
            leixing_list=leixing_list,
            only_xingshi=True,
        )
        jyh_wfzf_by, jyh_wfzf_total = wcnr_1393zhibiao_dao.count_jyh_after_cases_by_diqu(
            conn,
            start_date=start_date,
            end_date=end_date,
            leixing_list=leixing_list,
            only_xingshi=False,
        )

        bqh_base_rows = wcnr_1393zhibiao_dao.fetch_bqh_ajxx_base_detail(
            conn, start_time=meta["start_time"], end_time=meta["end_time"], leixing_list=leixing_list, diqu=None
        )
        bqh_by = _count_by_diqu(bqh_base_rows)
        bqh_total = len(bqh_base_rows)

        # 场所被侵害：在原始结果集上做地址分类二次过滤
        cs_rows = [dict(r) for r in bqh_base_rows]
        _append_addr_predictions(cs_rows, addr_col="发案地点")
        cs_rows = [r for r in cs_rows if str(r.get("分类结果") or "").strip() == "重点管控行业"]
        cs_by = _count_by_diqu(cs_rows)
        cs_total = len(cs_rows)

        # 基于矫治情况明细数据源的比率指标（统一在 helper 中计算）
        rate_rows_all = jzqk_tongji_dao.fetch_jzqk_data(
            conn, start_time=meta["start_time"], end_time=meta["end_time"], leixing_list=leixing_list
        )
        rate_stats = calc_rate_stats_bundle(rate_rows_all)
        yz_xz_num_by, yz_xz_denom_by, yz_xz_num_total, yz_xz_denom_total = rate_stats["yzbl_cover_xz"]
        yz_xs_num_by, yz_xs_denom_by, yz_xs_num_total, yz_xs_denom_total = rate_stats["yzbl_cover_xs"]
        sj_xz_num_by, sj_xz_denom_by, sj_xz_num_total, sj_xz_denom_total = rate_stats["syzmj_songjiao_xz"]
        sj_xs_num_by, sj_xs_denom_by, sj_xs_num_total, sj_xs_denom_total = rate_stats["syzmj_songjiao_xs"]
        jl_xz_num_by, jl_xz_denom_by, jl_xz_num_total, jl_xz_denom_total = rate_stats["zljqjh_rate_xz"]
        jl_xs_num_by, jl_xs_denom_by, jl_xs_num_total, jl_xs_denom_total = rate_stats["zljqjh_rate_xs"]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    all_codes = set()
    for d in (wfzf_by, jyh_cf_by, jyh_wfzf_by, cs_by, bqh_by, 
              yz_xz_denom_by, yz_xs_denom_by, 
              sj_xz_num_by, sj_xz_denom_by, sj_xs_num_by, sj_xs_denom_by, 
              jl_xz_num_by, jl_xz_denom_by, jl_xs_num_by, jl_xs_denom_by):
        all_codes.update(d.keys())
    ordered_codes = [c for c in DIQU_ORDER if c in all_codes]
    rest_codes = sorted([c for c in all_codes if c not in set(DIQU_ORDER)])
    codes = ordered_codes + rest_codes

    rows: List[Dict[str, Any]] = []
    for code in codes:
        rows.append(
            {
                "地区": map_diqu_name(code),
                "__diqu_code": code,
                LABEL_WFZF: int(wfzf_by.get(code, 0)),
                LABEL_JYH_CF: int(jyh_cf_by.get(code, 0)),
                LABEL_JYH_WFZF: int(jyh_wfzf_by.get(code, 0)),
                LABEL_CS_BQH: int(cs_by.get(code, 0)),
                LABEL_BQH: int(bqh_by.get(code, 0)),
                LABEL_YZBL_XZ: fmt_rate(
                    int(yz_xz_num_by.get(code, 0)), int(yz_xz_denom_by.get(code, 0))
                ),
                LABEL_YZBL_XS: fmt_rate(
                    int(yz_xs_num_by.get(code, 0)), int(yz_xs_denom_by.get(code, 0))
                ),
                LABEL_SYZMJ_XZ: fmt_rate(
                    int(sj_xz_num_by.get(code, 0)), int(sj_xz_denom_by.get(code, 0))
                ),
                LABEL_SYZMJ_XS: fmt_rate(
                    int(sj_xs_num_by.get(code, 0)), int(sj_xs_denom_by.get(code, 0))
                ),
                LABEL_ZLJQJH_XZ: fmt_rate(
                    int(jl_xz_num_by.get(code, 0)), int(jl_xz_denom_by.get(code, 0))
                ),
                LABEL_ZLJQJH_XS: fmt_rate(
                    int(jl_xs_num_by.get(code, 0)), int(jl_xs_denom_by.get(code, 0))
                ),
            }
        )

    rows.append(
        {
            "地区": "全市",
            "__diqu_code": "ALL",
            LABEL_WFZF: wfzf_total,
            LABEL_JYH_CF: jyh_cf_total,
            LABEL_JYH_WFZF: jyh_wfzf_total,
            LABEL_CS_BQH: cs_total,
            LABEL_BQH: bqh_total,
            LABEL_YZBL_XZ: fmt_rate(yz_xz_num_total, yz_xz_denom_total),
            LABEL_YZBL_XS: fmt_rate(yz_xs_num_total, yz_xs_denom_total),
            LABEL_SYZMJ_XZ: fmt_rate(sj_xz_num_total, sj_xz_denom_total),
            LABEL_SYZMJ_XS: fmt_rate(sj_xs_num_total, sj_xs_denom_total),
            LABEL_ZLJQJH_XZ: fmt_rate(jl_xz_num_total, jl_xz_denom_total),
            LABEL_ZLJQJH_XS: fmt_rate(jl_xs_num_total, jl_xs_denom_total),
        }
    )

    return meta, rows


def fetch_detail(
    *, metric: str, start_time: str, end_time: str, leixing_list: Sequence[str], diqu: str | None
) -> List[Dict[str, Any]]:
    metric = str(metric or "").strip()
    if not metric:
        raise ValueError("metric 不能为空")

    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    meta_start = fmt_dt(start_dt)
    meta_end = fmt_dt(end_dt)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    conn = get_database_connection()
    try:
        if metric == "wfzf":
            rows = wcnr_1393zhibiao_dao.fetch_wfzf_wcnr_detail(
                conn,
                start_time=meta_start,
                end_time=meta_end,
                leixing_list=leixing_list,
                diqu=diqu,
            )
        elif metric == "jyh_cf":
            rows = wcnr_1393zhibiao_dao.fetch_jyh_after_cases_detail(
                conn,
                start_date=start_date,
                end_date=end_date,
                leixing_list=leixing_list,
                only_xingshi=True,
                diqu=diqu,
            )
        elif metric == "jyh_wfzf":
            rows = wcnr_1393zhibiao_dao.fetch_jyh_after_cases_detail(
                conn,
                start_date=start_date,
                end_date=end_date,
                leixing_list=leixing_list,
                only_xingshi=False,
                diqu=diqu,
            )
        elif metric in ("bqh", "cs_bqh"):
            base_rows = wcnr_1393zhibiao_dao.fetch_bqh_ajxx_base_detail(
                conn,
                start_time=meta_start,
                end_time=meta_end,
                leixing_list=leixing_list,
                diqu=diqu,
            )
            if metric == "bqh":
                rows = base_rows
            else:
                rows = [dict(r) for r in base_rows]
                _append_addr_predictions(rows, addr_col="发案地点")
                rows = [r for r in rows if str(r.get("分类结果") or "").strip() == "重点管控行业"]
        elif metric in ("yzbl_cover_xz", "yzbl_cover_xs", 
                        "syzmj_songjiao_xz", "syzmj_songjiao_xs", 
                        "zljqjh_rate_xz", "zljqjh_rate_xs"):
            rate_rows = jzqk_tongji_dao.fetch_jzqk_data(
                conn, start_time=meta_start, end_time=meta_end, leixing_list=leixing_list
            )
            if diqu and str(diqu).strip() and str(diqu).strip().upper() != "ALL":
                code = str(diqu).strip()
                rate_rows = [r for r in rate_rows if str(r.get("地区") or "").strip() == code]
            # 根据 metric 过滤案件类型
            if metric.endswith("_xz"):
                rate_rows = [r for r in rate_rows if str(r.get("案件类型") or "").strip() == "行政"]
            elif metric.endswith("_xs"):
                rate_rows = [r for r in rate_rows if str(r.get("案件类型") or "").strip() == "刑事"]
            rows = rate_rows
        else:
            raise ValueError(f"不支持的 metric: {metric}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 地区映射显示
    for r in rows:
        if "地区" in r:
            r["地区"] = map_diqu_name(r.get("地区"))
    return rows


def metric_label(metric: str) -> str:
    for m in METRICS:
        if m.metric == metric:
            return m.label
    return metric


def fetch_all_details(
    *, start_time: str, end_time: str, leixing_list: Sequence[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    获取所有数据源明细（用于“导出详情”），单次连接复用，避免重复查询/重复模型推理。
    返回：{metric: rows}
    """
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    meta_start = fmt_dt(start_dt)
    meta_end = fmt_dt(end_dt)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    conn = get_database_connection()
    try:
        wfzf_rows = wcnr_1393zhibiao_dao.fetch_wfzf_wcnr_detail(
            conn, start_time=meta_start, end_time=meta_end, leixing_list=leixing_list, diqu="ALL"
        )

        jyh_cf_rows = wcnr_1393zhibiao_dao.fetch_jyh_after_cases_detail(
            conn,
            start_date=start_date,
            end_date=end_date,
            leixing_list=leixing_list,
            only_xingshi=True,
            diqu="ALL",
        )
        jyh_wfzf_rows = wcnr_1393zhibiao_dao.fetch_jyh_after_cases_detail(
            conn,
            start_date=start_date,
            end_date=end_date,
            leixing_list=leixing_list,
            only_xingshi=False,
            diqu="ALL",
        )

        bqh_base_rows = wcnr_1393zhibiao_dao.fetch_bqh_ajxx_base_detail(
            conn, start_time=meta_start, end_time=meta_end, leixing_list=leixing_list, diqu="ALL"
        )
        bqh_rows = bqh_base_rows

        cs_rows = [dict(r) for r in bqh_base_rows]
        _append_addr_predictions(cs_rows, addr_col="发案地点")
        cs_rows = [r for r in cs_rows if str(r.get("分类结果") or "").strip() == "重点管控行业"]

        rate_rows = jzqk_tongji_dao.fetch_jzqk_data(
            conn, start_time=meta_start, end_time=meta_end, leixing_list=leixing_list
        )
        yzbl_xz_rows = [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "行政"]
        yzbl_xs_rows = [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "刑事"]
        sj_xz_rows = [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "行政"]
        sj_xs_rows = [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "刑事"]
    finally:
        try:
            conn.close()
        except Exception:
            pass

    metric_rows: Dict[str, List[Dict[str, Any]]] = {
        "wfzf": wfzf_rows,
        "jyh_cf": jyh_cf_rows,
        "jyh_wfzf": jyh_wfzf_rows,
        "cs_bqh": cs_rows,
        "bqh": bqh_rows,
        "yzbl_cover_xz": yzbl_xz_rows,
        "yzbl_cover_xs": yzbl_xs_rows,
        "syzmj_songjiao_xz": sj_xz_rows,
        "syzmj_songjiao_xs": sj_xs_rows,
        "zljqjh_rate_xz": [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "行政"],
        "zljqjh_rate_xs": [dict(r) for r in rate_rows if str(r.get("案件类型") or "").strip() == "刑事"],
    }

    for rows in metric_rows.values():
        for r in rows:
            if "地区" in r:
                r["地区"] = map_diqu_name(r.get("地区"))

    return metric_rows
