from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple

from gonggong.config.database import get_database_connection
from hqzcsj.dao import zfba_jq_aj_dao


REGION_ORDER: List[Tuple[str, str]] = [
    ("445302", "云城"),
    ("445303", "云安"),
    ("445381", "罗定"),
    ("445321", "新兴"),
    ("445322", "郁南"),
    ("445300", "市局"),
]

RATIO_RULES: List[Tuple[str, str, str]] = [
    ("警情", "同比警情", "起"),
    ("案件数", "同比案件数", "起"),
    ("行政", "同比行政", "起"),
    ("刑事", "同比刑事", "起"),
    ("治安处罚", "同比治安处罚", "人次"),
    ("刑拘", "同比刑拘", "人次"),
    ("逮捕", "同比逮捕", "人次"),
    ("起诉", "同比起诉", "人次"),
    ("移送案件", "同比移送案件", "起"),
    ("办结", "同比办结", "起"),
    ("破案", "同比破案", "起"),
    ("高质量", "同比高质量", "起"),
]
RATIO_RULE_BY_YOY: Dict[str, Tuple[str, str]] = {yoy_col: (cur_col, unit) for cur_col, yoy_col, unit in RATIO_RULES}


@dataclass(frozen=True)
class SummaryMeta:
    start_time: str
    end_time: str
    yoy_start_time: str
    yoy_end_time: str


def default_time_range_for_page() -> Tuple[str, str]:
    """
    默认：前7天00:00:00 到 当天00:00:00
    """
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


def _to_num(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _fmt_num(v: float) -> str:
    return str(int(v)) if float(v).is_integer() else f"{v:.2f}".rstrip("0").rstrip(".")


def calc_ratio_text(current_value: Any, yoy_value: Any, unit: str) -> str:
    current_num = _to_num(current_value)
    yoy_num = _to_num(yoy_value)

    if current_num == yoy_num:
        return "持平"
    if current_num == 0 and yoy_num != 0:
        return f"下降{_fmt_num(yoy_num)}{unit}"
    if current_num != 0 and yoy_num == 0:
        return f"上升{_fmt_num(current_num)}{unit}"

    ratio = ((current_num - yoy_num) / yoy_num) * 100
    return f"{ratio:.2f}%"


def append_ratio_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        new_row: Dict[str, Any] = {}
        for key, value in row.items():
            new_row[key] = value
            rule = RATIO_RULE_BY_YOY.get(key)
            if rule:
                current_col, unit = rule
                ratio_col = f"{key}比例"
                new_row[ratio_col] = calc_ratio_text(row.get(current_col), row.get(key), unit)
        out.append(new_row)
    return out


def shift_year(dt: datetime, years: int = -1) -> datetime:
    """
    上一年同周期：优先 replace(year-1)，若遇到 2/29 则降为 2/28。
    """
    try:
        return dt.replace(year=dt.year + years)
    except Exception:
        if dt.month == 2 and dt.day == 29:
            return dt.replace(year=dt.year + years, day=28)
        raise


def build_summary(*, start_time: str, end_time: str, leixing_list: Sequence[str], za_types: Sequence[str] = None) -> Tuple[SummaryMeta, List[Dict[str, Any]]]:
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    yoy_start = shift_year(start_dt, -1)
    yoy_end = shift_year(end_dt, -1)
    meta = SummaryMeta(
        start_time=fmt_dt(start_dt),
        end_time=fmt_dt(end_dt),
        yoy_start_time=fmt_dt(yoy_start),
        yoy_end_time=fmt_dt(yoy_end),
    )

    def _call(stage: str, fn):
        try:
            return fn()
        except Exception as exc:
            raise RuntimeError(f"{stage}查询失败: {exc}") from exc

    conn = get_database_connection()
    try:
        patterns = _call("类型映射(案由)", lambda: zfba_jq_aj_dao.fetch_ay_patterns(conn, leixing_list=leixing_list))

        leixing_list = [str(x).strip() for x in (leixing_list or []) if str(x).strip()]
        za_types = [str(x).strip() for x in (za_types or []) if str(x).strip()]

        # 当前
        jq_now = _call(
            "当前-警情",
            lambda: zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, leixing_list=leixing_list),
        )
        # 新增：案件数（不区分行政/刑事）
        ajxx_all_now = _call(
            "当前-案件数",
            lambda: zfba_jq_aj_dao.count_ajxx_all_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        ajxx_now = _call(
            "当前-案件(行政/刑事)",
            lambda: zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        # 治拘改为治安处罚，支持 za_types 过滤
        zhiju_now = _call(
            "当前-治安处罚",
            lambda: zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns, za_types=za_types),
        )
        xingju_now = _call(
            "当前-刑拘",
            lambda: zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        daibu_now = _call(
            "当前-逮捕",
            lambda: zfba_jq_aj_dao.count_dbz_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        qisu_now = _call(
            "当前-起诉",
            lambda: zfba_jq_aj_dao.count_qsryxx_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        yisong_now = _call(
            "当前-移送案件",
            lambda: zfba_jq_aj_dao.count_ysajtzs_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )
        # 新增：办结/破案（未办结口径，按案件类型拆分）
        banjie_now = _call(
            "当前-办结",
            lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(
                conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns, ajlx="行政"
            ),
        )
        poan_now = _call(
            "当前-破案",
            lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(
                conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns, ajlx="刑事"
            ),
        )
        # 新增：高质量
        gaozhiliang_now = _call(
            "当前-高质量",
            lambda: zfba_jq_aj_dao.count_gaozhiliang_by_diqu(conn, start_time=meta.start_time, end_time=meta.end_time, patterns=patterns),
        )

        # 同比（上一年同周期）
        jq_yoy = _call(
            "同比-警情",
            lambda: zfba_jq_aj_dao.count_jq_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, leixing_list=leixing_list),
        )
        # 新增：案件数同比
        ajxx_all_yoy = _call(
            "同比-案件数",
            lambda: zfba_jq_aj_dao.count_ajxx_all_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        ajxx_yoy = _call(
            "同比-案件(行政/刑事)",
            lambda: zfba_jq_aj_dao.count_ajxx_by_diqu_and_ajlx(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        # 治安处罚同比
        zhiju_yoy = _call(
            "同比-治安处罚",
            lambda: zfba_jq_aj_dao.count_xzcfjds_zhiju_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns, za_types=za_types),
        )
        xingju_yoy = _call(
            "同比-刑拘",
            lambda: zfba_jq_aj_dao.count_jlz_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        daibu_yoy = _call(
            "同比-逮捕",
            lambda: zfba_jq_aj_dao.count_dbz_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        qisu_yoy = _call(
            "同比-起诉",
            lambda: zfba_jq_aj_dao.count_qsryxx_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        yisong_yoy = _call(
            "同比-移送案件",
            lambda: zfba_jq_aj_dao.count_ysajtzs_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )
        # 新增：办结/破案同比
        banjie_yoy = _call(
            "同比-办结",
            lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(
                conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns, ajlx="行政"
            ),
        )
        poan_yoy = _call(
            "同比-破案",
            lambda: zfba_jq_aj_dao.count_ajxx_banjie_by_diqu(
                conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns, ajlx="刑事"
            ),
        )
        # 新增：高质量同比
        gaozhiliang_yoy = _call(
            "同比-高质量",
            lambda: zfba_jq_aj_dao.count_gaozhiliang_by_diqu(conn, start_time=meta.yoy_start_time, end_time=meta.yoy_end_time, patterns=patterns),
        )

        def g(m: Dict[str, int], code: str) -> int:
            return int(m.get(code) or 0)

        rows: List[Dict[str, Any]] = []
        for code, name in REGION_ORDER:
            rows.append(
                {
                    "地区": name,
                    "地区代码": code,
                    "警情": g(jq_now, code),
                    "同比警情": g(jq_yoy, code),
                    "案件数": g(ajxx_all_now, code),
                    "同比案件数": g(ajxx_all_yoy, code),
                    "行政": g(ajxx_now.get("行政", {}), code),
                    "同比行政": g(ajxx_yoy.get("行政", {}), code),
                    "刑事": g(ajxx_now.get("刑事", {}), code),
                    "同比刑事": g(ajxx_yoy.get("刑事", {}), code),
                    "治安处罚": g(zhiju_now, code),
                    "同比治安处罚": g(zhiju_yoy, code),
                    "刑拘": g(xingju_now, code),
                    "同比刑拘": g(xingju_yoy, code),
                    "逮捕": g(daibu_now, code),
                    "同比逮捕": g(daibu_yoy, code),
                    "起诉": g(qisu_now, code),
                    "同比起诉": g(qisu_yoy, code),
                    "移送案件": g(yisong_now, code),
                    "同比移送案件": g(yisong_yoy, code),
                    "办结": g(banjie_now, code),
                    "同比办结": g(banjie_yoy, code),
                    "破案": g(poan_now, code),
                    "同比破案": g(poan_yoy, code),
                    "高质量": g(gaozhiliang_now, code),
                    "同比高质量": g(gaozhiliang_yoy, code),
                }
            )

        # 全市合计（6个地区 + 市局）
        total = {"地区": "全市", "地区代码": "__ALL__"}
        for k in [
            "警情",
            "同比警情",
            "案件数",
            "同比案件数",
            "行政",
            "同比行政",
            "刑事",
            "同比刑事",
            "治安处罚",
            "同比治安处罚",
            "刑拘",
            "同比刑拘",
            "逮捕",
            "同比逮捕",
            "起诉",
            "同比起诉",
            "移送案件",
            "同比移送案件",
            "办结",
            "同比办结",
            "破案",
            "同比破案",
            "高质量",
            "同比高质量",
        ]:
            total[k] = sum(int(r.get(k) or 0) for r in rows)
        rows.append(total)

        return meta, rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def fetch_detail(
    *,
    metric: str,
    diqu: str,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    za_types: Sequence[str] = None,
    limit: int = 5000,
) -> Tuple[List[Dict[str, Any]], bool]:
    conn = get_database_connection()
    try:
        return zfba_jq_aj_dao.fetch_detail_rows(
            conn,
            metric=metric,
            diqu=diqu,
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            za_types=za_types,
            limit=limit,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

