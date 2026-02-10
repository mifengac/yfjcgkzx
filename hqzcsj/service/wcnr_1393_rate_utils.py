from __future__ import annotations

from typing import Any, Dict, Sequence, Tuple


def _count_by_diqu(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        code = str(r.get("地区") or "").strip() or "未知"
        out[code] = out.get(code, 0) + 1
    return out


def _is_yes(v: Any) -> bool:
    return str(v or "").strip() == "是"


def calc_songjiao_stats(
    rate_rows_all: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int], int, int]:
    """
    送矫率口径：
    - 分子：是否送校=是（不限定是否符合送生）
    - 分母：是否符合送生=是
    """
    sj_num_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否送校"))])
    sj_denom_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否符合送生"))])
    sj_num_total = sum(sj_num_by.values())
    sj_denom_total = sum(sj_denom_by.values())
    return sj_num_by, sj_denom_by, sj_num_total, sj_denom_total


def calc_rate_stats_bundle(
    rate_rows_all: Sequence[Dict[str, Any]],
    *,
    wfzf_by: Dict[str, int],
    wfzf_total: int,
) -> Dict[str, Tuple[Dict[str, int], Dict[str, int], int, int]]:
    """
    统一计算 1393 中基于 jzqk 明细源的比率统计。

    返回：
    - yzbl_cover: (num_by, denom_by, num_total, denom_total)
    - syzmj_songjiao: (num_by, denom_by, num_total, denom_total)
    - zljqjh_rate: (num_by, denom_by, num_total, denom_total)
    """
    # 严重不良未成年人矫治教育覆盖率：是否开具矫治文书 / 明细总数
    yz_num_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否开具矫治文书"))])
    yz_denom_by = _count_by_diqu(rate_rows_all)
    yz_num_total = sum(yz_num_by.values())
    yz_denom_total = sum(yz_denom_by.values())

    # 适用专门（矫治）教育情形送矫率：是否送校 / 是否符合送生
    sj_num_by, sj_denom_by, sj_num_total, sj_denom_total = calc_songjiao_stats(rate_rows_all)

    # 责令加强监护率：是否开具家庭教育指导书 / 违法犯罪未成年人（wfzf）
    jl_num_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否开具家庭教育指导书"))])
    jl_denom_by = dict(wfzf_by or {})
    jl_num_total = sum(jl_num_by.values())
    jl_denom_total = int(wfzf_total or 0)

    return {
        "yzbl_cover": (yz_num_by, yz_denom_by, yz_num_total, yz_denom_total),
        "syzmj_songjiao": (sj_num_by, sj_denom_by, sj_num_total, sj_denom_total),
        "zljqjh_rate": (jl_num_by, jl_denom_by, jl_num_total, jl_denom_total),
    }
