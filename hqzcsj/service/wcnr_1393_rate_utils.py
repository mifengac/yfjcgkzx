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


def _is_songxiao_or_juliu(row: Dict[str, Any]) -> bool:
    # 兼容不同字段命名：是否拘留 / 是否刑拘
    return (
        _is_yes(row.get("是否送校"))
        or _is_yes(row.get("是否拘留"))
        or _is_yes(row.get("是否刑拘"))
    )


def calc_songjiao_stats(
    rate_rows_all: Sequence[Dict[str, Any]],
    *,
    include_juliu: bool = False,
) -> Tuple[Dict[str, int], Dict[str, int], int, int]:
    """
    送矫率口径：
    - 分子：默认=是否送校=是；当 include_juliu=True 时，分子=是否送校=是 或 是否拘留/是否刑拘=是
    - 分母：是否符合送生=是
    """
    if include_juliu:
        sj_num_by = _count_by_diqu([r for r in rate_rows_all if _is_songxiao_or_juliu(r)])
    else:
        sj_num_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否送校"))])
    sj_denom_by = _count_by_diqu([r for r in rate_rows_all if _is_yes(r.get("是否符合送生"))])
    sj_num_total = sum(sj_num_by.values())
    sj_denom_total = sum(sj_denom_by.values())
    return sj_num_by, sj_denom_by, sj_num_total, sj_denom_total


def calc_rate_stats_bundle(
    rate_rows_all: Sequence[Dict[str, Any]],
) -> Dict[str, Tuple[Dict[str, int], Dict[str, int], int, int]]:
    """
    统一计算 1393 中基于 jzqk 明细源的比率统计。

    返回：
    - yzbl_cover_xz: (num_by, denom_by, num_total, denom_total) 行政
    - yzbl_cover_xs: (num_by, denom_by, num_total, denom_total) 刑事
    - syzmj_songjiao_xz: (num_by, denom_by, num_total, denom_total) 行政
    - syzmj_songjiao_xs: (num_by, denom_by, num_total, denom_total) 刑事
    - zljqjh_rate_xz: (num_by, denom_by, num_total, denom_total) 行政
    - zljqjh_rate_xs: (num_by, denom_by, num_total, denom_total) 刑事
    """
    # 分离行政和刑事数据
    xz_rows = [r for r in rate_rows_all if str(r.get("案件类型") or "").strip() == "行政"]
    xs_rows = [r for r in rate_rows_all if str(r.get("案件类型") or "").strip() == "刑事"]

    # 严重不良未成年人矫治教育覆盖率：是否开具矫治文书 / 明细总数
    # 行政
    yz_xz_num_by = _count_by_diqu([r for r in xz_rows if _is_yes(r.get("是否开具矫治文书"))])
    yz_xz_denom_by = _count_by_diqu(xz_rows)
    yz_xz_num_total = sum(yz_xz_num_by.values())
    yz_xz_denom_total = sum(yz_xz_denom_by.values())
    # 刑事
    yz_xs_num_by = _count_by_diqu([r for r in xs_rows if _is_yes(r.get("是否开具矫治文书"))])
    yz_xs_denom_by = _count_by_diqu(xs_rows)
    yz_xs_num_total = sum(yz_xs_num_by.values())
    yz_xs_denom_total = sum(yz_xs_denom_by.values())

    # 适用专门（矫治）教育情形送矫率：是否送校 / 是否符合送生
    # 行政
    sj_xz_num_by, sj_xz_denom_by, sj_xz_num_total, sj_xz_denom_total = calc_songjiao_stats(xz_rows)
    # 刑事（分子：是否送校=是 或 是否拘留/是否刑拘=是）
    sj_xs_num_by, sj_xs_denom_by, sj_xs_num_total, sj_xs_denom_total = calc_songjiao_stats(
        xs_rows, include_juliu=True
    )

    # 责令加强监护率：是否开具家庭教育指导书 / 同案件类型明细总数
    # 行政
    jl_xz_num_by = _count_by_diqu([r for r in xz_rows if _is_yes(r.get("是否开具家庭教育指导书"))])
    jl_xz_denom_by = _count_by_diqu(xz_rows)
    jl_xz_num_total = sum(jl_xz_num_by.values())
    jl_xz_denom_total = sum(jl_xz_denom_by.values())
    # 刑事
    jl_xs_num_by = _count_by_diqu([r for r in xs_rows if _is_yes(r.get("是否开具家庭教育指导书"))])
    jl_xs_denom_by = _count_by_diqu(xs_rows)
    jl_xs_num_total = sum(jl_xs_num_by.values())
    jl_xs_denom_total = sum(jl_xs_denom_by.values())

    return {
        "yzbl_cover_xz": (yz_xz_num_by, yz_xz_denom_by, yz_xz_num_total, yz_xz_denom_total),
        "yzbl_cover_xs": (yz_xs_num_by, yz_xs_denom_by, yz_xs_num_total, yz_xs_denom_total),
        "syzmj_songjiao_xz": (sj_xz_num_by, sj_xz_denom_by, sj_xz_num_total, sj_xz_denom_total),
        "syzmj_songjiao_xs": (sj_xs_num_by, sj_xs_denom_by, sj_xs_num_total, sj_xs_denom_total),
        "zljqjh_rate_xz": (jl_xz_num_by, jl_xz_denom_by, jl_xz_num_total, jl_xz_denom_total),
        "zljqjh_rate_xs": (jl_xs_num_by, jl_xs_denom_by, jl_xs_num_total, jl_xs_denom_total),
    }
