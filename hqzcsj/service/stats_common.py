from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, List, Sequence, Tuple


def normalize_text_list(values: Sequence[str] | None) -> List[str]:
    return [text for text in (str(value or "").strip() for value in (values or [])) if text]


def default_recent_time_window(*, days: int = 7) -> Tuple[str, str]:
    today0 = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start = (today0 - timedelta(days=days)).strftime("%Y-%m-%d 00:00:00")
    end = today0.strftime("%Y-%m-%d 00:00:00")
    return start, end


def parse_dt(text: str) -> datetime:
    content = str(text or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(content, fmt)
        except Exception:
            continue
    raise ValueError(f"时间格式错误: {text}（期望 YYYY-MM-DD HH:MM:SS）")


def fmt_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def shift_year(value: datetime, years: int = -1) -> datetime:
    try:
        return value.replace(year=value.year + years)
    except Exception:
        if value.month == 2 and value.day == 29:
            return value.replace(year=value.year + years, day=28)
        raise


def infer_hb_range(start_dt: datetime, end_dt: datetime) -> Tuple[datetime, datetime]:
    delta = end_dt - start_dt
    hb_end = start_dt - timedelta(seconds=1)
    hb_start = hb_end - delta
    return hb_start, hb_end


def _to_num(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _fmt_num(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:.2f}".rstrip("0").rstrip(".")


def calc_percent_text(numerator: Any, denominator: Any) -> str:
    den = _to_num(denominator)
    if den <= 0:
        return "0.00%"
    num = _to_num(numerator)
    return f"{(num / den) * 100:.2f}%"


def calc_ratio_text(current_value: Any, compare_value: Any, unit: str) -> str:
    current_num = _to_num(current_value)
    compare_num = _to_num(compare_value)

    if current_num == compare_num:
        return "持平"
    if current_num == 0 and compare_num != 0:
        return f"下降{_fmt_num(compare_num)}{unit}"
    if current_num != 0 and compare_num == 0:
        return f"上升{_fmt_num(current_num)}{unit}"

    ratio = ((current_num - compare_num) / compare_num) * 100
    return f"{ratio:.2f}%"
