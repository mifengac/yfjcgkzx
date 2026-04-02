from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from time import perf_counter
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from gonggong.config.database import get_database_connection
from xxffmk.dao import xxffmk_dao


DIMENSION_ORDER: List[Dict[str, Any]] = [
    {"key": "songsheng", "label": "累计送生人数", "base_score": 20},
    {"key": "jingqing", "label": "涉校警情", "base_score": 15},
    {"key": "tuanhuo", "label": "案件团伙数", "base_score": 15},
    {"key": "chuoxue", "label": "辍学人数", "base_score": 20},
    {"key": "yebuguisu", "label": "夜不归宿学生人数", "base_score": 10},
]
DIMENSION_CONFIG: Dict[str, Dict[str, Any]] = {item["key"]: item for item in DIMENSION_ORDER}
SCHOOL_SUFFIX_RE = re.compile(
    r"([A-Za-z0-9\u4e00-\u9fff]{2,40}(?:中学|小学|学校|幼儿园|大学|职中|职高|技工学校|技校|实验学校|职业技术学校|高级中学))"
)
SEPARATOR_RE = re.compile(r"[-—－_/|｜]")
STRIP_RE = re.compile(r"[\s\u3000`~!@#$%^&*()+=\[\]{}\\|;:'\",.<>/?，。；：、“”‘’（）【】《》、·]+")
SCHOOL_ALIAS_MAP: Dict[str, str] = {}


@dataclass
class DimensionResult:
    counts_by_school: Dict[str, int]
    detail_rows_by_school: Dict[str, List[Dict[str, Any]]]
    unmatched_rows: List[Dict[str, Any]]
    raw_keys_by_school: Dict[str, List[str]] = field(default_factory=dict)


def parse_dt(text: str) -> datetime:
    value = str(text or "").strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                return parsed.replace(hour=0, minute=0, second=0)
            if fmt == "%Y-%m-%d %H:%M":
                return parsed.replace(second=0)
            return parsed
        except ValueError:
            continue
    raise ValueError(f"时间格式错误: {text}，期望 YYYY-MM-DD HH:MM[:SS]")


def fmt_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def default_time_range_for_page() -> Tuple[str, str]:
    now = datetime.now().replace(microsecond=0)
    start = now.replace(month=1, day=1, hour=0, minute=0, second=0)
    return fmt_dt(start), fmt_dt(now)


def refresh_materialized_views() -> Dict[str, Any]:
    conn = get_database_connection()
    started_at = datetime.now()
    try:
        refreshed_sql = xxffmk_dao.refresh_materialized_views(conn)
        conn.commit()
        refreshed_views = [sql.replace('REFRESH MATERIALIZED VIEW ', '') for sql in refreshed_sql]
        elapsed_seconds = max((datetime.now() - started_at).total_seconds(), 0.0)
        return {
            "refreshed_views": refreshed_views,
            "refreshed_count": len(refreshed_views),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "message": f"已刷新 {len(refreshed_views)} 个物化视图",
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _log_timing_anchor(stage: str, timings: Mapping[str, Any], extra: Optional[Mapping[str, Any]] = None) -> None:
    ordered_keys = [
        "school_records_seconds",
        "songsheng_seconds",
        "jingqing_seconds",
        "tuanhuo_seconds",
        "chuoxue_seconds",
        "yebuguisu_seconds",
        "rank_rows_seconds",
        "filter_rows_seconds",
        "slice_rows_seconds",
        "summary_seconds",
        "load_total_seconds",
        "detail_rows_seconds",
        "total_seconds",
    ]
    parts: List[str] = []
    for key in ordered_keys:
        if key in timings:
            parts.append(f"{key}={timings[key]}s")
    if extra:
        for key, value in extra.items():
            if value is None or value == "":
                continue
            parts.append(f"{key}={value}")
    print(f"[XXFFMK][{stage}] " + " | ".join(parts), flush=True)


def normalize_school_name(text: str) -> str:
    value = unicodedata.normalize("NFKC", str(text or "")).strip().upper()
    value = STRIP_RE.sub("", value)
    return value


def _normalize_school_codes(values: Sequence[str] | None) -> List[str]:
    out: List[str] = []
    for value in values or []:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _build_alias_map() -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    for raw_key, raw_value in SCHOOL_ALIAS_MAP.items():
        key = normalize_school_name(raw_key)
        value = normalize_school_name(raw_value)
        if key and value:
            alias_map[key] = value
    return alias_map


class SchoolMatcher:
    def __init__(self, school_records: Sequence[Mapping[str, Any]]):
        self.records: List[Dict[str, Any]] = [dict(item) for item in school_records]
        self.alias_map = _build_alias_map()
        self.exact_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.records_by_code: Dict[str, Dict[str, Any]] = {}
        for record in self.records:
            xxbsm = str(record.get("xxbsm") or "").strip()
            normalized = normalize_school_name(record.get("xxmc") or "")
            if xxbsm:
                self.records_by_code[xxbsm] = record
            if normalized:
                self.exact_map[normalized].append(record)

    def candidate_texts(self, text: str) -> List[str]:
        raw_text = unicodedata.normalize("NFKC", str(text or "")).strip()
        if not raw_text:
            return []

        candidates: List[str] = []
        seen = set()

        def add(candidate: str) -> None:
            normalized = normalize_school_name(candidate)
            if normalized and normalized not in seen:
                seen.add(normalized)
                candidates.append(normalized)

        add(raw_text)
        for match in SCHOOL_SUFFIX_RE.findall(raw_text):
            add(match)
        for piece in SEPARATOR_RE.split(raw_text):
            piece = piece.strip()
            if not piece:
                continue
            add(piece)
            for match in SCHOOL_SUFFIX_RE.findall(piece):
                add(match)
        return candidates

    def match(self, text: str) -> Optional[Dict[str, Any]]:
        candidates = self.candidate_texts(text)
        for candidate in candidates:
            alias_target = self.alias_map.get(candidate)
            if alias_target:
                matched = self._match_exact(alias_target)
                if matched is not None:
                    return matched
            matched = self._match_exact(candidate)
            if matched is not None:
                return matched
        for candidate in candidates:
            fuzzy = self._match_fuzzy(candidate)
            if fuzzy is not None:
                return fuzzy
        return None

    def _match_exact(self, candidate: str) -> Optional[Dict[str, Any]]:
        matches = self.exact_map.get(candidate) or []
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return sorted(matches, key=lambda item: (str(item.get("xxmc") or ""), str(item.get("xxbsm") or "")))[0]
        return None

    def _match_fuzzy(self, candidate: str) -> Optional[Dict[str, Any]]:
        if not candidate:
            return None
        matches = []
        for record in self.records:
            school_norm = normalize_school_name(record.get("xxmc") or "")
            if not school_norm:
                continue
            if school_norm.endswith(candidate) or candidate.endswith(school_norm):
                matches.append(record)
        if len(matches) == 1:
            return matches[0]
        return None


def _rank_counts(counts_by_school: Mapping[str, int], *, base_score: int) -> Dict[str, Dict[str, Any]]:
    ordered_items = sorted(counts_by_school.items(), key=lambda item: (-int(item[1]), item[0]))
    results: Dict[str, Dict[str, Any]] = {}
    previous_value: Optional[int] = None
    current_rank = 0
    for index, (school_code, raw_value) in enumerate(ordered_items, start=1):
        value = int(raw_value or 0)
        if previous_value != value:
            current_rank = index
            previous_value = value
        results[school_code] = {
            "value": value,
            "rank": current_rank,
            "score": max(base_score - current_rank + 1, 0),
        }
    return results


def _rank_total(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered_rows = sorted(
        rows,
        key=lambda item: (
            -int(item.get("total_score", 0) or 0),
            -int(item.get("dimension_scores", {}).get("songsheng", {}).get("value", 0) or 0),
            -int(item.get("dimension_scores", {}).get("chuoxue", {}).get("value", 0) or 0),
            -int(item.get("dimension_scores", {}).get("jingqing", {}).get("value", 0) or 0),
            -int(item.get("dimension_scores", {}).get("tuanhuo", {}).get("value", 0) or 0),
            -int(item.get("dimension_scores", {}).get("yebuguisu", {}).get("value", 0) or 0),
            str(item.get("xxmc") or ""),
            str(item.get("xxbsm") or ""),
        ),
    )
    previous_score: Optional[int] = None
    current_rank = 0
    for index, row in enumerate(ordered_rows, start=1):
        total_score = int(row.get("total_score", 0) or 0)
        if previous_score != total_score:
            current_rank = index
            previous_score = total_score
        row["rank"] = current_rank
    return ordered_rows


def _append_detail_row(
    detail_rows_by_school: MutableMapping[str, List[Dict[str, Any]]],
    school_code: str,
    row: Mapping[str, Any],
) -> None:
    detail_rows_by_school.setdefault(school_code, []).append(dict(row))


def _compute_dimension1(
    conn: Any,
    matcher: SchoolMatcher,
    start_time: str,
    end_time: str,
) -> DimensionResult:
    rows = xxffmk_dao.fetch_dimension1_rows(conn, start_time, end_time)
    counts_by_school: Dict[str, int] = defaultdict(int)
    detail_rows_by_school: Dict[str, List[Dict[str, Any]]] = {}
    raw_keys_by_school: Dict[str, List[str]] = defaultdict(list)
    unmatched_rows: List[Dict[str, Any]] = []
    for row in rows:
        raw_school_name = str(row.get("raw_school_name") or "").strip()
        matched = matcher.match(raw_school_name)
        raw_count = int(row.get("raw_count", 0) or 0)
        if matched is None:
            unmatched_rows.append({"raw_school_name": raw_school_name, "count": raw_count})
            continue
        school_code = str(matched.get("xxbsm") or "").strip()
        if not school_code:
            unmatched_rows.append({"raw_school_name": raw_school_name, "count": raw_count})
            continue
        counts_by_school[school_code] += raw_count
        if raw_school_name and raw_school_name not in raw_keys_by_school[school_code]:
            raw_keys_by_school[school_code].append(raw_school_name)
        _append_detail_row(
            detail_rows_by_school,
            school_code,
            {
                "原学校名称": raw_school_name,
                "送生人数": raw_count,
                "匹配学校": matched.get("xxmc") or "",
                "学校标识码": school_code,
            },
        )
    return DimensionResult(dict(counts_by_school), detail_rows_by_school, unmatched_rows, dict(raw_keys_by_school))


def _compute_dimension2(
    conn: Any,
    matcher: SchoolMatcher,
    start_time: str,
    end_time: str,
) -> DimensionResult:
    rows = xxffmk_dao.fetch_dimension2_rows(conn, start_time, end_time)
    counts_by_school: Dict[str, int] = defaultdict(int)
    detail_rows_by_school: Dict[str, List[Dict[str, Any]]] = {}
    unmatched_counter: Dict[str, int] = defaultdict(int)
    for row in rows:
        candidate = str(row.get("extracted_school_name") or "").strip()
        matched = matcher.match(candidate)
        if matched is None:
            unmatched_counter[candidate or "未提取到学校名称"] += 1
            continue
        school_code = str(matched.get("xxbsm") or "").strip()
        if not school_code:
            unmatched_counter[candidate or "未提取到学校名称"] += 1
            continue
        counts_by_school[school_code] += 1
        detail_row = {
            "案件编号": row.get("caseno") or "",
            "报警时间": row.get("calltime") or "",
            "原始提取学校": candidate,
            "匹配学校": matched.get("xxmc") or "",
            "发生地址": row.get("occuraddress") or "",
            "警情内容": row.get("casecontents") or "",
            "回复内容": row.get("replies") or "",
        }
        _append_detail_row(detail_rows_by_school, school_code, detail_row)
    unmatched_rows = [{"raw_school_name": name, "count": count} for name, count in unmatched_counter.items()]
    return DimensionResult(dict(counts_by_school), detail_rows_by_school, unmatched_rows)


def _compute_direct_dimension(
    rows: Sequence[Mapping[str, Any]],
    *,
    count_label: str,
    row_transform: Optional[Any] = None,
) -> DimensionResult:
    counts_by_school: Dict[str, int] = defaultdict(int)
    detail_rows_by_school: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        school_code = str(row.get("xxbsm") or "").strip()
        if not school_code:
            continue
        counts_by_school[school_code] += 1
        detail = dict(row_transform(row) if row_transform else row)
        detail[count_label] = 1
        _append_detail_row(detail_rows_by_school, school_code, detail)
    return DimensionResult(dict(counts_by_school), detail_rows_by_school, [])


def _compute_dimension3(conn: Any, start_time: str, end_time: str) -> DimensionResult:
    rows = xxffmk_dao.fetch_dimension3_rows(conn, start_time, end_time)
    return _compute_direct_dimension(
        rows,
        count_label="团伙涉案人数",
        row_transform=lambda row: {
            "案件编号": row.get("ajbh") or "",
            "身份证号": row.get("sfzjh") or "",
            "姓名": row.get("xm") or "",
            "录入时间": fmt_dt(row.get("xyrxx_lrsj")) if isinstance(row.get("xyrxx_lrsj"), datetime) else row.get("xyrxx_lrsj") or "",
            "学校标识码": row.get("xxbsm") or "",
            "学校名称": row.get("xxmc") or "",
        },
    )


def _compute_dimension4(conn: Any) -> DimensionResult:
    rows = xxffmk_dao.fetch_dimension4_rows(conn)
    return _compute_direct_dimension(
        rows,
        count_label="辍学人数",
        row_transform=lambda row: {
            "身份证号": row.get("zjhm") or "",
            "姓名": row.get("xm") or "",
            "性别": row.get("xb") or "",
            "学区": row.get("xq") or "",
            "就学情况": row.get("jxqk") or "",
            "家庭住址": row.get("jtzz") or "",
            "户籍地址": row.get("hjdz") or "",
            "监护人电话": row.get("jhrdh") or "",
            "学校标识码": row.get("xxbsm") or "",
            "学校名称": row.get("xxmc") or "",
            "年级": row.get("njmc") or "",
            "班级": row.get("bjmc") or "",
        },
    )


def _compute_dimension5(conn: Any, start_time: str, end_time: str) -> DimensionResult:
    rows = xxffmk_dao.fetch_dimension5_rows(conn, start_time, end_time)
    counts_by_school: Dict[str, int] = defaultdict(int)
    detail_rows_by_school: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        school_code = str(row.get("xxbsm") or "").strip()
        if not school_code:
            continue
        counts_by_school[school_code] += 1
        _append_detail_row(
            detail_rows_by_school,
            school_code,
            {
                "身份证号": row.get("sfzjh") or "",
                "姓名": row.get("xm") or "",
                "夜间出现天数": int(row.get("night_days", 0) or 0),
                "学校标识码": school_code,
                "学校名称": row.get("xxmc") or "",
                "年级": row.get("njmc") or "",
                "班级": row.get("bjmc") or "",
            },
        )
    return DimensionResult(dict(counts_by_school), detail_rows_by_school, [])


def _load_dimension_results(
    conn: Any,
    start_time: str,
    end_time: str,
) -> Tuple[SchoolMatcher, Dict[str, Dict[str, Any]], Dict[str, DimensionResult], Dict[str, float]]:
    timings: Dict[str, float] = {}
    load_started_at = perf_counter()

    step_started_at = perf_counter()
    school_records = xxffmk_dao.fetch_school_records(conn)
    timings["school_records_seconds"] = round(perf_counter() - step_started_at, 3)
    matcher = SchoolMatcher(school_records)
    school_info_map: Dict[str, Dict[str, Any]] = dict(matcher.records_by_code)

    dimension_results: Dict[str, DimensionResult] = {}
    for dimension_key, compute_fn in (
        ("songsheng", lambda: _compute_dimension1(conn, matcher, start_time, end_time)),
        ("jingqing", lambda: _compute_dimension2(conn, matcher, start_time, end_time)),
        ("tuanhuo", lambda: _compute_dimension3(conn, start_time, end_time)),
        ("chuoxue", lambda: _compute_dimension4(conn)),
        ("yebuguisu", lambda: _compute_dimension5(conn, start_time, end_time)),
    ):
        step_started_at = perf_counter()
        dimension_results[dimension_key] = compute_fn()
        timings[f"{dimension_key}_seconds"] = round(perf_counter() - step_started_at, 3)

    for result in dimension_results.values():
        for school_code, rows in result.detail_rows_by_school.items():
            if school_code in school_info_map or not rows:
                continue
            first_row = rows[0]
            school_info_map[school_code] = {
                "xxbsm": school_code,
                "xxmc": first_row.get("学校名称") or "",
                "source_type": "",
                "normalized_xxmc": normalize_school_name(first_row.get("学校名称") or ""),
            }
    timings["load_total_seconds"] = round(perf_counter() - load_started_at, 3)
    return matcher, school_info_map, dimension_results, timings


def _build_rank_rows(
    school_info_map: Mapping[str, Mapping[str, Any]],
    dimension_results: Mapping[str, DimensionResult],
) -> List[Dict[str, Any]]:
    dimension_scores = {
        key: _rank_counts(result.counts_by_school, base_score=int(DIMENSION_CONFIG[key]["base_score"]))
        for key, result in dimension_results.items()
    }

    school_codes = set()
    for result in dimension_results.values():
        school_codes.update(result.counts_by_school.keys())

    rows: List[Dict[str, Any]] = []
    for school_code in school_codes:
        school_info = dict(school_info_map.get(school_code) or {"xxbsm": school_code, "xxmc": ""})
        per_dimension: Dict[str, Dict[str, Any]] = {}
        total_score = 0
        for config in DIMENSION_ORDER:
            dimension_key = str(config["key"])
            payload = dict(dimension_scores.get(dimension_key, {}).get(school_code) or {"value": 0, "rank": None, "score": 0})
            payload["label"] = config["label"]
            payload["base_score"] = config["base_score"]
            per_dimension[dimension_key] = payload
            total_score += int(payload.get("score", 0) or 0)
        rows.append(
            {
                "xxbsm": school_code,
                "xxmc": school_info.get("xxmc") or "",
                "source_type": school_info.get("source_type") or "",
                "dimension_scores": per_dimension,
                "total_score": total_score,
            }
        )
    return _rank_total(rows)


def _filter_rank_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    school_codes: Sequence[str] | None,
    school_name: str,
) -> List[Dict[str, Any]]:
    normalized_codes = set(_normalize_school_codes(school_codes))
    normalized_school_name = normalize_school_name(school_name)
    filtered_rows = []
    for row in rows:
        school_code = str(row.get("xxbsm") or "").strip()
        school_label = str(row.get("xxmc") or "")
        if normalized_codes and school_code not in normalized_codes:
            continue
        if normalized_school_name and normalized_school_name not in normalize_school_name(school_label):
            continue
        filtered_rows.append(row)
    return filtered_rows


def build_rank_payload(
    *,
    start_time: str,
    end_time: str,
    limit: int,
    school_codes: Sequence[str] | None = None,
    school_name: str = "",
) -> Dict[str, Any]:
    overall_started_at = perf_counter()
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    conn = get_database_connection()
    try:
        _matcher, school_info_map, dimension_results, timings = _load_dimension_results(conn, fmt_dt(start_dt), fmt_dt(end_dt))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    rows_started_at = perf_counter()
    all_rows = _build_rank_rows(school_info_map, dimension_results)
    timings["rank_rows_seconds"] = round(perf_counter() - rows_started_at, 3)

    filter_started_at = perf_counter()
    filtered_rows = _filter_rank_rows(all_rows, school_codes=school_codes, school_name=school_name)
    timings["filter_rows_seconds"] = round(perf_counter() - filter_started_at, 3)

    sort_started_at = perf_counter()
    row_limit = max(1, int(limit or 10))
    display_rows = filtered_rows[:row_limit]
    timings["slice_rows_seconds"] = round(perf_counter() - sort_started_at, 3)

    summary_started_at = perf_counter()
    unmatched_summary = {
        "songsheng": xxffmk_dao.summarize_unmatched(dimension_results["songsheng"].unmatched_rows, name_key="raw_school_name"),
        "jingqing": xxffmk_dao.summarize_unmatched(dimension_results["jingqing"].unmatched_rows, name_key="raw_school_name"),
    }
    timings["summary_seconds"] = round(perf_counter() - summary_started_at, 3)
    timings["total_seconds"] = round(perf_counter() - overall_started_at, 3)
    _log_timing_anchor(
        "rank",
        timings,
        {
            "schools": len(display_rows),
            "filtered_total": len(filtered_rows),
        },
    )
    return {
        "filters": {
            "beginDate": fmt_dt(start_dt),
            "endDate": fmt_dt(end_dt),
            "limit": row_limit,
            "school_codes": _normalize_school_codes(school_codes),
            "school_name": str(school_name or "").strip(),
        },
        "dimension_order": DIMENSION_ORDER,
        "rows": display_rows,
        "total": len(filtered_rows),
        "unmatched_summary": unmatched_summary,
        "timings": timings,
    }


def get_school_detail(
    *,
    xxbsm: str,
    start_time: str,
    end_time: str,
) -> Dict[str, Any]:
    payload = build_rank_payload(start_time=start_time, end_time=end_time, limit=100000, school_codes=None, school_name="")
    target_code = str(xxbsm or "").strip()
    for row in payload["rows"]:
        if str(row.get("xxbsm") or "").strip() == target_code:
            return {
                "school": row,
                "dimension_order": payload["dimension_order"],
                "filters": payload["filters"],
            }
    raise LookupError("未找到对应学校")


def _dimension_detail_rows(
    *,
    dimension: str,
    xxbsm: str,
    start_time: str,
    end_time: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    detail_started_at = perf_counter()
    start_dt = parse_dt(start_time)
    end_dt = parse_dt(end_time)
    if end_dt < start_dt:
        raise ValueError("结束时间不能早于开始时间")

    conn = get_database_connection()
    try:
        matcher, _school_info_map, dimension_results, _timings = _load_dimension_results(conn, fmt_dt(start_dt), fmt_dt(end_dt))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    dimension_key = str(dimension or "").strip()
    if dimension_key not in DIMENSION_CONFIG:
        raise ValueError("未知维度")
    result = dimension_results[dimension_key]
    school_code = str(xxbsm or "").strip()
    rows: List[Dict[str, Any]] = []
    if dimension_key == "songsheng":
        raw_school_names = list(dict.fromkeys(result.raw_keys_by_school.get(school_code) or []))
        if raw_school_names:
            conn = get_database_connection()
            try:
                rows = xxffmk_dao.fetch_dimension1_detail_rows(conn, fmt_dt(start_dt), fmt_dt(end_dt), raw_school_names)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        if not rows:
            rows = list(result.detail_rows_by_school.get(school_code) or [])
    else:
        rows = list(result.detail_rows_by_school.get(school_code) or [])
    unmatched_rows = []
    if dimension_key in ("songsheng", "jingqing"):
        unmatched_rows = result.unmatched_rows
    _ = matcher
    timings = {
        "detail_rows_seconds": round(perf_counter() - detail_started_at, 3),
    }
    _log_timing_anchor(f"detail:{dimension_key}", timings, {"school_code": school_code, "rows": len(rows)})
    return rows, unmatched_rows


def get_dimension_detail(
    *,
    dimension: str,
    xxbsm: str,
    start_time: str,
    end_time: str,
    page: int,
    page_size: int,
) -> Dict[str, Any]:
    rows, unmatched_rows = _dimension_detail_rows(
        dimension=dimension,
        xxbsm=xxbsm,
        start_time=start_time,
        end_time=end_time,
    )
    paged = xxffmk_dao.paginate_rows(rows, page, page_size)
    columns = list(paged["rows"][0].keys()) if paged["rows"] else list(rows[0].keys()) if rows else []
    return {
        "dimension": DIMENSION_CONFIG[dimension]["label"],
        "dimension_key": dimension,
        "columns": columns,
        "rows": paged["rows"],
        "total": paged["total"],
        "page": paged["page"],
        "page_size": paged["page_size"],
        "unmatched_summary": xxffmk_dao.summarize_unmatched(unmatched_rows, name_key="raw_school_name") if unmatched_rows else [],
    }
