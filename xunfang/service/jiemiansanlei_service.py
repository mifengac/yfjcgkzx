"""
街面三类警情（地址分类）业务逻辑层。

流程：
1) 按条件从数据库读取警情（警情地址）。
2) 使用本地训练好的 5 类地址分类模型预测类别与置信度。
3) 返回给前端展示，或按组合导出 xlsx/xls。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from openpyxl import Workbook

from xunfang.dao.jiemiansanlei_dao import (
    JiemianSanleiQuery,
    SourceType,
    count_jingqings,
    fetch_jingqings,
    fetch_jingqings_for_export,
    list_case_types,
)

ExportFormat = Literal["xlsx", "xls"]
ReportBureau = Literal[
    "云城分局",
    "云安分局",
    "罗定市公安局",
    "新兴县公安局",
    "郁南县公安局",
    "ALL",
]

_MODEL_LOCK = threading.Lock()
_MODEL_BUNDLE: Optional["ModelBundle"] = None


@dataclass(frozen=True)
class ModelBundle:
    tokenizer: Any
    model: Any
    id2label: Dict[int, str]
    device: str


def get_case_types() -> List[str]:
    return list_case_types()


def query_classified(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    source_list: Sequence[SourceType],
    page: int,
    page_size: Optional[int],
) -> Dict[str, Any]:
    total = count_jingqings(
        JiemianSanleiQuery(
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            source_list=source_list,
            limit=None,
            offset=0,
        )
    )

    if page_size is None:
        limit = None
        offset = 0
        page = 1
    else:
        page = max(1, int(page or 1))
        limit = max(1, int(page_size))
        offset = (page - 1) * limit

    rows = fetch_jingqings(
        JiemianSanleiQuery(
            start_time=start_time,
            end_time=end_time,
            leixing_list=leixing_list,
            source_list=source_list,
            limit=limit,
            offset=offset,
        )
    )
    _append_predictions(rows)

    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        out_rows.append(
            {
                "警情性质": r.get("leixing") or "",
                "警情性质口径": r.get("yuanshiqueren") or "",
                "分局": r.get("分局") or "",
                "派出所编号": r.get("派出所编号") or "",
                "派出所名称": r.get("派出所名称") or "",
                "报警时间": _format_dt(r.get("报警时间")),
                "警情地址": r.get("警情地址") or "",
                "经度": _format_coord(r.get("经度")),
                "纬度": _format_coord(r.get("纬度")),
                "警情类型": r.get("jq_type") or "",
                "分类结果": r.get("pred_label") or "",
                "置信度": _format_prob(r.get("pred_prob")),
            }
        )

    return {"total": total, "page": page, "page_size": page_size, "rows": out_rows}


def export_classified(
    *,
    start_time: str,
    end_time: str,
    leixing_list: Sequence[str],
    source_list: Sequence[SourceType],
    fmt: ExportFormat,
) -> Tuple[bytes, str, str]:
    """
    返回：(文件 bytes, mimetype, filename)
    - 每个“原始/确认 × 警情性质”一个 sheet
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"街面三类警情地址分类{timestamp}.{fmt}"

    combos: List[Tuple[SourceType, str]] = []
    for source in source_list:
        for leixing in leixing_list:
            combos.append((source, leixing))

    if fmt == "xlsx":
        bio = BytesIO()
        wb = _build_xlsx_workbook(start_time, end_time, combos)
        wb.save(bio)
        bio.seek(0)
        return (
            bio.read(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename,
        )

    xls_bytes = _build_xls_bytes(start_time, end_time, combos)
    return xls_bytes, "application/vnd.ms-excel", filename


def export_report(
    *,
    start_time: str,
    end_time: str,
    hb_start_time: str,
    hb_end_time: str,
) -> Tuple[bytes, str, str]:
    """
    使用模板导出“街面三类警情统计表”（xlsx）。

    仅统计：
    - 警情性质：人身伤害类 / 侵犯财产类 / 扰乱秩序类
    - 口径：原始 / 确认
    - 分类结果严格等于：街面与公共区域
    - caseno 作为唯一标识去重（同一口径内）
    """
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"缺少依赖 openpyxl，无法导出报表：{exc}") from exc

    current_start = _parse_dt(start_time)
    current_end = _parse_dt(end_time)
    hb_start = _parse_dt(hb_start_time)
    hb_end = _parse_dt(hb_end_time)

    if current_start >= current_end:
        raise ValueError("开始时间必须早于结束时间")
    if hb_start >= hb_end:
        raise ValueError("环比开始必须早于环比结束")

    # 2.3：同比（针对 2.1）
    yoy_current_start = _shift_year(current_start, -1)
    yoy_current_end = _shift_year(current_end, -1)

    # 2.4：今年 1 月 1 日至 end_time
    ytd_start = datetime(current_end.year, 1, 1, 0, 0, 0)
    x_days = (current_end - ytd_start).days

    # 2.5：环比 2.4（今年 1 月 1 日向前推 x 天至今年 1 月 1 日）
    hb_ytd_start = ytd_start - timedelta(days=x_days)
    hb_ytd_end = ytd_start

    # 2.6：同比（针对 2.4）
    yoy_ytd_start = _shift_year(ytd_start, -1)
    yoy_ytd_end = _shift_year(current_end, -1)

    # 仅 2 次取数：一次取“今年窗口”（覆盖 2.1/2.2/2.4/2.5），一次取“去年窗口”（覆盖 2.3/2.6）
    leixing_list = ["人身伤害类", "侵犯财产类", "扰乱秩序类"]
    source_list: List[SourceType] = ["原始", "确认"]

    year_window_start = min(current_start, hb_start, ytd_start, hb_ytd_start)
    year_window_end = current_end
    last_year_window_start = yoy_ytd_start
    last_year_window_end = yoy_ytd_end

    rows_year = fetch_jingqings(
        JiemianSanleiQuery(
            start_time=_format_dt(year_window_start),
            end_time=_format_dt(year_window_end),
            leixing_list=leixing_list,
            source_list=source_list,
            limit=None,
            offset=0,
        )
    )
    _append_predictions(rows_year)

    rows_last_year = fetch_jingqings(
        JiemianSanleiQuery(
            start_time=_format_dt(last_year_window_start),
            end_time=_format_dt(last_year_window_end),
            leixing_list=leixing_list,
            source_list=source_list,
            limit=None,
            offset=0,
        )
    )
    _append_predictions(rows_last_year)

    counts, missing_sheets = _build_report_counts(
        rows_year=rows_year,
        rows_last_year=rows_last_year,
        segments_year=[
            ("current", current_start, current_end, "C", "D"),
            ("hb", hb_start, hb_end, "K", "N"),
            ("ytd", ytd_start, current_end, "Q", "R"),
            ("hb_ytd", hb_ytd_start, hb_ytd_end, "Y", "AB"),
        ],
        segments_last_year=[
            ("yoy_current", yoy_current_start, yoy_current_end, "E", "F"),
            ("yoy_ytd", yoy_ytd_start, yoy_ytd_end, "S", "T"),
        ],
    )

    template_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "templates", "jiemiansanleijingqing_template.xlsx")
    )
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"未找到报表模板文件：{template_path}")

    wb = load_workbook(template_path)

    time_range_text = f"{_format_zh_date(current_start)}-{_format_zh_date(current_end)}"
    for sheet_name in wb.sheetnames:
        try:
            wb[sheet_name]["A6"].value = time_range_text
        except Exception:
            continue

    expected_sheets = ["人身伤害类", "侵犯财产类", "扰乱秩序类", "三类合计"]
    for sheet_name in expected_sheets:
        if sheet_name not in wb.sheetnames:
            missing_sheets.append(sheet_name)
            continue

        ws = wb[sheet_name]
        for bureau, row_idx in _REPORT_BUREAU_ROW.items():
            for col in _REPORT_COLS:
                key = (sheet_name, bureau, col)
                ws[f"{col}{row_idx}"].value = int(counts.get(key, 0))

    if missing_sheets:
        missing = "、".join(sorted(set(missing_sheets)))
        raise RuntimeError(f"模板缺少 sheet：{missing}")

    filename = f"{_safe_filename_part(start_time)}-{_safe_filename_part(end_time)}_街面三类警情统计表.xlsx"
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return (
        bio.read(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename,
    )


def _build_xlsx_workbook(
    start_time: str,
    end_time: str,
    combos: Sequence[Tuple[SourceType, str]],
) -> Workbook:
    wb = Workbook()
    wb.remove(wb.active)

    for source, leixing in combos:
        rows = fetch_jingqings_for_export(
            start_time=start_time,
            end_time=end_time,
            leixing=leixing,
            source=source,
        )
        _append_predictions(rows)
        sheet_name = _safe_sheet_name(f"{source}{leixing}地址分类")
        ws = wb.create_sheet(title=sheet_name)
        _write_table_xlsx(ws, rows)

    return wb


def _build_xls_bytes(
    start_time: str,
    end_time: str,
    combos: Sequence[Tuple[SourceType, str]],
) -> bytes:
    try:
        import xlwt  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"缺少依赖 xlwt，无法导出 xls：{exc}") from exc

    wb = xlwt.Workbook(encoding="utf-8")
    for source, leixing in combos:
        rows = fetch_jingqings_for_export(
            start_time=start_time,
            end_time=end_time,
            leixing=leixing,
            source=source,
        )
        _append_predictions(rows)
        sheet_name = _safe_sheet_name(f"{source}{leixing}地址分类")
        ws = wb.add_sheet(sheet_name)
        _write_table_xls(ws, rows)

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.read()


def _write_table_xlsx(ws: Any, rows: Sequence[Dict[str, Any]]) -> None:
    headers = [
        "分局",
        "派出所编号",
        "派出所名称",
        "报警时间",
        "警情地址",
        "经度",
        "纬度",
        "报警内容",
        "处警情况",
        "警情类型",
        "分类结果",
        "置信度",
    ]
    ws.append(headers)
    for r in rows:
        ws.append(
            [
                r.get("分局") or "",
                r.get("派出所编号") or "",
                r.get("派出所名称") or "",
                _format_dt(r.get("报警时间")),
                r.get("警情地址") or "",
                _excel_number_or_blank(r.get("经度")),
                _excel_number_or_blank(r.get("纬度")),
                r.get("报警内容") or "",
                r.get("处警情况") or "",
                r.get("jq_type") or "",
                r.get("pred_label") or "",
                _format_prob(r.get("pred_prob")),
            ]
        )


def _write_table_xls(ws: Any, rows: Sequence[Dict[str, Any]]) -> None:
    headers = [
        "分局",
        "派出所编号",
        "派出所名称",
        "报警时间",
        "警情地址",
        "经度",
        "纬度",
        "报警内容",
        "处警情况",
        "警情类型",
        "分类结果",
        "置信度",
    ]
    for col, h in enumerate(headers):
        ws.write(0, col, h)

    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r.get("分局") or "")
        ws.write(i, 1, r.get("派出所编号") or "")
        ws.write(i, 2, r.get("派出所名称") or "")
        ws.write(i, 3, _format_dt(r.get("报警时间")))
        ws.write(i, 4, r.get("警情地址") or "")
        ws.write(i, 5, _excel_number_or_blank(r.get("经度")))
        ws.write(i, 6, _excel_number_or_blank(r.get("纬度")))
        ws.write(i, 7, r.get("报警内容") or "")
        ws.write(i, 8, r.get("处警情况") or "")
        ws.write(i, 9, r.get("jq_type") or "")
        ws.write(i, 10, r.get("pred_label") or "")
        ws.write(i, 11, _format_prob(r.get("pred_prob")))


def _append_predictions(rows: List[Dict[str, Any]]) -> None:
    texts = [str((r.get("警情地址") or "")).strip() for r in rows]
    preds = predict_addresses(texts)
    for r, (label, prob) in zip(rows, preds):
        r["pred_label"] = label
        r["pred_prob"] = prob


def predict_addresses(texts: Sequence[str]) -> List[Tuple[str, float]]:
    bundle = _get_model_bundle()
    if not texts:
        return []

    import torch  # type: ignore
    import torch.nn.functional as F  # type: ignore

    results: List[Tuple[str, float]] = []
    batch_size = 64
    for i in range(0, len(texts), batch_size):
        batch = list(texts[i : i + batch_size])
        encoded = bundle.tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=128,
            return_tensors="pt",
        )
        encoded = {k: v.to(bundle.device) for k, v in encoded.items()}
        with torch.no_grad():
            outputs = bundle.model(**encoded)
            logits = outputs.logits
            probs = F.softmax(logits, dim=-1)
            best_prob, best_idx = torch.max(probs, dim=-1)
        for t, p, idx in zip(batch, best_prob.tolist(), best_idx.tolist()):
            if not str(t).strip():
                results.append(("", 0.0))
                continue
            label = bundle.id2label.get(int(idx), str(idx))
            results.append((label, float(p)))
    return results


def _get_model_bundle() -> ModelBundle:
    global _MODEL_BUNDLE  # noqa: PLW0603
    if _MODEL_BUNDLE is not None:
        return _MODEL_BUNDLE

    with _MODEL_LOCK:
        if _MODEL_BUNDLE is not None:
            return _MODEL_BUNDLE

        model_dir = os.path.join(os.path.dirname(__file__), "..", "5lei_dizhi_model", "best_model")
        id2label_path = os.path.join(os.path.dirname(__file__), "..", "5lei_dizhi_model", "id2label.json")
        model_dir = os.path.abspath(model_dir)
        id2label_path = os.path.abspath(id2label_path)

        try:
            import torch  # type: ignore
            from transformers import AutoModelForSequenceClassification, AutoTokenizer  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"缺少依赖 torch/transformers，无法加载地址分类模型：{exc}") from exc

        with open(id2label_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        id2label = {int(k): str(v) for k, v in raw.items()}

        device = "cpu"
        tokenizer = AutoTokenizer.from_pretrained(model_dir)
        model = AutoModelForSequenceClassification.from_pretrained(model_dir)
        model.to(device)
        model.eval()

        _MODEL_BUNDLE = ModelBundle(tokenizer=tokenizer, model=model, id2label=id2label, device=device)
        return _MODEL_BUNDLE


def _format_dt(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    return str(val)


def _format_coord(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    try:
        return f"{float(val):.6f}"
    except Exception:
        return s


def _excel_number_or_blank(val: Any) -> Any:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    try:
        return float(val)
    except Exception:
        return s


def _format_prob(prob: Any) -> str:
    try:
        return f"{float(prob):.5f}"
    except Exception:
        return "0.00000"


_ILLEGAL_SHEET_CHARS = set(r"[]:*?/\\")


def _safe_sheet_name(name: str) -> str:
    cleaned = "".join("_" if ch in _ILLEGAL_SHEET_CHARS else ch for ch in (name or "sheet"))
    cleaned = cleaned.strip() or "sheet"
    return cleaned[:31]


def _safe_filename_part(val: str) -> str:
    s = str(val or "").strip()
    # Windows 文件名不允许 ":" 等字符
    return (
        s.replace(":", "-")
        .replace("/", "-")
        .replace("\\", "-")
        .replace(" ", "_")
        .replace("\t", "_")
    )


def _format_zh_date(dt: datetime) -> str:
    return f"{dt.year}年{dt.month}月{dt.day}日"


_REPORT_BUREAU_ROW: Dict[ReportBureau, int] = {
    "云城分局": 6,
    "云安分局": 7,
    "罗定市公安局": 8,
    "新兴县公安局": 9,
    "郁南县公安局": 10,
    "ALL": 11,
}

_REPORT_COLS = ["C", "D", "E", "F", "K", "N", "Q", "R", "S", "T", "Y", "AB"]


def _parse_dt(s: str) -> datetime:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S")


def _shift_year(dt: datetime, years: int) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        # 处理 2/29 等日期：回退到当月最后一天
        base = dt.replace(day=1, month=dt.month, year=dt.year + years)
        next_month = base.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        return dt.replace(year=dt.year + years, day=last_day.day)


def _as_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _build_report_counts(
    *,
    rows_year: Sequence[Dict[str, Any]],
    rows_last_year: Sequence[Dict[str, Any]],
    segments_year: Sequence[Tuple[str, datetime, datetime, str, str]],
    segments_last_year: Sequence[Tuple[str, datetime, datetime, str, str]],
) -> Tuple[Dict[Tuple[str, ReportBureau, str], int], List[str]]:
    """
    返回 counts[(sheet_name, bureau_key, col_letter)] = count
    """
    target_leixing = {"人身伤害类", "侵犯财产类", "扰乱秩序类"}
    target_sources = {"原始", "确认"}
    target_pred = "街面与公共区域"
    bureaus = {"云城分局", "云安分局", "罗定市公安局", "新兴县公安局", "郁南县公安局"}

    counts: Dict[Tuple[str, ReportBureau, str], int] = {}
    seen: Dict[Tuple[str, ReportBureau, str], set] = {}
    missing_sheets: List[str] = []

    def inc(sheet: str, bureau: ReportBureau, col: str, caseno_key: str) -> None:
        k = (sheet, bureau, col)
        s = seen.setdefault(k, set())
        if caseno_key in s:
            return
        s.add(caseno_key)
        counts[k] = counts.get(k, 0) + 1

    def process_rows(rows: Sequence[Dict[str, Any]], segs: Sequence[Tuple[str, datetime, datetime, str, str]]) -> None:
        for idx, r in enumerate(rows):
            leixing = str(r.get("leixing") or "").strip()
            source = str(r.get("yuanshiqueren") or "").strip()
            if leixing not in target_leixing or source not in target_sources:
                continue

            if str(r.get("pred_label") or "").strip() != target_pred:
                continue

            dt = _as_dt(r.get("报警时间"))
            if dt is None:
                continue

            bureau_raw = str(r.get("分局") or "").strip()
            bureau_keys: List[ReportBureau] = ["ALL"]
            if bureau_raw in bureaus:
                bureau_keys = [bureau_raw, "ALL"]  # type: ignore[list-item]

            caseno = str(r.get("caseno") or "").strip()
            caseno_key = caseno or f"__row__{idx}"

            for _, start, end, col_orig, col_conf in segs:
                # 统一按 [start, end) 计算，避免边界重复
                if dt < start or dt >= end:
                    continue
                col = col_orig if source == "原始" else col_conf
                for sheet in (leixing, "三类合计"):
                    for b in bureau_keys:
                        inc(sheet, b, col, caseno_key)

    process_rows(rows_year, segments_year)
    process_rows(rows_last_year, segments_last_year)
    return counts, missing_sheets
