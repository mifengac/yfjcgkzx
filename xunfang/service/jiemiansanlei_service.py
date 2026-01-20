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
from datetime import datetime
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
    headers = ["分局", "派出所编号", "派出所名称", "报警时间", "警情地址", "警情类型", "分类结果", "置信度"]
    ws.append(headers)
    for r in rows:
        ws.append(
            [
                r.get("分局") or "",
                r.get("派出所编号") or "",
                r.get("派出所名称") or "",
                _format_dt(r.get("报警时间")),
                r.get("警情地址") or "",
                r.get("jq_type") or "",
                r.get("pred_label") or "",
                _format_prob(r.get("pred_prob")),
            ]
        )


def _write_table_xls(ws: Any, rows: Sequence[Dict[str, Any]]) -> None:
    headers = ["分局", "派出所编号", "派出所名称", "报警时间", "警情地址", "警情类型", "分类结果", "置信度"]
    for col, h in enumerate(headers):
        ws.write(0, col, h)

    for i, r in enumerate(rows, start=1):
        ws.write(i, 0, r.get("分局") or "")
        ws.write(i, 1, r.get("派出所编号") or "")
        ws.write(i, 2, r.get("派出所名称") or "")
        ws.write(i, 3, _format_dt(r.get("报警时间")))
        ws.write(i, 4, r.get("警情地址") or "")
        ws.write(i, 5, r.get("jq_type") or "")
        ws.write(i, 6, r.get("pred_label") or "")
        ws.write(i, 7, _format_prob(r.get("pred_prob")))


def _append_predictions(rows: List[Dict[str, Any]]) -> None:
    texts = [str((r.get("警情地址") or "")).strip() for r in rows]
    preds = predict_addresses(texts)
    for r, (label, prob) in zip(rows, preds, strict=False):
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
        for t, p, idx in zip(batch, best_prob.tolist(), best_idx.tolist(), strict=False):
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
