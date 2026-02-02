from __future__ import annotations

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import pandas as pd

from gonggong.utils.error_handler import log_info, log_warning
from shuju_tiqu.service.llama_client import LlamaClientError, chat_completions, load_llama_chat_config


JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


@dataclass(frozen=True)
class ExtractTarget:
    name: str
    desc: str = ""


def parse_targets(raw: Any) -> List[ExtractTarget]:
    targets: List[ExtractTarget] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                n = item.strip()
                if n:
                    targets.append(ExtractTarget(name=n))
                continue
            if isinstance(item, dict):
                n = str(item.get("name", "")).strip()
                d = str(item.get("desc", "")).strip()
                if n:
                    targets.append(ExtractTarget(name=n, desc=d))
    seen = set()
    deduped: List[ExtractTarget] = []
    for t in targets:
        if t.name in seen:
            continue
        seen.add(t.name)
        deduped.append(t)
    return deduped


def build_messages(user_prompt: str, targets: List[ExtractTarget], rows: List[Dict[str, Any]]) -> list[dict[str, str]]:
    system = (
        "你是一个信息抽取助手。你必须只输出严格的 JSON（不要 Markdown，不要解释，不要多余字符）。\n"
        "输出必须符合 schema：\n"
        '{"rows":[{"__row_id":123,"字段1":"...","字段2":"..."}]}\n'
        "其中字段名必须与输入 targets 中的 name 完全一致；无法提取时填空字符串。"
    )
    payload = {
        "targets": [{"name": t.name, "desc": t.desc} for t in targets],
        "rows": rows,
        "instruction": user_prompt or "",
    }
    user = (
        "按 instruction 抽取 targets 字段，逐条处理 rows；输出仅允许 JSON。\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _try_parse_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("空输出")
    try:
        return json.loads(text)
    except Exception:
        m = JSON_OBJECT_RE.search(text)
        if not m:
            raise
        return json.loads(m.group(0))


def _row_payload_from_df(df: pd.DataFrame, row_index: int, source_columns: List[str]) -> Dict[str, Any]:
    d: Dict[str, Any] = {"__row_id": int(row_index)}
    for col in source_columns:
        v = df.at[row_index, col]
        if pd.isna(v):
            d[col] = ""
        else:
            d[col] = v
    return d


def _apply_results_to_df(
    df: pd.DataFrame,
    targets: List[ExtractTarget],
    rows_out: List[Dict[str, Any]],
    errors: Dict[int, str],
) -> None:
    for t in targets:
        if t.name not in df.columns:
            df[t.name] = ""
    if "llm_error" not in df.columns:
        df["llm_error"] = ""

    for item in rows_out:
        if not isinstance(item, dict):
            continue
        rid = item.get("__row_id")
        try:
            rid_int = int(rid)
        except Exception:
            continue
        for t in targets:
            v = item.get(t.name, "")
            df.at[rid_int, t.name] = "" if v is None else str(v)

    for rid, msg in errors.items():
        df.at[rid, "llm_error"] = str(msg)[:500]


def extract_fields(
    df: pd.DataFrame,
    source_columns: List[str],
    targets: List[ExtractTarget],
    user_prompt: str,
    *,
    batch_size: int,
    concurrency: int,
) -> pd.DataFrame:
    if not source_columns:
        raise ValueError("请选择输入字段")
    if not targets:
        raise ValueError("请填写输出字段")
    for t in targets:
        if t.name in df.columns:
            raise ValueError(f"输出字段名已存在于原表: {t.name}")
    for c in source_columns:
        if c not in df.columns:
            raise ValueError(f"输入字段不存在: {c}")

    max_rows = int(os.getenv("LLM_EXTRACT_MAX_ROWS") or "5000")
    if len(df) > max_rows:
        raise ValueError(f"行数过多（{len(df)}），请分批处理（限制 {max_rows} 行）")

    cfg = load_llama_chat_config()
    parallel_cap = int(os.getenv("LLM_SERVER_PARALLEL") or "4")
    concurrency = max(1, min(int(concurrency), parallel_cap))
    batch_size = max(1, int(batch_size))

    idx_list = list(df.index)
    batches: List[List[int]] = []
    cur: List[int] = []
    for rid in idx_list:
        cur.append(int(rid))
        if len(cur) >= batch_size:
            batches.append(cur)
            cur = []
    if cur:
        batches.append(cur)

    log_info(f"数据提取：rows={len(df)} batch_size={batch_size} batches={len(batches)} concurrency={concurrency}")

    errors: Dict[int, str] = {}
    rows_out: List[Dict[str, Any]] = []

    def run_one_batch(row_ids: List[int]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
        batch_rows = [_row_payload_from_df(df, rid, source_columns) for rid in row_ids]
        messages = build_messages(user_prompt, targets, batch_rows)
        try:
            suggested_max = 128 + len(batch_rows) * len(targets) * 24
            max_tokens = min(2048, max(cfg.max_tokens, suggested_max))
            cfg2 = cfg if max_tokens == cfg.max_tokens else type(cfg)(
                base_url=cfg.base_url,
                model=cfg.model,
                temperature=cfg.temperature,
                max_tokens=max_tokens,
                timeout_seconds=cfg.timeout_seconds,
                retries=cfg.retries,
            )
            text, meta = chat_completions(messages, config=cfg2)
            parsed = _try_parse_json(text)
            out_rows = parsed.get("rows")
            if not isinstance(out_rows, list):
                raise ValueError("输出 JSON 缺少 rows 数组")
            out_map: Dict[int, Dict[str, Any]] = {}
            for item in out_rows:
                if not isinstance(item, dict):
                    continue
                try:
                    rid2 = int(item.get("__row_id"))
                except Exception:
                    continue
                out_map[rid2] = item
            missing = [rid for rid in row_ids if rid not in out_map]
            berr: Dict[int, str] = {}
            if missing:
                for rid in missing:
                    berr[rid] = "LLM 未返回该行结果"
            log_info(f"LLM batch ok: rows={len(row_ids)} elapsed={meta.get('elapsed_seconds')}")
            return list(out_map.values()), berr
        except Exception as exc:  # noqa: BLE001
            msg = f"{type(exc).__name__}: {exc}"
            # 批次失败：拆分单行再试一次
            if len(row_ids) > 1:
                log_warning(f"LLM batch failed，拆分单行重试: {msg}")
                all_out: List[Dict[str, Any]] = []
                all_err: Dict[int, str] = {}
                for rid in row_ids:
                    o, e = run_one_batch([rid])
                    all_out.extend(o)
                    all_err.update(e)
                return all_out, all_err
            rid = row_ids[0]
            if isinstance(exc, LlamaClientError):
                return [], {rid: f"LLM 调用失败: {exc}"}
            return [], {rid: msg}

    start_all = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(run_one_batch, b): b for b in batches}
        for fut in as_completed(futures):
            out_rows, out_err = fut.result()
            rows_out.extend(out_rows)
            errors.update(out_err)

    _apply_results_to_df(df, targets, rows_out, errors)
    log_info(f"数据提取完成：elapsed={time.time() - start_all:.2f}s errors={len(errors)}")
    return df
