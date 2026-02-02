from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass(frozen=True)
class LlamaChatConfig:
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    timeout_seconds: int
    retries: int


def load_llama_chat_config() -> LlamaChatConfig:
    base_url = (os.getenv("LLAMA_BASE_URL") or "http://127.0.0.1:8080").rstrip("/")
    model = os.getenv("LLAMA_MODEL") or "police-qwen3-8b"
    temperature = float(os.getenv("LLAMA_TEMPERATURE") or "0")
    max_tokens = int(os.getenv("LLAMA_MAX_TOKENS") or "512")
    timeout_seconds = int(os.getenv("LLAMA_TIMEOUT_SECONDS") or "300")
    retries = int(os.getenv("LLAMA_RETRIES") or "1")
    return LlamaChatConfig(
        base_url=base_url,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )


class LlamaClientError(RuntimeError):
    pass


def _extract_content_from_openai_response(payload: Dict[str, Any]) -> str:
    try:
        choices = payload.get("choices") or []
        content = choices[0]["message"]["content"]
        if content is None:
            raise KeyError("content is null")
        return str(content)
    except Exception as exc:  # noqa: BLE001
        raise LlamaClientError(f"LLM 响应格式异常: {exc}") from None


def chat_completions(
    messages: list[dict[str, str]],
    *,
    config: Optional[LlamaChatConfig] = None,
) -> Tuple[str, Dict[str, Any]]:
    cfg = config or load_llama_chat_config()
    url = f"{cfg.base_url}/v1/chat/completions"
    req: Dict[str, Any] = {
        "model": cfg.model,
        "messages": messages,
        "temperature": cfg.temperature,
        "max_tokens": cfg.max_tokens,
    }

    last_error: Optional[str] = None
    for attempt in range(cfg.retries + 1):
        start = time.time()
        try:
            resp = requests.post(url, json=req, timeout=cfg.timeout_seconds)
            elapsed = time.time() - start
            if resp.status_code != 200:
                last_error = f"HTTP {resp.status_code}: {resp.text[:500]}"
                continue
            data = resp.json()
            content = _extract_content_from_openai_response(data)
            meta = {
                "elapsed_seconds": elapsed,
                "usage": data.get("usage"),
                "model": data.get("model"),
            }
            return content, meta
        except requests.Timeout:
            last_error = f"请求超时（{cfg.timeout_seconds}s）"
        except json.JSONDecodeError:
            last_error = f"响应不是合法 JSON: {getattr(resp, 'text', '')[:500]}"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

        if attempt < cfg.retries:
            time.sleep(0.6 * (attempt + 1))

    raise LlamaClientError(last_error or "LLM 调用失败")

