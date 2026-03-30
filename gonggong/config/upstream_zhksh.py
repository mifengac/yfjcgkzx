from __future__ import annotations

import os
from urllib.parse import urljoin

try:
    from dotenv import load_dotenv

    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    load_dotenv(dotenv_path=DOTENV_PATH, encoding="utf-8-sig")
except Exception:
    pass


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    try:
        return int(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


UPSTREAM_ZHKSH_CONFIG = {
    "base_url": os.getenv("ZHKSH_BASE_URL", "http://68.253.2.107").rstrip("/"),
    "username": os.getenv("ZHKSH_USERNAME", "270378"),
    "password": os.getenv("ZHKSH_PASSWORD", "jpx8hLPMyV7EDVX1p9d89Q=="),
    "login_path": os.getenv("ZHKSH_LOGIN_PATH", "/zhksh/login"),
    "validate_path": os.getenv("ZHKSH_VALIDATE_PATH", "/zhksh/system/user/getInfo"),
    "session_ttl_hours": _get_int("ZHKSH_SESSION_TTL_HOURS", 4),
    "validate_leeway_minutes": _get_int("ZHKSH_VALIDATE_LEEWAY_MINUTES", 30),
    "request_timeout_seconds": _get_int("ZHKSH_REQUEST_TIMEOUT_SECONDS", 180),
    "validation_connect_timeout_seconds": _get_int("ZHKSH_VALIDATE_CONNECT_TIMEOUT_SECONDS", 3),
    "validation_read_timeout_seconds": _get_int("ZHKSH_VALIDATE_READ_TIMEOUT_SECONDS", 10),
}


def build_zhksh_url(path: str) -> str:
    normalized_path = str(path or "").lstrip("/")
    return urljoin(f"{UPSTREAM_ZHKSH_CONFIG['base_url']}/", normalized_path)
