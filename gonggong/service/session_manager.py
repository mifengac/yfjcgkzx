from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests

from gonggong.config.upstream_zhksh import UPSTREAM_ZHKSH_CONFIG, build_zhksh_url


logger = logging.getLogger(__name__)


def _decode_response_text(content: bytes, preferred: Optional[str] = None) -> Tuple[str, str]:
    candidates = []
    if preferred:
        candidates.append(preferred)
    candidates.extend(["utf-8", "gb18030", "gbk"])

    last_error: Optional[Exception] = None
    for encoding in candidates:
        try:
            return content.decode(encoding), encoding
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    logger.warning("Failed to decode response body, fallback to utf-8 replace: %s", last_error)
    return content.decode("utf-8", errors="replace"), "utf-8(replace)"


class SessionManager:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self._login_lock = threading.Lock()
        self.session: Optional[requests.Session] = None
        self.last_login_time: Optional[datetime] = None
        self.login_failure_count = 0
        self.last_login_failure_time: Optional[datetime] = None
        self.login_failure_cooldown = timedelta(minutes=1)
        self._load_config()

    def _load_config(self) -> None:
        config = UPSTREAM_ZHKSH_CONFIG
        self.base_url = str(config.get("base_url") or "").rstrip("/")
        self.username = str(config.get("username") or "")
        self.password = str(config.get("password") or "")
        self.login_path = str(config.get("login_path") or "/zhksh/login")
        self.validate_path = str(config.get("validate_path") or "/zhksh/system/user/getInfo")
        self.login_url = build_zhksh_url(self.login_path)
        self.validate_url = build_zhksh_url(self.validate_path)
        self.target_host = urlparse(self.base_url).netloc
        self.session_ttl = timedelta(hours=int(config.get("session_ttl_hours") or 4))
        self.validate_leeway = timedelta(minutes=int(config.get("validate_leeway_minutes") or 30))
        self.request_timeout = int(config.get("request_timeout_seconds") or 180)
        self.validation_timeout = (
            float(config.get("validation_connect_timeout_seconds") or 3),
            float(config.get("validation_read_timeout_seconds") or 10),
        )

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "X-Requested-With": "XMLHttpRequest",
            }
        )
        return session

    def _clear_session(self) -> None:
        self.session = None
        self.last_login_time = None

    def _is_target_url(self, url: str) -> bool:
        return bool(self.target_host and self.target_host in str(url or ""))

    def _looks_like_login_page(self, response: requests.Response) -> bool:
        if response.status_code == 401:
            return True

        response_url = str(getattr(response, "url", "") or "").lower()
        if "/login" in response_url and "zhksh" in response_url:
            return True

        content_type = str(response.headers.get("Content-Type", "") or "").lower()
        if "html" not in content_type:
            return False

        sample = str(getattr(response, "text", "") or "")[:3000]
        markers = ["/zhksh/login", 'name="username"', 'name="password"', "rememberMe", "login"]
        return any(marker in sample for marker in markers)

    def _is_session_expired(self) -> bool:
        if self.session is None or self.last_login_time is None:
            return True

        elapsed = datetime.now() - self.last_login_time
        if elapsed >= self.session_ttl:
            return True

        if self.session_ttl - elapsed > self.validate_leeway:
            return False

        try:
            response = self.session.get(self.validate_url, timeout=self.validation_timeout)
            if response.status_code != 200 or self._looks_like_login_page(response):
                logger.info("Existing zhksh session is no longer valid, will relogin.")
                return True

            self.last_login_time = datetime.now()
            return False
        except requests.exceptions.Timeout:
            logger.warning("Timed out while validating zhksh session, treat as expired.")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to validate zhksh session: %s", exc)
            return True

    def get_session(self, force: bool = False) -> Optional[requests.Session]:
        with self.lock:
            if force:
                self._clear_session()
            if self.session is None or self._is_session_expired():
                logger.info("zhksh session missing or expired, logging in on demand.")
                self._login(force=True)
            return self.session

    def _login(self, force: bool = False) -> None:
        if not force and self.session is not None and not self._is_session_expired():
            return

        if (
            self.last_login_failure_time
            and datetime.now() - self.last_login_failure_time < self.login_failure_cooldown
        ):
            remaining = self.login_failure_cooldown - (datetime.now() - self.last_login_failure_time)
            raise Exception(f"Login cooldown active, retry after {remaining.total_seconds():.1f}s")

        with self._login_lock:
            if not force and self.session is not None and not self._is_session_expired():
                return

            try:
                session = self._create_session()
                login_data = {
                    "username": self.username,
                    "password": self.password,
                    "rememberMe": True,
                    "isPkiLogin": False,
                    "isAccLogin": True,
                    "isSmsLogin": False,
                }
                response = session.post(self.login_url, data=login_data, timeout=self.request_timeout)
                preferred = None
                try:
                    preferred = response.apparent_encoding
                except Exception:  # noqa: BLE001
                    preferred = None
                response_text, used_encoding = _decode_response_text(response.content, preferred=preferred)
                if used_encoding and not used_encoding.endswith("(replace)"):
                    response.encoding = used_encoding

                login_success = self._is_login_success(response, response_text)
                if not login_success:
                    raise Exception(self._extract_login_error(response, response_text))

                self.session = session
                self.last_login_time = datetime.now()
                self.login_failure_count = 0
                self.last_login_failure_time = None
                self.login_failure_cooldown = timedelta(minutes=1)
                logger.info("zhksh login succeeded, session is ready.")
            except requests.exceptions.Timeout as exc:
                self._record_login_failure()
                raise Exception("Login request timed out") from exc
            except Exception:
                self._record_login_failure()
                raise

    def _is_login_success(self, response: requests.Response, response_text: str) -> bool:
        text = str(response_text or "")
        if text and ("操作成功" in text or "登录成功" in text):
            return True

        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            payload = None

        if isinstance(payload, dict):
            if (
                payload.get("code") in {0, 200}
                or payload.get("success") is True
                or payload.get("msg") == "操作成功"
                or payload.get("message") == "操作成功"
            ):
                return True

        return response.status_code == 200 and not self._looks_like_login_page(response)

    def _extract_login_error(self, response: requests.Response, response_text: str) -> str:
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            payload = None

        if isinstance(payload, dict):
            for key in ("msg", "message", "error"):
                if payload.get(key):
                    return str(payload[key])

        text = str(response_text or "").strip()
        if text:
            return text[:200]
        return f"Login failed with status {response.status_code}"

    def _record_login_failure(self) -> None:
        self.login_failure_count += 1
        self.last_login_failure_time = datetime.now()

        if self.login_failure_count > 5:
            self.login_failure_cooldown = timedelta(minutes=15)
        elif self.login_failure_count > 3:
            self.login_failure_cooldown = timedelta(minutes=5)
        else:
            self.login_failure_cooldown = timedelta(minutes=1)

        logger.warning(
            "zhksh login failed count=%s cooldown=%ss",
            self.login_failure_count,
            self.login_failure_cooldown.total_seconds(),
        )

    def test_login(self) -> bool:
        try:
            self._clear_session()
            self._login(force=True)
            return self.session is not None
        except Exception as exc:  # noqa: BLE001
            logger.error("zhksh login test failed: %s", exc)
            return False

    def make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        if not self._is_target_url(url):
            kwargs.setdefault("timeout", self.request_timeout)
            return requests.request(method, url, **kwargs)

        last_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                session = self.get_session(force=attempt > 0)
                kwargs.setdefault("timeout", self.request_timeout)
                response = session.request(method, url, **kwargs)
                if not self._looks_like_login_page(response):
                    return response

                logger.info("zhksh request hit login page, forcing relogin: %s", url)
                self._clear_session()
                last_error = Exception("Session expired and redirected to login page")
            except requests.exceptions.Timeout:
                logger.error("zhksh request timed out: %s", url)
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("zhksh request failed on attempt %s: %s", attempt + 1, exc)
                self._clear_session()
                last_error = exc

        raise Exception(f"Request failed after relogin: {last_error}")


session_manager = SessionManager()
