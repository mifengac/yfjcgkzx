from __future__ import annotations

import logging
import threading
import urllib.parse

import requests

from gonggong.config.upstream_jingqing import UPSTREAM_JINGQING_CONFIG


logger = logging.getLogger(__name__)


class JingQingApiClient:
    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super(JingQingApiClient, cls).__new__(cls)
                    cls._instance._init_session()
        return cls._instance

    def _init_session(self) -> None:
        self.session = requests.Session()
        self.base_url = str(UPSTREAM_JINGQING_CONFIG.get("base_url") or "").rstrip("/")
        self.username = str(UPSTREAM_JINGQING_CONFIG.get("username") or "")
        self.password = str(UPSTREAM_JINGQING_CONFIG.get("password") or "")
        self.login_path = str(UPSTREAM_JINGQING_CONFIG.get("login_path") or "/dsjfx/login")
        self._logged_in = False
        self._login_lock = threading.Lock()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/100.0.0.0 Safari/537.36"
                )
            }
        )

    def login(self, *, force: bool = False) -> bool:
        if self._logged_in and not force:
            return True

        with self._login_lock:
            if self._logged_in and not force:
                return True

            login_page_url = f"{self.base_url}{self.login_path}"
            try:
                self.session.get(login_page_url, timeout=20)
                payload = {
                    "username": self.username,
                    "password": self.password,
                    "rememberMe": "true",
                }
                response = self.session.post(
                    login_page_url,
                    data=payload,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Referer": login_page_url,
                    },
                    timeout=20,
                )
                self._logged_in = response.status_code == 200
                logger.info("JingQingApiClient login attempted. Status: %s", response.status_code)
                return self._logged_in
            except Exception as exc:
                self._logged_in = False
                logger.error("JingQingApiClient login failed: %s", exc)
                return False

    def _looks_like_login_page(self, response_text):
        sample = str(response_text or "")[:3000]
        markers = ["/dsjfx/login", "name=\"username\"", "name=\"password\"", "rememberMe", "登录", "鐧诲綍"]
        return any(marker in sample for marker in markers)

    def _ensure_logged_in(self, response):
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type.lower():
            return True
        if content_type.startswith("text/html"):
            if self._looks_like_login_page(response.text) or (
                "/login" in response.url and "dsjfx" in response.url
            ):
                return False
        return True

    def request_with_retry(self, method, endpoint, **kwargs):
        if not self._logged_in and not self.login():
            return None

        url = f"{self.base_url}{endpoint}"
        retries = 2
        for i in range(retries):
            try:
                if method.upper() == "GET":
                    response = self.session.get(url, **kwargs)
                elif method.upper() == "POST":
                    response = self.session.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method {method}")

                if self._ensure_logged_in(response):
                    return response

                logger.info("Session expired, re-logging in...")
                self._logged_in = False
                if not self.login(force=True):
                    return None
            except requests.exceptions.RequestException as exc:
                logger.error("API request failed: %s", exc)
                if i == retries - 1:
                    raise
        return None

    def get_tree_view_data(self):
        response = self.request_with_retry("GET", "/dsjfx/plan/treeViewData", timeout=15)
        if response and response.status_code == 200:
            return response.json()
        return []

    def get_nature_tree_new_view_data(self):
        response = self.request_with_retry("GET", "/dsjfx/nature/treeNewViewData", timeout=15)
        if response and response.status_code == 200:
            return response.json()
        return []

    def get_srr_list(self, payload):
        body = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote, safe="[]")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        chara_no = str((payload or {}).get("charaNo", "")).strip()
        jsession = self.session.cookies.get("JSESSIONID") if self.session else ""
        logger.info(
            "SRR request charaNoLen=%s hasJSESSIONID=%s body=%s",
            len(chara_no),
            bool(jsession),
            body[:300],
        )

        response = self.request_with_retry(
            "POST", "/dsjfx/srr/list", data=body, headers=headers, timeout=20
        )
        if not response or response.status_code != 200:
            logger.error(
                "SRR list request failed, status=%s",
                response.status_code if response else "None",
            )
            return {"total": 0, "rows": [], "code": -1}

        content_type = (response.headers.get("Content-Type", "") or "").lower()
        if "json" not in content_type and self._looks_like_login_page(response.text):
            logger.warning("SRR non-JSON looks like login page, force relogin+retry")
            self._logged_in = False
            if not self.login(force=True):
                return {"total": 0, "rows": [], "code": -1}
            response = self.request_with_retry(
                "POST", "/dsjfx/srr/list", data=body, headers=headers, timeout=20
            )
            if not response or response.status_code != 200:
                logger.error("SRR retry after relogin failed")
                return {"total": 0, "rows": [], "code": -1}

        try:
            result = response.json()
        except Exception as exc:
            logger.error(
                "SRR list JSON parse failed: %s | body=%s",
                exc,
                (response.text or "")[:500],
            )
            return {"total": 0, "rows": [], "code": -1}

        rows_len = len(result.get("rows", []))
        logger.info(
            "SRR list response code=%s total=%s rows=%s",
            result.get("code"),
            result.get("total"),
            rows_len,
        )

        upstream_code = result.get("code")
        if upstream_code != 0:
            logger.error(
                "SRR upstream business error code=%s msg=%s head=%s",
                upstream_code,
                result.get("msg"),
                (response.text or "")[:300],
            )
            return result

        if rows_len == 0 and chara_no:
            logger.warning("SRR empty rows with non-empty charaNo; no relogin retry (business-empty)")
            logger.warning("SRR empty response head=%s", (response.text or "")[:300])

        return result

    def get_case_list(self, payload):
        page_num = (payload or {}).get("pageNum")
        page_size = (payload or {}).get("pageSize")
        begin_date = (payload or {}).get("beginDate")
        end_date = (payload or {}).get("endDate")
        response = self.request_with_retry("POST", "/dsjfx/case/list", data=payload, timeout=60)
        if response and response.status_code == 200:
            result = response.json()
            logger.info(
                "case/list pageNum=%s pageSize=%s begin=%s end=%s code=%s total=%s rows=%s",
                page_num,
                page_size,
                begin_date,
                end_date,
                result.get("code"),
                result.get("total"),
                len(result.get("rows", [])),
            )
            return result

        logger.warning(
            "case/list request failed pageNum=%s pageSize=%s begin=%s end=%s status=%s",
            page_num,
            page_size,
            begin_date,
            end_date,
            response.status_code if response else "None",
        )
        return {"total": 0, "rows": [], "code": -1}


api_client = JingQingApiClient()
