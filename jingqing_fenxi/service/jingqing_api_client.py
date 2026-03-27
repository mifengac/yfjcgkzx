import logging
import urllib.parse

import requests

logger = logging.getLogger(__name__)


class JingQingApiClient:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(JingQingApiClient, cls).__new__(cls)
            cls._instance._init_session()
        return cls._instance

    def _init_session(self):
        self.session = requests.Session()
        self.base_url = "http://68.253.2.111"
        self.username = "270378"
        self.password = "jpx8hLPMyV7EDVX1p9d89Q=="  # Hardcoded encrypted password
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/100.0.0.0 Safari/537.36"
                )
            }
        )
        self.login()

    def login(self):
        """Perform login and hold session cookies."""
        login_page_url = f"{self.base_url}/dsjfx/login"
        try:
            self.session.get(login_page_url, timeout=10)
            payload = {"username": self.username, "password": self.password, "rememberMe": "true"}
            res = self.session.post(
                login_page_url,
                data=payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": login_page_url,
                },
                timeout=10,
            )
            logger.info("JingQingApiClient login attempted. Status: %s", res.status_code)
        except Exception as exc:
            logger.error("JingQingApiClient login failed: %s", exc)

    def _looks_like_login_page(self, response_text):
        sample = str(response_text or "")[:3000]
        markers = ["/dsjfx/login", "name=\"username\"", "name=\"password\"", "rememberMe", "登录", "鐧诲綍"]
        return any(marker in sample for marker in markers)

    def _ensure_logged_in(self, response):
        """Check if response implies logged-out status."""
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
        url = f"{self.base_url}{endpoint}"
        retries = 2
        for i in range(retries):
            try:
                if method.upper() == "GET":
                    res = self.session.get(url, **kwargs)
                elif method.upper() == "POST":
                    res = self.session.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method {method}")

                if self._ensure_logged_in(res):
                    return res
                logger.info("Session expired, re-logging in...")
                self.login()
            except requests.exceptions.RequestException as exc:
                logger.error("API request failed: %s", exc)
                if i == retries - 1:
                    raise
        return None

    def get_tree_view_data(self):
        """Get case-type tree data."""
        res = self.request_with_retry("GET", "/dsjfx/plan/treeViewData", timeout=15)
        if res and res.status_code == 200:
            return res.json()
        return []

    def get_srr_list(self, payload, trace_id=None):
        """Get same-period and month-over-month regional comparison data."""
        trace = trace_id or "-"
        body = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote, safe="[]")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        chara_no = str((payload or {}).get("charaNo", "")).strip()
        jsession = self.session.cookies.get("JSESSIONID") if self.session else ""
        logger.info(
            "[trace:%s] SRR request charaNoLen=%s hasJSESSIONID=%s body=%s",
            trace,
            len(chara_no),
            bool(jsession),
            body[:300],
        )

        res = self.request_with_retry(
            "POST", "/dsjfx/srr/list", data=body, headers=headers, timeout=20
        )
        if not res or res.status_code != 200:
            logger.error(
                "[trace:%s] SRR list request failed, status=%s",
                trace,
                res.status_code if res else "None",
            )
            return {"total": 0, "rows": [], "code": -1}

        content_type = (res.headers.get("Content-Type", "") or "").lower()
        if "json" not in content_type and self._looks_like_login_page(res.text):
            logger.warning(
                "[trace:%s] SRR non-JSON looks like login page, force relogin+retry",
                trace,
            )
            self.login()
            res = self.request_with_retry(
                "POST", "/dsjfx/srr/list", data=body, headers=headers, timeout=20
            )
            if not res or res.status_code != 200:
                logger.error("[trace:%s] SRR retry after relogin failed", trace)
                return {"total": 0, "rows": [], "code": -1}

        try:
            result = res.json()
        except Exception as exc:
            logger.error(
                "[trace:%s] SRR list JSON parse failed: %s | body=%s",
                trace,
                exc,
                (res.text or "")[:500],
            )
            return {"total": 0, "rows": [], "code": -1}

        rows_len = len(result.get("rows", []))
        logger.info(
            "[trace:%s] SRR list response code=%s total=%s rows=%s",
            trace,
            result.get("code"),
            result.get("total"),
            rows_len,
        )

        upstream_code = result.get("code")
        if upstream_code != 0:
            logger.error(
                "[trace:%s] SRR upstream business error code=%s msg=%s head=%s",
                trace,
                upstream_code,
                result.get("msg"),
                (res.text or "")[:300],
            )
            return result

        # code=0 but rows empty: only log diagnostics, do not relogin/retry.
        if rows_len == 0 and chara_no:
            logger.warning(
                "[trace:%s] SRR empty rows with non-empty charaNo; no relogin retry (business-empty)",
                trace,
            )
            logger.warning("[trace:%s] SRR empty response head=%s", trace, (res.text or "")[:300])

        return result

    def get_case_list(self, payload):
        """Get case detail list."""
        page_num = (payload or {}).get("pageNum")
        page_size = (payload or {}).get("pageSize")
        begin_date = (payload or {}).get("beginDate")
        end_date = (payload or {}).get("endDate")
        res = self.request_with_retry("POST", "/dsjfx/case/list", data=payload, timeout=20)
        if res and res.status_code == 200:
            result = res.json()
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
            res.status_code if res else "None",
        )
        return {"total": 0, "rows": [], "code": -1}


api_client = JingQingApiClient()
