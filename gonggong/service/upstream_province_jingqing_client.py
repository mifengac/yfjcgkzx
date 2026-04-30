from __future__ import annotations

import logging
import threading
from typing import Any, Dict

import requests

from gonggong.config.upstream_province_jingqing import UPSTREAM_PROVINCE_JINGQING_CONFIG
from gonggong.service.upstream_jingqing_client import JingQingApiClient


logger = logging.getLogger(__name__)


class ProvinceJingQingApiClient(JingQingApiClient):
    _instance = None
    _instance_lock = threading.Lock()

    def _init_session(self) -> None:
        self.session = requests.Session()
        self.base_url = str(UPSTREAM_PROVINCE_JINGQING_CONFIG.get("base_url") or "").rstrip("/")
        self.username = str(UPSTREAM_PROVINCE_JINGQING_CONFIG.get("username") or "")
        self.password = str(UPSTREAM_PROVINCE_JINGQING_CONFIG.get("password") or "")
        self.login_path = str(
            UPSTREAM_PROVINCE_JINGQING_CONFIG.get("login_path") or "/dsjfxxb/login"
        )
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

    def _looks_like_login_page(self, response_text):
        sample = str(response_text or "")[:3000]
        markers = [
            "/dsjfxxb/login",
            "/dsjfx/login",
            'name="username"',
            'name="password"',
            "rememberMe",
            "登录",
        ]
        return any(marker in sample for marker in markers)

    def get_case_list(self, payload: Dict[str, Any]):
        page_num = (payload or {}).get("pageNum")
        page_size = (payload or {}).get("pageSize")
        start_time = (payload or {}).get("params[startTime]")
        end_time = (payload or {}).get("params[endTime]")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        response = self.request_with_retry(
            "POST",
            "/dsjfxxb/case/list",
            data=payload,
            headers=headers,
            timeout=60,
        )
        if response and response.status_code == 200:
            result = response.json()
            logger.info(
                "province case/list pageNum=%s pageSize=%s start=%s end=%s code=%s total=%s rows=%s",
                page_num,
                page_size,
                start_time,
                end_time,
                result.get("code"),
                result.get("total"),
                len(result.get("rows", [])),
            )
            return result

        logger.warning(
            "province case/list request failed pageNum=%s pageSize=%s start=%s end=%s status=%s",
            page_num,
            page_size,
            start_time,
            end_time,
            response.status_code if response else "None",
        )
        return {"total": 0, "rows": [], "code": -1}


province_api_client = ProvinceJingQingApiClient()
