import requests
import logging
import urllib.parse

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
        
        # User agent to fake regular browser
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"
        })
        self.login()

    def login(self):
        """Perform login and hold session cookies."""
        # Initial GET to set up basic cookies
        login_page_url = f"{self.base_url}/dsjfx/login"
        try:
            self.session.get(login_page_url, timeout=10)
            
            # Post authentication
            payload = {
                "username": self.username,
                "password": self.password,
                "rememberMe": "true"
            }
            res = self.session.post(
                login_page_url, 
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded", "Referer": login_page_url},
                timeout=10
            )
            # You might want to check res for "success" or specific string
            # Generally if we don't get redirected to /dsjfx/login it's successful
            logger.info("JingQingApiClient login attempted. Status: %s", res.status_code)
        except Exception as e:
            logger.error("JingQingApiClient Login Failed: %s", e)

    def _ensure_logged_in(self, response):
        """Check if response implies we're logged out (redirect to login)"""
        content_type = response.headers.get("Content-Type", "")
        # If it's JSON, we're definitely logged in
        if "json" in content_type:
            return True
        # If it's HTML and contains login-related strings, we're logged out
        if content_type.startswith("text/html"):
            text_sample = response.text[:2000]
            if "未登录" in text_sample or ("/login" in response.url and "dsjfx" in response.url):
                return False
        return True

    def request_with_retry(self, method, endpoint, **kwargs):
        url = f"{self.base_url}{endpoint}"
        retries = 2
        for i in range(retries):
            try:
                if method.upper() == 'GET':
                    res = self.session.get(url, **kwargs)
                elif method.upper() == 'POST':
                    res = self.session.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method {method}")
                
                if self._ensure_logged_in(res):
                    return res
                else:
                    logger.info("Session expired, re-logging in...")
                    self.login()
            except requests.exceptions.RequestException as e:
                logger.error("API Request failed: %s", e)
                if i == retries - 1:
                    raise e
        return None

    def get_tree_view_data(self):
        """获取警情类型树形数据"""
        res = self.request_with_retry("GET", "/dsjfx/plan/treeViewData", timeout=15)
        if res and res.status_code == 200:
            return res.json()
        return []

    def get_srr_list(self, payload):
        """获取各地同环比数据"""
        # urlencode with quote() + safe='[]' to keep bracket keys literal
        # Default quote_plus encodes [ ] to %5B %5D which the server won't match
        body = urllib.parse.urlencode(
            payload,
            quote_via=urllib.parse.quote,
            safe='[]'
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        logger.info("SRR request body: %s", body[:300])
        res = self.request_with_retry("POST", "/dsjfx/srr/list",
                                      data=body, headers=headers, timeout=20)
        if res and res.status_code == 200:
            try:
                result = res.json()
                logger.info("SRR list response code=%s total=%s rows=%s",
                            result.get('code'), result.get('total'), len(result.get('rows', [])))
                return result
            except Exception as e:
                logger.error("SRR list JSON parse failed: %s | body: %s", e, res.text[:500])
                return {"total": 0, "rows": [], "code": -1}
        logger.error("SRR list request failed, status=%s", res.status_code if res else 'None')
        return {"total": 0, "rows": [], "code": -1}

    def get_case_list(self, payload):
        """获取警情明细列表数据"""
        res = self.request_with_retry("POST", "/dsjfx/case/list", data=payload, timeout=20)
        if res and res.status_code == 200:
            return res.json()
        return {"total": 0, "rows": [], "code": -1}

# Singleton instance for simple usage
api_client = JingQingApiClient()
