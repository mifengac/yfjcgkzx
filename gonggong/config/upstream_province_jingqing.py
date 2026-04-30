import os

try:
    from dotenv import load_dotenv

    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    load_dotenv(dotenv_path=DOTENV_PATH, encoding="utf-8-sig")
except Exception:
    pass


UPSTREAM_PROVINCE_JINGQING_CONFIG = {
    "base_url": os.getenv("PROVINCE_JINGQING_UPSTREAM_BASE_URL", "http://68.29.179.170").rstrip("/"),
    "username": os.getenv(
        "PROVINCE_JINGQING_UPSTREAM_USERNAME",
        os.getenv("JINGQING_UPSTREAM_USERNAME", ""),
    ),
    "password": os.getenv(
        "PROVINCE_JINGQING_UPSTREAM_PASSWORD",
        os.getenv("JINGQING_UPSTREAM_PASSWORD", ""),
    ),
    "login_path": os.getenv("PROVINCE_JINGQING_UPSTREAM_LOGIN_PATH", "/dsjfxxb/login"),
}
