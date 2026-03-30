import os

try:
    from dotenv import load_dotenv

    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    load_dotenv(dotenv_path=DOTENV_PATH, encoding="utf-8-sig")
except Exception:
    pass


UPSTREAM_JINGQING_CONFIG = {
    "base_url": os.getenv("JINGQING_UPSTREAM_BASE_URL", "http://68.253.2.111").rstrip("/"),
    "username": os.getenv("JINGQING_UPSTREAM_USERNAME", "270378"),
    "password": os.getenv("JINGQING_UPSTREAM_PASSWORD", "jpx8hLPMyV7EDVX1p9d89Q=="),
    "login_path": os.getenv("JINGQING_UPSTREAM_LOGIN_PATH", "/dsjfx/login"),
}
