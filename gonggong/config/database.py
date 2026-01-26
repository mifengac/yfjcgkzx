# 数据库配置文件

import logging
import os
from typing import Optional

# 设置 PostgreSQL 客户端编码环境变量（部分人大金仓/内网环境使用 GBK/GB18030）
# 注意：连接失败时 libpq 返回的错误信息可能是 GBK/GB18030，但 psycopg2 在未建立连接前会尝试按 UTF-8 解码，
# 因此需要在异常处理中做兜底，避免前端只看到 UnicodeDecodeError。
os.environ.setdefault("PGCLIENTENCODING", "GB18030")

PREFERRED_CLIENT_ENCODING = "GB18030"
FALLBACK_CLIENT_ENCODING = "GBK"

try:
    from dotenv import load_dotenv

    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")
    # Windows 下 .env 文件常带 UTF-8 BOM；用 utf-8-sig 读取可自动剔除 BOM，避免 DB_HOST 读不到。
    load_dotenv(dotenv_path=DOTENV_PATH, encoding="utf-8-sig")
except Exception:
    pass

import psycopg2


# 数据库连接配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT") or 54321),  # 人大金仓默认端口
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "schema": os.getenv("DB_SCHEMA", "ywdata"),
}

# 数据库连接字符串
DB_CONNECTION_STRING = (
    f"kingbase://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}"
    f":{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


def _decode_connection_error_detail(exc: Exception) -> Optional[str]:
    if isinstance(exc, UnicodeDecodeError) and isinstance(getattr(exc, "object", None), (bytes, bytearray)):
        raw = bytes(exc.object)
        for enc in ("gb18030", "gbk", "utf-8", "latin1"):
            try:
                return raw.decode(enc, errors="replace")
            except Exception:
                continue
        return raw.decode("latin1", errors="replace")
    return None


def get_database_connection():
    """
    获取数据库连接的公共函数，包含连接超时设置
    """
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG.get("port", 54321),
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            connect_timeout=30,  # 连接超时30秒
            options="-c statement_timeout=300000",  # 查询超时5分钟（300秒）
        )
        # 连接后设置客户端编码（部分环境未识别 options 中的 client_encoding 时兜底）
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"SET client_encoding = '{PREFERRED_CLIENT_ENCODING}'")
            except Exception:
                cursor.execute(f"SET client_encoding = '{FALLBACK_CLIENT_ENCODING}'")
        logging.info("数据库连接成功")
        return connection
    except UnicodeDecodeError as exc:
        detail = _decode_connection_error_detail(exc) or repr(exc)
        hint = "数据库连接失败：连接错误信息编码异常（请检查 .env 配置，尤其是 DB_HOST/DB_USER/DB_PASSWORD）"
        if "Password" in detail or "认证失败" in detail or "authentication failed" in detail.lower():
            hint = "数据库连接失败：用户名/密码认证失败，请检查 .env 中 DB_HOST/DB_USER/DB_PASSWORD 是否正确"
        logging.error("%s；detail=%s", hint, detail)
        raise RuntimeError(hint) from None
    except Exception as exc:
        logging.error("数据库连接失败: %s", exc)
        raise


def execute_query(query: str, params: tuple = None):
    """
    执行数据库查询的公共函数
    """
    connection = None
    cursor = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        if params:
            print(query)
            cursor.execute(query, params)
        else:
            print(query)
            cursor.execute(query)

        results = cursor.fetchall()

        # 获取列名
        columns = [desc[0] for desc in cursor.description]

        # 将结果转换为字典列表
        data = []
        for row in results:
            data.append(dict(zip(columns, row)))

        return data

    except Exception as exc:
        logging.error(f"执行查询失败: {exc}")
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
