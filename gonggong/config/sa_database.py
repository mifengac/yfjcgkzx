"""
SQLAlchemy 数据库连接配置（用于自定义驱动报表系统）。

说明：
- 该仓库原有模块主要使用 psycopg2；本文件为新增的 SQLAlchemy 2.x 连接封装，避免影响旧代码。
- 人大金仓 Kingbase V8 兼容 PostgreSQL 协议，因此使用 SQLAlchemy 的 postgresql+psycopg2 方言即可。
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL

# 复用既有 .env 加载与客户端编码设置逻辑
from gonggong.config.database import DB_CONFIG  # noqa: E402


@dataclass(frozen=True)
class PgConnInfo:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "public"


_ENGINE_CACHE_LOCK = threading.RLock()
_ENGINE_CACHE: dict[str, Engine] = {}


def _pg_url(info: PgConnInfo) -> URL:
    """
    生成 PostgreSQL/Kingbase SQLAlchemy URL。

    注意：Kingbase V8 支持 PG 协议，通常可直接使用 postgresql+psycopg2。
    """
    return URL.create(
        drivername="postgresql+psycopg2",
        username=info.user,
        password=info.password,
        host=info.host,
        port=int(info.port),
        database=info.database,
    )


def _normalize_schema(schema: Optional[str]) -> str:
    schema = (schema or "").strip()
    return schema or "public"


def get_meta_engine() -> Engine:
    """
    获取“配置库”（data_source/dataset/module_def 所在库）的 Engine。

    DB_* 环境变量来源：仓库根目录 .env（参见 gonggong/config/database.py）。
    """
    info = PgConnInfo(
        host=str(DB_CONFIG["host"] or "127.0.0.1"),
        port=int(DB_CONFIG.get("port") or 54321),
        database=str(DB_CONFIG["database"] or ""),
        user=str(DB_CONFIG["user"] or ""),
        password=str(DB_CONFIG["password"] or ""),
        schema=_normalize_schema(DB_CONFIG.get("schema")),
    )
    return get_engine_for_pg(info)


def get_engine_for_pg(
    info: PgConnInfo,
    *,
    connect_timeout_s: int = 30,
    pool_pre_ping: bool = True,
    pool_recycle_s: int = 1800,
) -> Engine:
    """
    获取/创建指定 PG/Kingbase 连接信息对应的 Engine（带缓存）。

    Args:
        info: 连接信息
        connect_timeout_s: 连接超时（秒）
        pool_pre_ping: 连接池健康检查
        pool_recycle_s: 连接回收（秒），用于避免长连接被服务端断开
    """
    cache_key = f"{info.user}@{info.host}:{info.port}/{info.database}?schema={info.schema}"
    with _ENGINE_CACHE_LOCK:
        engine = _ENGINE_CACHE.get(cache_key)
        if engine is not None:
            return engine

        url = _pg_url(info)
        engine = create_engine(
            url,
            pool_pre_ping=pool_pre_ping,
            pool_recycle=pool_recycle_s,
            # psycopg2 connect 参数：connect_timeout / options 等
            connect_args={
                "connect_timeout": int(connect_timeout_s),
                # 部分内网/金仓环境的报错信息编码为 GBK/GB18030；这里同步旧项目的默认设置
                "options": f"-c client_encoding={os.environ.get('PGCLIENTENCODING', 'GB18030')}",
            },
            future=True,
        )
        _ENGINE_CACHE[cache_key] = engine
        return engine


def build_pg_info_from_datasource_row(row: Mapping[str, Any]) -> PgConnInfo:
    """
    从 data_source 表行数据构造 PgConnInfo。

    约定字段名（若你们实际表字段不同，请在此处做适配）：
    - host / port / database / username / password
    - schema（可选）
    """
    host = str(row.get("host") or row.get("db_host") or "")
    if not host:
        raise ValueError("data_source.host 为空")

    port = int(row.get("port") or row.get("db_port") or 54321)
    database = str(row.get("database") or row.get("db_name") or "")
    if not database:
        raise ValueError("data_source.database 为空")

    user = str(row.get("username") or row.get("user") or row.get("db_user") or "")
    if not user:
        raise ValueError("data_source.username 为空")

    password = str(row.get("password") or row.get("db_password") or "")
    if not password:
        raise ValueError("data_source.password 为空")

    schema = _normalize_schema(row.get("schema") or row.get("db_schema"))
    return PgConnInfo(host=host, port=port, database=database, user=user, password=password, schema=schema)

