from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional

import os

from sqlalchemy import Engine, Table, create_engine, insert, select, update
from sqlalchemy.engine import URL
from sqlalchemy.engine import RowMapping

from gonggong.config.sa_database import PgConnInfo, get_engine_for_pg
from zidingyi_baobiao.core.exceptions import NotFoundError, ValidationError
from zidingyi_baobiao.models.meta_tables import get_meta_tables
from zidingyi_baobiao.services._row_accessors import first_of
from zidingyi_baobiao.utils.crypto import decrypt_text, encrypt_text


def create_datasource(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    新增数据源（data_source）。

    约定字段：
    - db_type: 目前仅支持 kingbase/postgresql
    - host/port/database/username/password/schema
    - password 明文仅用于请求入参，落库时写入 password_enc
    """
    tables = get_meta_tables()
    ds = tables.data_source

    db_type = str(payload.get("db_type") or "kingbase").strip().lower()
    if db_type not in {"kingbase", "postgres", "postgresql"}:
        raise ValidationError("db_type 仅支持 kingbase/postgresql")

    host = str(payload.get("host") or "").strip()
    database = str(payload.get("database") or "").strip()
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    port = int(payload.get("port") or 54321)
    schema = str(payload.get("schema") or "").strip() or "public"

    if not host or not database or not username or not password:
        raise ValidationError("host/database/username/password 为必填")

    data: Dict[str, Any] = {
        "db_type": db_type,
        "host": host,
        "port": port,
        "database": database,
        "username": username,
        "schema": schema,
        "password_enc": encrypt_text(password),
    }
    if "name" in ds.c:
        data["name"] = str(payload.get("name") or "").strip()

    data = _only_existing_columns(ds, data)

    pk = _pk_column_name(ds)
    with tables.engine.begin() as conn:
        stmt = insert(ds).values(**data)
        if pk:
            stmt = stmt.returning(ds.c[pk])
        result = conn.execute(stmt)
        new_id = None
        if pk:
            new_id = result.scalar_one()
        return {"id": new_id, "db_type": db_type, "host": host, "port": port, "database": database, "username": username, "schema": schema}


def update_datasource(datasource_id: int, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """
    更新数据源（data_source）。

    - password 若传入则重新加密写入 password_enc
    """
    tables = get_meta_tables()
    ds = tables.data_source

    row = get_datasource_row(datasource_id)
    update_data: Dict[str, Any] = {}
    for key in ("name", "db_type", "host", "port", "database", "username", "schema"):
        if key in payload:
            update_data[key] = payload.get(key)
    if "password" in payload and payload.get("password"):
        update_data["password_enc"] = encrypt_text(str(payload.get("password")))

    update_data = _only_existing_columns(ds, update_data)
    if not update_data:
        return {"id": datasource_id}

    pk = _pk_column_name(ds)
    if not pk:
        raise ValidationError("data_source 缺少主键，无法更新")

    with tables.engine.begin() as conn:
        conn.execute(update(ds).where(ds.c[pk] == datasource_id).values(**update_data))
    return {"id": datasource_id}


def list_datasources() -> Dict[str, Any]:
    tables = get_meta_tables()
    ds = tables.data_source
    pk = _pk_column_name(ds) or "id"
    with tables.engine.connect() as conn:
        rows = conn.execute(select(ds)).mappings().all()
    # 不返回密码相关字段
    sanitized = []
    for r in rows:
        item = dict(r)
        item.pop("password", None)
        item.pop("password_enc", None)
        if pk in item:
            item["id"] = item.get(pk)
        sanitized.append(item)
    return {"items": sanitized, "count": len(sanitized)}


def get_datasource_row(datasource_id: int) -> RowMapping:
    tables = get_meta_tables()
    ds = tables.data_source
    pk = _pk_column_name(ds)
    if not pk:
        raise ValidationError("data_source 缺少主键，无法查询")
    with tables.engine.connect() as conn:
        row = conn.execute(select(ds).where(ds.c[pk] == datasource_id)).mappings().first()
    if not row:
        raise NotFoundError(f"data_source 不存在：{datasource_id}")
    return row


def test_datasource(datasource_id: int, *, timeout_s: int = 5) -> Dict[str, Any]:
    """
    测试数据源连接（SELECT 1）。
    """
    row = get_datasource_row(datasource_id)
    info = _row_to_conn_info(row)
    engine = _build_test_engine(info, connect_timeout_s=timeout_s)
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        return {"success": True}
    except Exception as exc:
        logging.error("数据源连接测试失败: %s", exc)
        return {"success": False, "message": str(exc)}


def get_engine_for_datasource_row(row: Mapping[str, Any]) -> Engine:
    """
    根据 data_source 行数据获取 Engine（带缓存）。
    """
    info = _row_to_conn_info(row)
    return get_engine_for_pg(info)


def _row_to_conn_info(row: Mapping[str, Any]) -> PgConnInfo:
    host = str(first_of(row, "host", "db_host", default="")).strip()
    database = str(first_of(row, "database", "db_name", default="")).strip()
    username = str(first_of(row, "username", "user", "db_user", default="")).strip()
    port = int(first_of(row, "port", "db_port", default=54321))
    schema = str(first_of(row, "schema", "db_schema", default="public")).strip() or "public"

    password = ""
    if "password_enc" in row and row.get("password_enc"):
        password = decrypt_text(str(row.get("password_enc")))
    elif "password" in row and row.get("password"):
        # 兼容旧字段：不建议使用明文 password
        password = str(row.get("password"))

    if not host or not database or not username or not password:
        raise ValidationError("data_source 缺少连接信息（host/database/username/password）")
    return PgConnInfo(host=host, port=port, database=database, user=username, password=password, schema=schema)


def _build_test_engine(info: PgConnInfo, *, connect_timeout_s: int) -> Engine:
    """
    构造用于“测试连接”的短超时 Engine（不缓存）。
    """
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=info.user,
        password=info.password,
        host=info.host,
        port=int(info.port),
        database=info.database,
    )
    return create_engine(
        url,
        pool_pre_ping=False,
        connect_args={
            "connect_timeout": int(connect_timeout_s),
            "options": f"-c client_encoding={os.environ.get('PGCLIENTENCODING', 'GB18030')}",
        },
        future=True,
    )


def _only_existing_columns(table: Table, data: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in data.items() if k in table.c}


def _pk_column_name(table: Table) -> Optional[str]:
    pks = list(table.primary_key.columns)
    if pks:
        return pks[0].name
    if "id" in table.c:
        return "id"
    return None
