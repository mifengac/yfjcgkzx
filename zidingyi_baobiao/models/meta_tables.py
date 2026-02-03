from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from sqlalchemy import Engine, MetaData, Table

from gonggong.config.database import DB_CONFIG
from gonggong.config.sa_database import get_meta_engine


@dataclass(frozen=True)
class MetaTables:
    """
    仅包含三张“配置表”的 SQLAlchemy Table 映射。

    注意：表结构来自数据库反射（autoload_with），因此不会在代码里创建新表。
    """

    engine: Engine
    schema: str
    metadata: MetaData
    data_source: Table
    dataset: Table
    module_def: Table


@lru_cache(maxsize=1)
def get_meta_tables(schema: Optional[str] = None) -> MetaTables:
    """
    获取（并缓存）配置表映射。

    Args:
        schema: 若为空，使用 .env 的 DB_SCHEMA（默认 public）。
    """
    engine = get_meta_engine()
    schema_name = (schema or DB_CONFIG.get("schema") or "public").strip() or "public"

    metadata = MetaData()
    data_source = Table("data_source", metadata, schema=schema_name, autoload_with=engine)
    dataset = Table("dataset", metadata, schema=schema_name, autoload_with=engine)
    module_def = Table("module_def", metadata, schema=schema_name, autoload_with=engine)
    return MetaTables(
        engine=engine,
        schema=schema_name,
        metadata=metadata,
        data_source=data_source,
        dataset=dataset,
        module_def=module_def,
    )

