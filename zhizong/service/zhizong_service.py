"""
治综平台数据统计模块的数据服务层。

主要职责：
1. 读取治综任务元数据，生成首页按钮列表；
2. 根据任务配置中指定的数据表与字段拼接统计 SQL；
3. 提供地区详情数据，支持导出与深度过滤。
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from gonggong.config.database import execute_query

# -----------------------------------------------------------------------------
# 正则表达式常量
# -----------------------------------------------------------------------------
# 仅允许字母、数字与下划线，避免通过表名注入恶意 SQL。
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
# 允许列名包含别名前缀（如 gz.xgdw_dm），因此放宽小数点。
COLUMN_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.]+$")

# -----------------------------------------------------------------------------
# SQL 模板常量
# -----------------------------------------------------------------------------
# 读取任务元数据，任务名称用于按钮显示。
TASK_METADATA_SQL = """
SELECT id,
       task_name,
       table_name,
       region_field,
       date_field,
       description,
       base_sql,
       condition,
       schema,
       is_active
  FROM ywdata.task_metadata
 WHERE is_active IS TRUE
 ORDER BY task_name
"""

SINGLE_TASK_SQL = """
SELECT id,
       task_name,
       table_name,
       region_field,
       date_field,
       description,
       base_sql,
       condition,
       schema,
       is_active
  FROM ywdata.task_metadata
 WHERE id = %s
"""

SUMMARY_SQL_TEMPLATE = """
SELECT dm.name AS 地区,
       LEFT(gz.{region_field}, 6) AS dwdm,
       COUNT(gz.{id_field}) AS 数量
  FROM {schema}.{table_name} AS gz
  LEFT JOIN ywdata.sys_dq_pcs AS dm
    ON LEFT(gz.{region_field}, 6) = dm.code
 WHERE gz.deleteflag = 0 {additional_condition}
 GROUP BY dm.name, LEFT(gz.{region_field}, 6)
 ORDER BY 数量 DESC
"""


# -----------------------------------------------------------------------------
# 工具函数
# -----------------------------------------------------------------------------
def _ensure_safe_table_name(table_name: str) -> None:
    """校验表名是否合法。"""
    if not table_name:
        raise ValueError("任务配置缺少数据表名")
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"非法数据表名：{table_name}")


def _ensure_safe_column(column_name: str, label: str) -> None:
    """校验列名是否合法。"""
    if not column_name:
        raise ValueError(f"任务配置缺少{label}")
    if not COLUMN_NAME_PATTERN.fullmatch(column_name):
        raise ValueError(f"非法{label}：{column_name}")


# 注意：base_sql 为完整 SQL（包含 SELECT ... FROM ...），不需要额外包裹 SELECT * FROM (...)

def _append_where_or_and(sql_text: str, clause: Optional[str]) -> str:
    """在给定 SQL 末尾追加条件子句（WHERE/AND），自动判断是否已存在 WHERE。

    - 去除末尾分号
    - clause 为空则原样返回
    """
    base = (sql_text or "").strip().rstrip(";")
    cond = (clause or "").strip()
    if not cond:
        return base
    if re.search(r"\bwhere\b", base, flags=re.IGNORECASE):
        return f"{base} AND {cond}"
    return f"{base} WHERE {cond}"


# -----------------------------------------------------------------------------
# 元数据查询函数
# -----------------------------------------------------------------------------
def fetch_active_tasks() -> List[Dict[str, Optional[str]]]:
    """查询所有启用的任务，用于渲染首页按钮。"""
    return execute_query(TASK_METADATA_SQL)


def get_task_metadata(task_id: int) -> Optional[Dict[str, Optional[str]]]:
    """根据任务主键获取完整配置。"""
    rows = execute_query(SINGLE_TASK_SQL, (task_id,))
    return rows[0] if rows else None


# -----------------------------------------------------------------------------
# 统计查询函数
# -----------------------------------------------------------------------------
def fetch_task_summary(
    task_metadata: Dict[str, Optional[str]],
    dwdm: Optional[str] = None,
) -> List[Dict[str, Optional[str]]]:
    """按照任务配置统计各地区数量，可选按地区编码过滤。"""
    table_name = (task_metadata.get("table_name") or "").strip()
    region_field = (task_metadata.get("region_field") or "").strip()
    id_field = (task_metadata.get("id_field") or "id").strip()

    _ensure_safe_table_name(table_name)
    _ensure_safe_column(region_field, "地区字段名")
    if not id_field:
        id_field = "id"
    _ensure_safe_column(id_field, "主键字段名")

    additional_condition = ""
    params: Optional[Tuple[str, ...]] = None
    if dwdm:
        additional_condition = f" AND LEFT({region_field}, 6) = %s"
        params = (dwdm,)

    # schema 动态，默认 stdata
    schema = (task_metadata.get("schema") or "stdata").strip()
    _ensure_safe_table_name(schema)

    sql = SUMMARY_SQL_TEMPLATE.format(
        schema=schema,
        table_name=table_name,
        region_field=region_field,
        id_field=id_field,
        additional_condition=additional_condition,
    )

    return execute_query(sql, params)


def fetch_task_detail_rows(
    task_metadata: Dict[str, Optional[str]],
    dwdm: str,
) -> List[Dict[str, Optional[str]]]:
    """根据任务配置与地区编码查询详情数据：优先使用 base_sql+condition，其次表名。"""
    if not dwdm:
        raise ValueError("缺少地区编码参数")

    table_name = (task_metadata.get("table_name") or "").strip()
    region_field = (task_metadata.get("region_field") or "").strip()
    base_sql = (task_metadata.get("base_sql") or "").strip()
    condition = (task_metadata.get("condition") or "").strip()

    _ensure_safe_column(region_field, "地区字段名")

    if base_sql:
        # base_sql + condition + 地区过滤，直接在原 SQL 末尾追加，无需外层 SELECT
        sql_with_cond = _append_where_or_and(base_sql, condition)
        sql_with_region = _append_where_or_and(sql_with_cond, f"LEFT(zz.{region_field}, 6) = %s")
        return execute_query(sql_with_region, (dwdm,))

    # fallback: 基于表名，保留 where 1=1 再追加 condition 与地区过滤
    _ensure_safe_table_name(table_name)
    sql = f"SELECT * FROM stdata.{table_name} zz WHERE 1=1"
    if condition:
        sql += f" AND {condition}"
    sql += f" AND LEFT(zz.{region_field}, 6) = %s"
    return execute_query(sql, (dwdm,))


# -----------------------------------------------------------------------------
# 首页汇总（按任务 × 固定地区列）
# -----------------------------------------------------------------------------
REGION_MAP = {
    "云城": "445302",
    "云安": "445303",
    "罗定": "445381",
    "新兴": "445321",
    "郁南": "445322",
    "市局": "445300",
}


def _build_home_summary_sql(table_name: str, region_field: str) -> Tuple[str, Optional[Tuple]]:
    """构造首页汇总所需 SQL，直接基于表名统计，末尾保留 WHERE 1=1。"""
    _ensure_safe_table_name(table_name)
    _ensure_safe_column(region_field, "地区字段名")
    sql = f"""
    SELECT
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['云城']}' THEN 1 END) AS 云城,
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['云安']}' THEN 1 END) AS 云安,
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['罗定']}' THEN 1 END) AS 罗定,
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['新兴']}' THEN 1 END) AS 新兴,
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['郁南']}' THEN 1 END) AS 郁南,
      COUNT(CASE WHEN LEFT(zz.{region_field}, 6) = '{REGION_MAP['市局']}' THEN 1 END) AS 市局,
      COUNT(*) AS 总计
    FROM stdata.{table_name} zz WHERE 1=1
    """
    return sql, None


def fetch_home_summary() -> List[Dict[str, Optional[str]]]:
    """首页初始化：按活动任务统计六地区与总计，并拼接任务名。

    返回行结构：
      { '任务名': str, '云城': int, '云安': int, '罗定': int, '新兴': int, '郁南': int, '市局': int, '总计': int, 'task_id': int }
    """
    tasks = fetch_active_tasks()
    rows: List[Dict[str, Optional[str]]] = []
    for t in tasks:
        table_name = (t.get("table_name") or "").strip()
        region_field = (t.get("region_field") or "").strip()

        sql, params = _build_home_summary_sql(table_name, region_field)
        result = execute_query(sql, params)
        summary = result[0] if result else {
            "云城": 0, "云安": 0, "罗定": 0, "新兴": 0, "郁南": 0, "市局": 0, "总计": 0
        }
        row = {
            "任务名": t.get("task_name") or table_name,
            **summary,
            "task_id": t.get("id"),
        }
        rows.append(row)
    return rows


def fetch_task_rows_all(task_metadata: Dict[str, Optional[str]]) -> List[Dict[str, Optional[str]]]:
    """根据任务配置返回全部详情（不加地区过滤）：优先 base_sql+condition，其次表。"""
    table_name = (task_metadata.get("table_name") or "").strip()
    base_sql = (task_metadata.get("base_sql") or "").strip()
    condition = (task_metadata.get("condition") or "").strip()

    if base_sql:
        # base_sql + condition（直接执行，不包裹）
        sql = _append_where_or_and(base_sql, condition)
        return execute_query(sql)

    _ensure_safe_table_name(table_name)
    sql = f"SELECT * FROM stdata.{table_name} zz WHERE 1=1"
    if condition:
        sql += f" AND {condition}"
    return execute_query(sql)
