from __future__ import annotations

from typing import Optional

import pandas as pd

from gzrzdd.dao.gzrzdd_dao import query_to_dataframe


# TODO: 内网环境由你补齐 SQL（需返回中文别名列）：
# - 姓名
# - 证件号码
# - 风险等级（取值：高风险/中风险/低风险）
# - 分局名称
# - 所属派出所
# - 列管时间
# - 工作日志开展工作时间
DEFAULT_ZDRYGZRZS_SQL: str = ""


def load_zdrygzrzs(sql: Optional[str] = None) -> pd.DataFrame:
    sql = (sql if sql is not None else DEFAULT_ZDRYGZRZS_SQL) or ""
    if not sql.strip():
        raise ValueError("未配置 SQL：请在 gzrzdd/dao/gzrzdd_cqtj_dao.py 设置 DEFAULT_ZDRYGZRZS_SQL")
    return query_to_dataframe(sql)

