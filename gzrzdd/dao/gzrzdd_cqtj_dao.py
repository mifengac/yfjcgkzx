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
DEFAULT_ZDRYGZRZS_SQL: str = """
SELECT
	a.xm AS 姓名,
	a.zjhm AS 证件号码,
	e.detail AS 风险等级,
	CASE
		WHEN substring(a.lgdw, 1, 6)= '445302' THEN '云城分局'
		WHEN substring(a.lgdw, 1, 6)= '445303' THEN '云安分局'
		WHEN substring(a.lgdw, 1, 6)= '445321' THEN '新兴县公安局'
		WHEN substring(a.lgdw, 1, 6)= '445381' THEN '罗定市公安局'
		WHEN substring(a.lgdw, 1, 6)= '445322' THEN '郁南县公安局'
		ELSE a.lgdw
	END AS 分局名称,
	b.sspcs AS "所属派出所",
	a.lgsj AS 列管时间,
	c.kzgzsj AS 工作日志开展工作时间,
	d.detail AS 工作日志工作类型,
	c.gzqksm AS 工作日志工作情况说明,
	c.djsj AS 工作日志系统登记时间
FROM
	(
		SELECT
			*
		FROM
			stdata.b_per_mdjffxrygl
		WHERE
			"deleteflag" = '0'
			AND gkzt = '01'
	) a
LEFT JOIN stdata.b_dic_zzjgdm b ON
	a.lgdw = b.sspcsdm LEFT JOIN (SELECT * FROM stdata.b_zdry_ryxx_gzrz WHERE deleteflag='0') c ON a.systemid=c.zdryid LEFT JOIN (SELECT code,detail  FROM stdata.s_sg_dict WHERE kind_code='ZAZDRY_GZRZ_GZLX') d ON c.gzlx=d.code  LEFT JOIN (SELECT code,detail FROM stdata.s_sg_dict WHERE kind_code='ZDRY_YYBF_FXPG' ) e ON a.fxdj=e.code WHERE c.kzgzsj >='2025-1-1'
"""


def load_zdrygzrzs(sql: Optional[str] = None) -> pd.DataFrame:
    sql = (sql if sql is not None else DEFAULT_ZDRYGZRZS_SQL) or ""
    if not sql.strip():
        raise ValueError("未配置 SQL：请在 gzrzdd/dao/gzrzdd_cqtj_dao.py 设置 DEFAULT_ZDRYGZRZS_SQL")
    return query_to_dataframe(sql)

