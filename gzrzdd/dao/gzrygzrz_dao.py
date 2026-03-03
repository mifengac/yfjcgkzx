from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Sequence

import pandas as pd

from gonggong.config.database import DB_CONFIG, get_database_connection


BASE_SQL = """
SELECT
  *
FROM
  (
    SELECT
      a.ryxm AS 姓名,
      a.zjhm AS 证件号码,
      CASE WHEN a.sfyjjfx = 1 THEN '是' WHEN a.sfyjjfx = 0 THEN '否' ELSE a.sfyjjfx::text END AS 两会是否有进京风险,
      a.lxdh AS 联系电话,
      CASE
        WHEN substring(a.sjgsdwdm, 1, 6) = '445302' THEN '云城分局'
        WHEN substring(a.sjgsdwdm, 1, 6) = '445303' THEN '云安分局'
        WHEN substring(a.sjgsdwdm, 1, 6) = '445321' THEN '新兴县公安局'
        WHEN substring(a.sjgsdwdm, 1, 6) = '445381' THEN '罗定市公安局'
        WHEN substring(a.sjgsdwdm, 1, 6) = '445322' THEN '郁南县公安局'
        ELSE a.sjgsdwdm
      END AS 分局名称,
      d.sspcs AS 所属派出所,
      a.djsj AS 数据登记时间,
      b.djsj AS 工作日志系统登记时间,
      b.gzkzsj::date AS 工作日志_工作开展时间,
      c.detail AS 工作日志_工作方式,
      b.gzqksm AS 工作日志_工作开展情况,
      CASE WHEN b.sfczjjzx = 1 THEN '是' WHEN b.sfczjjzx = 0 THEN '否' ELSE b.sfczjjzx::text END AS 是否存在进京指向,
      b.mqryszwz AS 目前人员所在位置
    FROM
      stdata.b_per_gzry a
      LEFT JOIN (
        SELECT
          *
        FROM
          stdata.b_per_gzrygzrz
        WHERE
          "deleteflag" = 0
          AND djsj >= %s
          AND djsj <= %s
      ) b ON a.id = b."flid"
      LEFT JOIN (
        SELECT
          code,
          detail
        FROM
          stdata.s_sg_dict
        WHERE
          "kind_code" = 'stgzrzfs'
      ) c ON b.stgzfs = c.code
      LEFT JOIN stdata.b_dic_zzjgdm d ON a.sjgsdwdm = d.sspcsdm
    WHERE
      a."deleteflag" = 0
  ) t
WHERE
  1 = 1
"""


def _run_query(sql: str, params: Sequence[object]) -> pd.DataFrame:
    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            schema = (DB_CONFIG.get("schema") or "").strip()
            if schema:
                cur.execute(f"SET search_path TO {schema};")
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def normalize_datetime_text(value: str) -> str:
    text = (value or "").strip().replace("T", " ")
    if not text:
        raise ValueError("时间不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"时间格式错误: {text}，应为 YYYY-MM-DD HH:MM:SS")


def query_gzrygzrz(
    *,
    start_time: str,
    end_time: str,
    sfczjjzx: str = "",
    branches: Iterable[str] | None = None,
) -> pd.DataFrame:
    start_text = normalize_datetime_text(start_time)
    end_text = normalize_datetime_text(end_time)
    start_dt = datetime.strptime(start_text, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_text, "%Y-%m-%d %H:%M:%S")
    if start_dt > end_dt:
        raise ValueError("开始时间不能大于结束时间")

    sql = BASE_SQL
    params: List[object] = [start_text, end_text]

    sfczjjzx_text = (sfczjjzx or "").strip()
    if sfczjjzx_text in ("是", "否"):
        sql += ' AND t."是否存在进京指向" = %s'
        params.append(sfczjjzx_text)
    elif sfczjjzx_text:
        raise ValueError("是否存在进京指向取值仅支持: 空/是/否")

    branch_list = [x.strip() for x in (branches or []) if x and x.strip()]
    if branch_list:
        placeholders = ",".join(["%s"] * len(branch_list))
        sql += f' AND t."分局名称" IN ({placeholders})'
        params.extend(branch_list)

    sql += ' ORDER BY t."工作日志系统登记时间" DESC NULLS LAST, t."数据登记时间" DESC NULLS LAST'
    return _run_query(sql, params)
