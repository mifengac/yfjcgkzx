from __future__ import annotations

import argparse
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence


KINGBASE_SQL: str = r"""
SELECT
DISTINCT on(a.systemid)
a.systemid AS 系统编号,
    a.ywlsh AS 业务流水号,
        a.jfmc AS 纠纷名称,
    c.detail AS 纠纷类型,
    a.jyqk AS 简要情况,
    a.fssj AS 发生时间,
    CASE
        WHEN a.sssj = '445300000000' THEN '云浮市公安局'
        ELSE a.sssj
    END AS "所属市局",
     CASE
        WHEN substring(a.ssfj, 1, 6)= '445302' THEN '云城分局'
        WHEN substring(a.ssfj, 1, 6)= '445303' THEN '云安分局'
        WHEN substring(a.ssfj, 1, 6)= '445321' THEN '新兴县公安局'
        WHEN substring(a.ssfj, 1, 6)= '445381' THEN '罗定市公安局'
        WHEN substring(a.ssfj, 1, 6)= '445322' THEN '郁南县公安局'
        ELSE a.ssfj
    END AS 分局名称,
     e.sspcs AS 所属派出所,
    d.detail AS 流转状态,
    a.djsj AS 纠纷登记时间,
    a.djdw_mc AS 纠纷登记单位名称,
    a.xgsj AS 纠纷修改时间,
    b.yjqqsj AS 移交请求时间,
    g.detail AS 粤平安反馈状态,
    CASE
        WHEN b.tczt = '1' THEN '已化解'
        WHEN b.tczt = '0' THEN '未化解'
        ELSE b.tczt
    END AS "调处状态",
    b.rksj AS 入库时间,
    CASE
        WHEN b.orderstate = '2' THEN '已登记:已分发待确认'
        WHEN b.orderstate = '5' THEN '处理中:其他'
        WHEN b.orderstate = '6' THEN '已结案'
        WHEN b.orderstate = '4' THEN '处理中:业务系统已受理'
        ELSE b.orderstate
    END AS "粤平安流程节点状态",
    b.processtime AS 粤平安流程节点时间,
    round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2) AS 粤平安移交时间差,
    case when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=12  and b.yjqqsj is null then '12小时内未移交'
     when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=24  and b.yjqqsj is null then '24小时内未移交'
     when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=48  and b.yjqqsj is null then '48小时内未移交'
     when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)<=72  and b.yjqqsj is null then '72小时内未移交'
     when round((EXTRACT(epoch FROM (now() -a.djsj))/86400*24),2)>72  and b.yjqqsj is null then '超出72小时仍未移交'
     when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=48 then '48小时内移交'
     when round((EXTRACT(epoch FROM (b.yjqqsj -a.djsj))/86400*24),2)<=72 then '72小时内移交' else '超出72小时移交' end as "12-24-48-72小时内移交情况"
FROM
    (
        SELECT
            *
        FROM
            stdata.b_per_mdjfjfsjgl
        WHERE
            deleteflag = '0'
            AND sfgazzfw = '0'
            AND djsj >= '2026-01-01'
    ) a
LEFT JOIN (
        SELECT
            *
        FROM
            stdata.b_per_mdjfypafhsj
        WHERE
            deleteflag = '0'
    ) b ON
    a.systemid = b.systemid
LEFT JOIN (
        SELECT
            code ,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_JFLX'
    )c ON
    a.jflx = c.code
LEFT JOIN (
        SELECT
            code ,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_LCZT'
    )d ON
    a.lczt = d.code
LEFT JOIN (
        SELECT
            code ,
            detail
        FROM
            "stdata"."s_sg_dict"
        WHERE
            "kind_code" = 'SQRY_XGNMK_MDJF_YJFKZT'
    )g ON
    b.yjfkzt = g.code
LEFT JOIN stdata.b_dic_zzjgdm e ON
    a.sspcs = e.sspcsdm WHERE a.lczt<>'6'
"""


MOBILES: List[str] = ["15635202857"]
DEDUP_HOURS: int = 12


LOGGER = logging.getLogger("0123_dxpt_ceshi")


@dataclass(frozen=True)
class KingbaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class OracleConfig:
    user: str
    password: str
    dsn: str
    instantclient_lib_dir: Optional[str]


def _require_env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        if default is not None and str(default).strip():
            return str(default).strip()
        raise SystemExit(f"缺少环境变量 {name}，请先设置后再运行。")
    return value.strip()


def load_kingbase_config_from_env() -> KingbaseConfig:
    return KingbaseConfig(
        host=_require_env("KINGBASE_HOST"),
        port=int(os.environ.get("KINGBASE_PORT", "54321")),
        dbname=_require_env("KINGBASE_DBNAME"),
        user=_require_env("KINGBASE_USER"),
        password=_require_env("KINGBASE_PASSWORD"),
    )


def load_oracle_config_from_env() -> OracleConfig:
    dsn = _require_env("ORACLE_DSN")
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", dsn) or (
        ":" in dsn and "/" not in dsn and "(" not in dsn
    ):
        raise SystemExit(
            "ORACLE_DSN 缺少 SERVICE_NAME（导致 ORA-12504）。请设置为形如 "
            "'host:1521/service_name'（例如 '10.0.0.1:1521/orcl'），或改用 TNS 别名。"
        )
    return OracleConfig(
        user=_require_env("ORACLE_USER"),
        password=_require_env("ORACLE_PASSWORD"),
        dsn=dsn,
        instantclient_lib_dir=os.environ.get("ORACLE_CLIENT_LIB_DIR"),
    )


def _format_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def build_sms_content(row: Dict[str, Any]) -> str:
    fj = str(row.get("分局名称") or "")
    pcs = str(row.get("所属派出所") or "")
    djsj = _format_dt(row.get("纠纷登记时间"))
    yjqk = str(row.get("12-24-48-72小时内移交情况") or "")
    jfmc = str(row.get("纠纷名称") or "")
    jflx = str(row.get("纠纷类型") or "")
    jyqk = str(row.get("简要情况") or "")
    fssj = _format_dt(row.get("发生时间"))
    return (
        f"{fj}{pcs}"
        f"【纠纷登记时间】：{djsj}，"
        f"【移交情况】：{yjqk}"
        f"【纠纷名称】：{jfmc}，"
        f"【纠纷类型】：{jflx}，"
        f"【简要情况】：{jyqk}"
        f"【发生时间】：{fssj}"
    )


def should_send(row: Dict[str, Any]) -> bool:
    v = row.get("12-24-48-72小时内移交情况")
    if v is None:
        return True
    s = str(v)
    return "未移交" in s


def compute_eids(rows: List[Dict[str, Any]]) -> List[str]:
    ywlsh_list: List[str] = []
    for row in rows:
        ywlsh = row.get("业务流水号")
        if ywlsh is None:
            ywlsh_list.append("")
        else:
            ywlsh_list.append(str(ywlsh).strip())

    counts = Counter([x for x in ywlsh_list if x])

    eids: List[str] = []
    for row, ywlsh in zip(rows, ywlsh_list):
        systemid = str(row.get("系统编号") or "").strip()
        if not ywlsh:
            eids.append(systemid)
            continue
        if counts.get(ywlsh, 0) > 1:
            eids.append(systemid)
            continue
        eids.append(ywlsh)
    return eids


def fetch_kingbase_rows(cfg: KingbaseConfig, sql: str) -> List[Dict[str, Any]]:
    try:
        import psycopg2
        import psycopg2.extras
    except ModuleNotFoundError as e:
        raise SystemExit("缺少依赖 psycopg2，无法连接人大金仓。请先执行: pip install psycopg2") from e

    LOGGER.info("连接人大金仓: %s:%s/%s", cfg.host, cfg.port, cfg.dbname)
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def ensure_oracle_client_initialized(cfg: OracleConfig) -> None:
    if not cfg.instantclient_lib_dir:
        return
    import oracledb

    try:
        oracledb.init_oracle_client(lib_dir=cfg.instantclient_lib_dir)
        LOGGER.info("已初始化 Oracle Instant Client: %s", cfg.instantclient_lib_dir)
    except Exception:
        LOGGER.exception("Oracle Instant Client 初始化失败")
        raise


def get_last_deadtime(conn: Any, *, eid: str, mobile: str) -> Optional[datetime]:
    sql = (
        "SELECT MAX(deadtime) AS last_deadtime "
        "FROM yfgadb.dfsdl "
        "WHERE eid = :eid AND mobile = :mobile"
    )
    with conn.cursor() as cur:
        cur.execute(sql, {"eid": eid, "mobile": mobile})
        row = cur.fetchone()
        if not row:
            return None
        last_deadtime = row[0]
        if last_deadtime is None:
            return None
        if isinstance(last_deadtime, datetime):
            return last_deadtime
        return None


def insert_sms(conn: Any, *, mobile: str, content: str, eid: str) -> None:
    sql = (
        "insert into yfgadb.dfsdl("
        "id,mobile,content,deadtime,status,eid,userid,password,userport"
        ") values (yfgadb.seq_sendsms.nextval,:mobile,:content,sysdate,'0',:eid,"
        "'admin','yfga8130018','0006')"
    )
    with conn.cursor() as cur:
        cur.execute(sql, {"mobile": mobile, "content": content, "eid": eid})

def normalize_mobile(mobile: str) -> str:
    return str(mobile).strip()


def iter_mobiles(mobiles: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for m in mobiles:
        nm = normalize_mobile(m)
        if not nm:
            continue
        if nm in seen:
            continue
        seen.add(nm)
        out.append(nm)
    return out


def iter_targets(rows: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for row in rows:
        if should_send(row):
            yield row


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="0123_dxpt_ceshi.py",
        description="从人大金仓读取纠纷数据，满足条件则向 Oracle11g 写入 dfsdl 以触发短信。",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计不写入 Oracle")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="限制处理条数（0 表示不限制）",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not MOBILES:
        LOGGER.error("MOBILES 为空，未配置任何电话号码，退出。")
        return 2
    mobiles = iter_mobiles(MOBILES)
    if not mobiles:
        LOGGER.error("MOBILES 规范化后为空，未配置有效电话号码，退出。")
        return 2

    king_cfg = load_kingbase_config_from_env()
    ora_cfg = load_oracle_config_from_env()

    rows = fetch_kingbase_rows(king_cfg, KINGBASE_SQL)
    LOGGER.info("人大金仓查询返回 %d 条", len(rows))

    targets = list(iter_targets(rows))
    LOGGER.info("命中（未移交或NULL）%d 条", len(targets))

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]
        LOGGER.info("limit 生效，实际处理 %d 条", len(targets))

    eids = compute_eids(targets)
    now = datetime.now()
    dedup_delta = timedelta(hours=DEDUP_HOURS)

    try:
        import oracledb
    except ModuleNotFoundError as e:
        raise SystemExit("缺少依赖 oracledb，无法连接 Oracle。请先执行: pip install oracledb==3.4.1") from e

    ensure_oracle_client_initialized(ora_cfg)

    if args.dry_run:
        LOGGER.info("dry-run: 不连接/不写入 Oracle，仅输出统计。")
        for row, eid in zip(targets, eids):
            _ = build_sms_content(row)
            LOGGER.debug("would_send eid=%s mobiles=%s", eid, mobiles)
        LOGGER.info("OK(dry-run)")
        return 0

    inserted = 0
    skipped_dedup = 0
    with oracledb.connect(user=ora_cfg.user, password=ora_cfg.password, dsn=ora_cfg.dsn) as conn:
        for row, eid in zip(targets, eids):
            if not eid:
                LOGGER.warning("EID 为空，跳过 row=%s", row)
                continue

            content = build_sms_content(row)
            for mobile in mobiles:
                last_deadtime = get_last_deadtime(conn, eid=eid, mobile=mobile)
                if last_deadtime is not None:
                    hours_ago = (now - last_deadtime).total_seconds() / 3600
                    LOGGER.debug(
                        "dedup_check eid=%s mobile=%s last_deadtime=%s hours_ago=%.2f",
                        eid,
                        mobile,
                        last_deadtime,
                        hours_ago,
                    )
                else:
                    LOGGER.debug("dedup_check eid=%s mobile=%s last_deadtime=None", eid, mobile)

                if last_deadtime is not None and (now - last_deadtime) <= dedup_delta:
                    skipped_dedup += 1
                    LOGGER.info(
                        "skip_dedup eid=%s mobile=%s last_deadtime=%s (<=%dh)",
                        eid,
                        mobile,
                        last_deadtime,
                        DEDUP_HOURS,
                    )
                    continue

                LOGGER.info("insert eid=%s mobile=%s content_len=%d", eid, mobile, len(content))
                insert_sms(conn, mobile=mobile, content=content, eid=eid)
                inserted += 1

        conn.commit()

    LOGGER.info("OK inserted=%d skipped_dedup=%d targets=%d", inserted, skipped_dedup, len(targets))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
