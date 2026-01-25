from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Sequence

from .service import MetricResult, REGIONS

# -----------------------------------------------------------------------------
# 可配置项（按 0123_dxpt_ceshi.py 的“变量配置”风格）
# -----------------------------------------------------------------------------

SEND_SMS_PASSWORD: str = "qqq"
MOBILES: List[str] = ["15635202857"]


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
        raise RuntimeError(f"缺少环境变量 {name}")
    return value.strip()


def load_oracle_config_from_env() -> OracleConfig:
    dsn = _require_env("ORACLE_DSN")
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", dsn) or (":" in dsn and "/" not in dsn and "(" not in dsn):
        raise RuntimeError(
            "ORACLE_DSN 缺少 SERVICE_NAME（导致 ORA-12504）。请设置为形如 "
            "'host:1521/service_name'（例如 '10.0.0.1:1521/orcl'），或改用 TNS 别名。"
        )
    return OracleConfig(
        user=_require_env("ORACLE_USER"),
        password=_require_env("ORACLE_PASSWORD"),
        dsn=dsn,
        instantclient_lib_dir=os.environ.get("ORACLE_CLIENT_LIB_DIR"),
    )


def ensure_oracle_client_initialized(cfg: OracleConfig) -> None:
    if not cfg.instantclient_lib_dir:
        return
    import oracledb  # type: ignore

    oracledb.init_oracle_client(lib_dir=cfg.instantclient_lib_dir)


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


def insert_sms(conn: Any, *, mobile: str, content: str, eid: str) -> None:
    sql = (
        "insert into yfgadb.dfsdl("
        "id,mobile,content,deadtime,status,eid,userid,password,userport"
        ") values (yfgadb.seq_sendsms.nextval,:mobile,:content,sysdate,'0',:eid,"
        "'admin','yfga8130018','0006')"
    )
    with conn.cursor() as cur:
        cur.execute(sql, {"mobile": mobile, "content": content, "eid": eid})


def _is_zero(val: Any) -> bool:
    try:
        return abs(float(val or 0)) < 1e-9
    except Exception:
        return True


def _fmt_value(series_name: str, val: Any) -> str:
    name = str(series_name or "")
    is_rate = "率" in name
    try:
        num = float(val or 0)
    except Exception:
        num = 0.0
    if is_rate:
        return f"{num:.2f}%"
    if abs(num - int(num)) < 1e-9:
        return str(int(num))
    return str(num)


def build_dashboard_sms_content(results: List[MetricResult]) -> str:
    """
    模板：
    未成年人打击斗殴专项行动指标监测:1.xxx:全市A{x},B{y},C{z},其中云城...,云安...;2....
    规则：若某地区（不含全市）三项均为 0，则省略该地区段落。
    """
    parts: List[str] = ["未成年人打击斗殴专项行动指标监测:"]

    for idx, r in enumerate(results, start=1):
        by_region = {str(x.get("地区") or ""): x for x in r.chart_rows}
        all_city = by_region.get("全市") or {}

        base_items = []
        for s in r.series:
            base_items.append(f"{s}{_fmt_value(s, all_city.get(s))}")
        section = f"{idx}.{r.title}:全市" + "，".join(base_items)

        region_segs: List[str] = []
        for region in [x for x in REGIONS if x != "全市"]:
            row = by_region.get(region) or {}
            vals = [row.get(s) for s in r.series]
            if len(vals) == 3 and all(_is_zero(v) for v in vals):
                continue
            seg_items = [f"{s}{_fmt_value(s, row.get(s))}" for s in r.series]
            region_segs.append(f"{region}" + "，".join(seg_items))

        if region_segs:
            section += "，其中" + "，".join(region_segs)

        parts.append(section)

    return ";".join(parts)


def send_dashboard_sms(*, start_time: datetime, end_time: datetime, results: List[MetricResult]) -> dict:
    try:
        import oracledb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少依赖 oracledb，无法连接 Oracle。请先安装 oracledb") from exc

    mobiles = iter_mobiles(MOBILES)
    if not mobiles:
        raise RuntimeError("MOBILES 未配置有效号码")

    ora_cfg = load_oracle_config_from_env()
    ensure_oracle_client_initialized(ora_cfg)

    content = build_dashboard_sms_content(results)
    eid = f"WCN_DJDO_{int(datetime.now().timestamp())}"

    inserted = 0
    with oracledb.connect(user=ora_cfg.user, password=ora_cfg.password, dsn=ora_cfg.dsn) as conn:
        for m in mobiles:
            insert_sms(conn, mobile=m, content=content, eid=eid)
            inserted += 1
        conn.commit()

    return {
        "inserted": inserted,
        "eid": eid,
        "mobiles": mobiles,
        "content": content,
        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
    }

