from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

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
    parts: List[str] = ["未成年人打架斗殴专项行动指标监测:"]

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


def desensitize_name(name: str) -> str:
    """
    姓名脱敏：
    - 两字名：张三 → 张X
    - 三字及以上：张小三 → 张XX
    """
    name = str(name or "").strip()
    if len(name) <= 1:
        return name
    if len(name) == 2:
        return name[0] + "X"
    return name[0] + "XX"


def desensitize_case_name(case_name: str) -> str:
    """
    案件名称脱敏：根据关键字匹配并脱敏姓名
    例如：'张小三殴打他人案' → '张XX殴打他人案'
    """
    case_name = str(case_name or "").strip()
    keywords = ["殴打", "打架", "滋事", "故意伤害", "斗殴"]

    # 尝试匹配：姓名 + 关键字
    for keyword in keywords:
        # 匹配模式：2-4个中文字符 + 关键字
        pattern = rf"([\u4e00-\u9fa5]{{2,4}})({keyword})"
        match = re.search(pattern, case_name)
        if match:
            original_name = match.group(1)
            desensitized = desensitize_name(original_name)
            case_name = case_name.replace(original_name, desensitized, 1)
            break

    return case_name


def filter_mobile_phones(phone_json: Any) -> List[str]:
    """
    从联系电话JSON中过滤出手机号，排除座机号
    座机格式：0XXX-XXXXXXX 或 0XXXXXXXXXX
    手机格式：11位数字
    """
    if not phone_json:
        return []

    phones: List[str] = []
    if isinstance(phone_json, list):
        phones = [str(p).strip() for p in phone_json if p]
    elif isinstance(phone_json, str):
        phones = [phone_json.strip()]
    else:
        return []

    mobile_phones: List[str] = []
    for phone in phones:
        # 移除所有空格和短横线
        clean_phone = re.sub(r"[\s\-]", "", phone)

        # 排除座机号（以0开头的固定电话）
        if clean_phone.startswith("0"):
            continue

        # 只保留11位手机号
        if re.match(r"^1\d{10}$", clean_phone):
            mobile_phones.append(clean_phone)

    return mobile_phones


def send_dashboard_sms(
    *,
    start_time: datetime,
    end_time: datetime,
    results: List[MetricResult],
    mobiles: Optional[List[str]] = None,
    content: Optional[str] = None,
) -> dict:
    """
    发送大屏短信（发送给领导）

    Args:
        start_time: 开始时间
        end_time: 结束时间
        results: 指标结果列表
        mobiles: 自定义接收号码列表（可选，默认使用配置的MOBILES）
        content: 自定义短信内容（可选，默认使用build_dashboard_sms_content生成）
    """
    try:
        import oracledb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少依赖 oracledb，无法连接 Oracle。请先安装 oracledb") from exc

    # 使用自定义号码或配置的号码
    if mobiles is None:
        mobiles = iter_mobiles(MOBILES)
    else:
        mobiles = iter_mobiles(mobiles)

    if not mobiles:
        raise RuntimeError("未提供有效的接收号码")

    ora_cfg = load_oracle_config_from_env()
    ensure_oracle_client_initialized(ora_cfg)

    # 使用自定义内容或生成内容
    if content is None:
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


def send_batch_sms(items: List[Dict[str, str]]) -> dict:
    """
    批量发送短信（发送给责任人）

    Args:
        items: 短信列表，每项包含 {"mobile": "13800138000", "content": "短信内容"}

    Returns:
        {"inserted": 10, "eid": "WCN_DJDO_xxx", "failed": []}
    """
    try:
        import oracledb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少依赖 oracledb，无法连接 Oracle。请先安装 oracledb") from exc

    if not items:
        raise RuntimeError("未提供待发送的短信")

    ora_cfg = load_oracle_config_from_env()
    ensure_oracle_client_initialized(ora_cfg)

    eid = f"WCN_DJDO_RESP_{int(datetime.now().timestamp())}"

    inserted = 0
    failed: List[Dict[str, str]] = []

    with oracledb.connect(user=ora_cfg.user, password=ora_cfg.password, dsn=ora_cfg.dsn) as conn:
        for item in items:
            mobile = str(item.get("mobile") or "").strip()
            content = str(item.get("content") or "").strip()

            if not mobile or not content:
                failed.append({"mobile": mobile, "reason": "号码或内容为空"})
                continue

            try:
                insert_sms(conn, mobile=mobile, content=content, eid=eid)
                inserted += 1
            except Exception as e:
                failed.append({"mobile": mobile, "reason": str(e)})

        conn.commit()

    return {
        "inserted": inserted,
        "eid": eid,
        "failed": failed,
    }

