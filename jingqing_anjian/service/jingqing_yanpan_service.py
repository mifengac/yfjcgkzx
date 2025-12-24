from __future__ import annotations

"""
警情研判报告生成服务。

将 scripts/generate_report_nei.py 中的核心逻辑抽取为服务函数，供 Web 接口调用。
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
import io
import logging

import pandas as pd
from docxtpl import DocxTemplate

from gonggong.config.database import get_database_connection

try:
    from openai import OpenAI  # type: ignore
    import httpx  # type: ignore
except ImportError:  # pragma: no cover - 运行环境缺少依赖时降级
    OpenAI = None  # type: ignore
    httpx = None  # type: ignore


# LLM 调用配置（保持与独立脚本一致，方便本地调试）
LLM_BASE_URL = "http://127.0.0.1:11434/v1"
LLM_API_KEY = "ollama"
LLM_MODEL_NAME = "police-qwen3-8b-VL"

LLM_OPTIONS: Dict[str, Any] = {
    "num_ctx": 2048,
}

MAX_ANALYSIS_CHARS = 6000


SQL_QUERY = """
SELECT 
    jq.calltime AS 报警时间,
    jq.cmdname AS 地区,
    jq.dutydeptname AS 派出所,
    jq.occuraddress AS 警情地址,
    jq.casecontents AS 报警内容,
    jq.replies AS 处警情况,
    jq.leixing AS 警情性质 
FROM ywdata.v_jq_optimized jq 
WHERE jq.leixing in('涉黄','打架斗殴','赌博') 
AND jq.calltime between %s and %s
"""


@dataclass
class YanpanReportResult:
    content: bytes
    filename: str


def _extract_block_from_marker(text: str, marker: str) -> str:
    if not text:
        return ""
    idx = text.find(marker)
    if idx == -1:
        return ""
    sub = text[idx:]
    lines = sub.splitlines()
    kept_lines: List[str] = []
    for line in lines:
        if line.strip() == "" and kept_lines:
            break
        kept_lines.append(line)
    return "\n".join(kept_lines).strip()


def clean_feedback_text(raw: Any) -> str:
    text = "" if raw is None else str(raw)
    text = text.replace("\r\n", "\n")

    if "【结警反馈】" in text:
        return _extract_block_from_marker(text, "【结警反馈】") or "【结警反馈】"

    if "【过程反馈】" in text:
        return _extract_block_from_marker(text, "【过程反馈】") or "【过程反馈】"

    if "关联重复报警" in text:
        return "重复报警"

    if "处理结果说明" in text:
        return _extract_block_from_marker(text, "处理结果说明") or "处理结果说明"

    return "未反馈"


def clean_region_text(raw: Any) -> str:
    if raw is None:
        return ""
    text = str(raw)
    for kw in ["云城", "云安", "罗定", "新兴", "郁南"]:
        if kw in text:
            return kw
    return text


def create_llm_client() -> Any:
    """
    创建 LLM 客户端。

    如依赖缺失或服务不可用，将抛出 RuntimeError，交由上层提示“请启动大模型服务！”。
    """
    if OpenAI is None or httpx is None:
        logging.error("openai/httpx 依赖未安装，无法连接大模型服务")
        raise RuntimeError("请启动大模型服务！")

    try:
        transport = httpx.HTTPTransport()
        # 拉长与大模型服务的超时时间，避免频繁超时重试
        timeout = httpx.Timeout(
            connect=30.0,  # 连接超时
            read=600.0,    # 单次响应读取超时，支持长推理
            write=600.0,
            pool=30.0,
        )
        http_client = httpx.Client(transport=transport, timeout=timeout)

        client = OpenAI(
            base_url=LLM_BASE_URL,
            api_key=LLM_API_KEY,
            http_client=http_client,
            # 如仍想减少“Retrying request...”频率，可适当调小重试次数
            max_retries=2,
        )
        return client
    except Exception as exc:  # pragma: no cover - 防御性代码
        logging.error("创建 LLM 客户端失败: %s", exc)
        raise RuntimeError("请启动大模型服务！") from exc


def llm_chat(
    client: Any,
    prompt: str,
    system_prompt: str,
    max_tokens: int = 256,
    temperature: float = 0.2,
) -> str:
    try:
        extra_body: Dict[str, Any] = {"enable_thinking": False}
        if LLM_OPTIONS:
            extra_body["options"] = LLM_OPTIONS
        resp = client.chat.completions.create(
            model=LLM_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            extra_body=extra_body,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:  # pragma: no cover - 防御性代码
        logging.error("LLM 调用失败: %s", exc)
        # 将异常转换为统一的提示，交由路由层返回给前端
        raise RuntimeError("请启动大模型服务！") from exc


def summarize_single_case(client: Any, content: str, reply: str) -> str:
    content = content or ""
    reply = reply or ""
    system_prompt = (
        "你是一名资深公安情报分析员，负责对警情进行简要概括。"
        "要求：中文输出、简洁准确、突出案件性质和处置结果。"
    )
    user_prompt = (
        "请根据以下“报警内容”和“处警情况”，用一句中文进行概括，"
        "控制在 50 字以内，只输出概括句本身，不要加前缀或说明。\n\n"
        f"【报警内容】{content}\n"
        f"【处警情况】{reply}\n"
    )
    summary = llm_chat(client, user_prompt, system_prompt, max_tokens=100)
    return summary.replace("\n", " ").strip()


def analyze_overall_problems(client: Any, all_case_summaries: str) -> str:
    system_prompt = (
        "你是一名公安情报分析员，擅长对大量警情信息进行综合研判。"
        "现在需要你根据警情概括，提炼出执法工作中的重点问题。"
    )
    user_prompt = (
        "下面是近期涉黄、赌博、打架斗殴类警情的一句话概括列表。\n"
        "请从中提炼不少于 3 个“重点问题”，用编号 （一）、（二）、（三）的方式列出，每个问题后要列出对应的警情案例"
        "每点控制在 1-3 句话，要求客观、专业、便于直接写入报告。\n\n"
        f"【警情概括列表】\n{all_case_summaries}\n"
    )
    return llm_chat(client, user_prompt, system_prompt, max_tokens=800)


def analyze_overall_measures(
    client: Any, all_case_summaries: str, problems_text: str
) -> str:
    system_prompt = (
        "你是一名公安情报分析员，擅长根据问题提出有针对性的管控措施。"
    )
    user_prompt = (
        "下面是近期涉黄、赌博、打架斗殴类警情的概括信息和已经提炼出的重点问题。\n"
        "请围绕这些问题，提出“下一步措施”建议，用编号 （一）、（二）、（三） 的方式列出，"
        "每点 1-3 句话，要求具体、可操作、便于直接写入报告。\n\n"
        f"【警情概括列表】\n{all_case_summaries}\n\n"
        f"【重点问题】\n{problems_text}\n"
    )
    return llm_chat(client, user_prompt, system_prompt, max_tokens=800)


def compute_basic_stats(df: pd.DataFrame) -> Dict[str, Any]:
    total_count = int(len(df))

    type_counts = df["警情性质"].value_counts()
    type_parts = [f"{t}类警情{cnt}起" for t, cnt in type_counts.items()]
    type_summary_str = "，".join(type_parts) if type_parts else "无相关警情"

    region_counts = df["地区"].value_counts()
    region_parts = [f"{region}{cnt}起" for region, cnt in region_counts.items()]
    region_summary_str = "，".join(region_parts) if region_parts else "无相关警情"

    return {
        "total_count": total_count,
        "type_summary_str": type_summary_str,
        "region_summary_str": region_summary_str,
    }


def build_nested_list_for_type(
    df: pd.DataFrame, type_value: str
) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []

    df_type = df[df["警情性质"] == type_value]
    if df_type.empty:
        return result

    for region_name, df_region in df_type.groupby("地区"):
        stations: List[Dict[str, Any]] = []
        for station_name, df_station in df_region.groupby("派出所"):
            cases = df_station["case_summary"].tolist()
            count = len(cases)
            summary_text = "；".join(
                f"{idx + 1}. {text}" for idx, text in enumerate(cases)
            )
            stations.append(
                {
                    "name": station_name,
                    "count": count,
                    "cases": cases,
                    "summary_text": summary_text,
                }
            )

        result.append(
            {
                "cmdname": region_name,
                "stations": stations,
            }
        )

    return result


def read_cases_from_db(start_dt: str, end_dt: str) -> pd.DataFrame:
    connection = None
    try:
        connection = get_database_connection()
        with connection.cursor() as cursor:
            cursor.execute(SQL_QUERY, (start_dt, end_dt))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as exc:
        logging.error("读取警情数据失败: %s", exc)
        raise
    finally:
        if connection is not None:
            connection.close()


def generate_yanpan_report(
    start_time_str: str,
    end_time_str: str,
    template_path: Path,
) -> YanpanReportResult:
    """
    生成警情研判报告。

    参数:
        start_time_str: 页面传入的开始时间（datetime-local 值，例如 2025-01-01T00:00:00）
        end_time_str: 页面传入的结束时间
        template_path: 模板文件路径
    """
    if not start_time_str or not end_time_str:
        raise ValueError("开始时间和结束时间不能为空")

    def _normalize(dt_str: str) -> Tuple[datetime, str]:
        text = dt_str.strip()
        text = text.replace("T", " ")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            # 兜底：只取日期部分
            try:
                dt = datetime.strptime(text[:10], "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"无效的时间格式: {dt_str}")
        return dt, dt.strftime("%Y-%m-%d %H:%M:%S")

    start_dt, start_dt_str = _normalize(start_time_str)
    end_dt, end_dt_str = _normalize(end_time_str)

    report_date = start_dt.strftime("%Y年%m月%d日")

    if not template_path.exists():
        raise FileNotFoundError(f"模板文件不存在: {template_path}")

    df = read_cases_from_db(start_dt_str, end_dt_str)
    if df.empty:
        raise ValueError("指定时间段内没有相关警情数据")

    if "地区" in df.columns:
        df["地区_原始"] = df["地区"]
        df["地区"] = df["地区"].apply(clean_region_text)
    if "处警情况" in df.columns:
        df["处警情况_原始"] = df["处警情况"]
        df["处警情况"] = df["处警情况"].apply(clean_feedback_text)

    stats = compute_basic_stats(df)

    # 必须成功连接大模型服务，否则中断流程
    client = create_llm_client()

    logging.info("正在调用 LLM 对每条警情进行一句话概括...")
    summaries: List[str] = []
    for _, row in df.iterrows():
        summary = summarize_single_case(
            client,
            content=row.get("报警内容", ""),
            reply=row.get("处警情况", ""),
        )
        summaries.append(summary)
    df["case_summary"] = summaries

    def _format_case_for_analysis(row: pd.Series) -> str:  # type: ignore[name-defined]
        alarm_time = str(row.get("报警时间", "")).strip()
        region = str(row.get("地区", "")).strip()
        station = str(row.get("派出所", "")).strip()
        summary = str(row.get("case_summary", "")).strip()
        parts = [p for p in [alarm_time, region, station, summary] if p]
        return " ".join(parts)

    all_case_summaries = "\n".join(
        _format_case_for_analysis(row) for _, row in df.iterrows()
    )
    if len(all_case_summaries) > MAX_ANALYSIS_CHARS:
        all_case_summaries = all_case_summaries[:MAX_ANALYSIS_CHARS]

    logging.info("正在调用 LLM 生成“重点问题”和“下一步措施”...")
    llm_analysis_problems = analyze_overall_problems(client, all_case_summaries)
    llm_analysis_measures = analyze_overall_measures(
        client, all_case_summaries, llm_analysis_problems
    )

    fight_list = build_nested_list_for_type(df, "打架斗殴")
    gamble_list = build_nested_list_for_type(df, "赌博")
    sex_list = build_nested_list_for_type(df, "涉黄")

    context: Dict[str, Any] = {
        "report_date": report_date,
        "total_count": stats["total_count"],
        "type_summary_str": stats["type_summary_str"],
        "region_summary_str": stats["region_summary_str"],
        "fight_list": fight_list,
        "gamble_list": gamble_list,
        "sex_list": sex_list,
        "llm_analysis_problems": llm_analysis_problems,
        "llm_analysis_measures": llm_analysis_measures,
    }

    tpl = DocxTemplate(str(template_path))
    tpl.render(context)

    buffer = io.BytesIO()
    tpl.save(buffer)
    buffer.seek(0)

    # 导出文件名：{start-date}涉黄、赌、打架斗殴类警情研判分析报告.docx
    filename = f"{start_dt.strftime('%Y-%m-%d')}涉黄、赌、打架斗殴类警情研判分析报告.docx"
    return YanpanReportResult(content=buffer.read(), filename=filename)
