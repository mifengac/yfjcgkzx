from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Set

from zidingyi_baobiao.core.exceptions import SqlSecurityError


_LEADING_COMMENT_RE = re.compile(r"^(\s*(--[^\n]*\n|\s*/\*.*?\*/\s*))+", re.DOTALL)
_START_TOKEN_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_NAMED_PARAM_RE = re.compile(r"(?<!:):[a-zA-Z_][a-zA-Z0-9_]*")


@dataclass(frozen=True)
class SqlValidationResult:
    named_params: Set[str]


def _strip_leading_comments(sql: str) -> str:
    """
    去掉开头的 SQL 注释（-- 或 /* */），避免影响 SELECT/WITH 起始判断。
    """
    s = sql.lstrip("\ufeff")  # 兼容 UTF-8 BOM
    m = _LEADING_COMMENT_RE.match(s)
    return s[m.end() :] if m else s


def validate_sql_template(sql: str) -> SqlValidationResult:
    """
    SQL 安全校验（保存 dataset 与执行前都必须调用）。

    规则（严格）：
    - 仅允许 SELECT/WITH 起始（允许 CTE）
    - 禁止：insert/update/delete/drop/alter/truncate/; / do $$ / call / exec
    - 禁止使用位置参数（? / %s），必须使用命名参数 :param
    """
    if not isinstance(sql, str) or not sql.strip():
        raise SqlSecurityError("SQL 不能为空")

    normalized = _strip_leading_comments(sql).strip()
    if not _START_TOKEN_RE.match(normalized):
        raise SqlSecurityError("SQL 仅允许以 SELECT 或 WITH 开头")

    lowered = normalized.lower()

    # 严禁分号：防止多语句
    if ";" in lowered:
        raise SqlSecurityError("SQL 禁止包含分号（;）")

    banned_keyword_patterns: Iterable[str] = (
        r"\binsert\b",
        r"\bupdate\b",
        r"\bdelete\b",
        r"\bdrop\b",
        r"\balter\b",
        r"\btruncate\b",
        r"\bcall\b",
        r"\bexec\b",
        r"\bexecute\b",
        r"\bcreate\b",
        r"\bgrant\b",
        r"\brevoke\b",
    )
    for pat in banned_keyword_patterns:
        if re.search(pat, lowered, flags=re.IGNORECASE):
            raise SqlSecurityError(f"SQL 包含禁止关键字：{pat.strip('\\\\b')}")

    if re.search(r"\bdo\s*\$\$", lowered, flags=re.IGNORECASE):
        raise SqlSecurityError("SQL 禁止包含 DO $$ 块")

    # 禁止位置参数
    if "?" in lowered or "%s" in lowered:
        raise SqlSecurityError("SQL 参数必须使用命名参数（:param），禁止 ? / %s 位置参数")

    named_params = {m.group(0)[1:] for m in _NAMED_PARAM_RE.finditer(normalized)}

    return SqlValidationResult(named_params=named_params)


def extract_named_params(sql: str) -> Set[str]:
    """提取 SQL 中的命名参数列表（不含前导冒号）。"""
    return validate_sql_template(sql).named_params


def wrap_limit(sql: str, *, limit_param: str) -> str:
    """
    为任意 SELECT/WITH SQL 包装一层 LIMIT。

    注意：
    - 这里会拼接 SQL 字符串，但仅拼接经过 validate_sql_template 校验的 SQL 模板；
      且 LIMIT 使用绑定参数（:limit_param），避免注入风险。
    """
    validate_sql_template(sql)
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", limit_param):
        raise SqlSecurityError("limit 参数名非法")
    return f"SELECT * FROM ( {sql} ) AS t LIMIT :{limit_param}"


def to_psycopg2_named_paramstyle(sql: str) -> str:
    """
    将 :param 风格的命名参数转换为 psycopg2 支持的 %(param)s 风格。

    注意：
    - 会跳过单引号字符串、双引号标识符、以及 dollar-quoted 字符串
    - 会跳过 :: 类型转换（避免把 : 误判为参数）
    """
    validate_sql_template(sql)
    s = sql
    out: list[str] = []
    i = 0
    n = len(s)

    in_single = False
    in_double = False
    dollar_tag: str | None = None

    def startswith_at(prefix: str, pos: int) -> bool:
        return s.startswith(prefix, pos)

    while i < n:
        ch = s[i]

        # dollar quote start/end: $tag$
        if not in_single and not in_double and ch == "$":
            j = i + 1
            while j < n and re.match(r"[a-zA-Z0-9_]", s[j]):
                j += 1
            if j < n and s[j] == "$":
                tag = s[i : j + 1]  # includes ending $
                if dollar_tag is None:
                    dollar_tag = tag
                    out.append(tag)
                    i = j + 1
                    continue
                if dollar_tag == tag:
                    dollar_tag = None
                    out.append(tag)
                    i = j + 1
                    continue
            # fallthrough if not a valid tag

        if dollar_tag is not None:
            out.append(ch)
            i += 1
            continue

        # single-quoted string
        if not in_double and ch == "'":
            out.append(ch)
            if in_single:
                # handle escaped ''
                if i + 1 < n and s[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_single = False
            else:
                in_single = True
            i += 1
            continue

        # double-quoted identifier
        if not in_single and ch == '"':
            out.append(ch)
            if in_double:
                if i + 1 < n and s[i + 1] == '"':
                    out.append('"')
                    i += 2
                    continue
                in_double = False
            else:
                in_double = True
            i += 1
            continue

        if in_single or in_double:
            out.append(ch)
            i += 1
            continue

        # parameter :name (not :: cast)
        if ch == ":" and not startswith_at("::", i):
            if i + 1 < n and re.match(r"[a-zA-Z_]", s[i + 1]):
                j = i + 2
                while j < n and re.match(r"[a-zA-Z0-9_]", s[j]):
                    j += 1
                name = s[i + 1 : j]
                out.append(f"%({name})s")
                i = j
                continue

        out.append(ch)
        i += 1

    return "".join(out)
