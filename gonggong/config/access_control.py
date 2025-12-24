"""
模块访问控制配置。

通过配置不同 IP 地址对应的模块列表，实现模块入口的精细化展示与保护。
"""

from typing import Dict, Iterable, List, Set

# -----------------------------------------------------------------------------
# 访问控制基表
# -----------------------------------------------------------------------------
# 字典结构说明：
# - key  为字符串形式的 IP 地址或特殊关键字 "default"。
# - value 为该 IP 可访问的模块 key 列表（需与 MODULE_DEFINITIONS 中的 key 对应）。
MODULE_ACCESS_RULES: Dict[str, List[str]] = {
    "default": ["jingqing_anjian"],  # 默认任何 IP 皆可访问警情案件模块
    "127.0.0.1": ["xunfang", "zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.114": ["xunfang", "zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.193": ["xunfang", "zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.109": ["xunfang", "zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.175": ["xunfang", "zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.66": ["zhizong"],  # 本机示例：额外开放巡防与治综模块
    "68.253.16.126": ["zhizong"],
    "68.253.16.80": ["zhizong"],
    "68.253.27.90": ["xunfang"],
    "68.253.27.187": ["xunfang", "zhizong"],
    "68.253.27.159": ["xunfang", "zhizong"],
}


def get_allowed_modules(ip: str) -> Set[str]:
    """
    获取某 IP 可访问的模块 key 集合。
    默认模块会自动包含在结果中。
    """
    allowed: Set[str] = set(MODULE_ACCESS_RULES.get("default", []))
    allowed.update(MODULE_ACCESS_RULES.get(ip, []))
    return allowed


def is_module_allowed(module_key: str, ip: str) -> bool:
    """
    判断指定模块对于当前 IP 是否可见。
    """
    return module_key in get_allowed_modules(ip)


def filter_modules(modules: Iterable[str], ip: str) -> List[str]:
    """
    在给定模块列表中，筛选出当前 IP 可访问的模块。
    常用于基于配置动态构建菜单或按钮。
    """
    allowed = get_allowed_modules(ip)
    return [name for name in modules if name in allowed]
