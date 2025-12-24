"""
请求相关的通用工具函数。
"""

from flask import Request


def get_client_ip(req: Request) -> str:
    """
    获取客户端 IP 地址。

    处理逻辑：
    1. 优先读取 `X-Forwarded-For`，解决反向代理部署场景下真实 IP 的获取问题。
    2. 再尝试读取 `X-Real-IP`，兼容部分代理服务器的写法。
    3. 若上述均不存在，则使用 Flask 提供的 `remote_addr`。
    """
    forwarded_for = req.headers.get("X-Forwarded-For")
    if forwarded_for:
        # 该头可能包含多个 IP，按照约定第一个即为真实访客 IP。
        return forwarded_for.split(",")[0].strip()

    real_ip = req.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()

    return req.remote_addr or "0.0.0.0"
