from __future__ import annotations

import logging
import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from zidingyi_baobiao.core.exceptions import ValidationError


_ENV_KEY = "ZDYBB_FERNET_KEY"


@lru_cache(maxsize=1)
def get_fernet() -> Fernet:
    """
    获取 Fernet 实例。

    说明：
    - 优先读取环境变量 ZDYBB_FERNET_KEY
    - 若未配置，则自动生成一次并打印提示（方便你复制到 .env 固化）
    """
    key = (os.getenv(_ENV_KEY) or "").strip()
    if not key:
        key = Fernet.generate_key().decode("utf-8")
        os.environ[_ENV_KEY] = key
        logging.warning(
            "未配置 %s，已为本次进程临时生成。请将该值写入仓库根目录 .env 以保证重启后可解密：%s=%s",
            _ENV_KEY,
            _ENV_KEY,
            key,
        )
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:
        raise ValidationError(f"{_ENV_KEY} 格式不正确：{exc}") from None


def encrypt_text(plaintext: str) -> str:
    """加密文本（用于 data_source.password_enc）。"""
    if plaintext is None:
        raise ValidationError("待加密内容不能为空")
    token = get_fernet().encrypt(str(plaintext).encode("utf-8"))
    return token.decode("utf-8")


def decrypt_text(token: str) -> str:
    """解密文本（用于 data_source.password_enc）。"""
    if not token:
        raise ValidationError("待解密内容不能为空")
    try:
        raw = get_fernet().decrypt(str(token).encode("utf-8"))
        return raw.decode("utf-8")
    except InvalidToken:
        raise ValidationError("解密失败：请确认 ZDYBB_FERNET_KEY 与历史加密密钥一致") from None

