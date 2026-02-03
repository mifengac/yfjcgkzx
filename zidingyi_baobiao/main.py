from __future__ import annotations

import logging
import os
from typing import Any

from flask import Flask, jsonify

from zidingyi_baobiao.api.datasource import datasource_bp
from zidingyi_baobiao.api.dataset import dataset_bp
from zidingyi_baobiao.api.module import module_bp
from zidingyi_baobiao.core.exceptions import AppError


def create_app() -> Flask:
    """
    Flask App Factory。

    说明：
    - 本模块作为“自定义驱动报表系统”的独立后端，可单独运行。
    - 数据库连接配置复用 yfjcgkzx/gonggong/config 中的 .env 读取与连接参数。
    """
    app = Flask(__name__)

    _configure_logging()
    _register_error_handlers(app)

    app.register_blueprint(datasource_bp)
    app.register_blueprint(dataset_bp)
    app.register_blueprint(module_bp)

    @app.get("/health")
    def health() -> Any:
        return jsonify({"success": True, "status": "ok"})

    return app


def _configure_logging() -> None:
    log_level = os.getenv("ZDYBB_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s",
    )


def _register_error_handlers(app: Flask) -> None:
    @app.errorhandler(AppError)
    def _handle_app_error(exc: AppError):  # type: ignore[no-untyped-def]
        return jsonify({"success": False, "message": str(exc), "code": exc.code}), exc.http_status

    @app.errorhandler(Exception)
    def _handle_uncaught(exc: Exception):  # type: ignore[no-untyped-def]
        logging.exception("Unhandled error: %s", exc)
        return jsonify({"success": False, "message": "服务器内部错误"}), 500


if __name__ == "__main__":
    # 默认端口与仓库主应用（5003）错开，避免冲突
    app = create_app()
    app.run(host="0.0.0.0", port=int(os.getenv("ZDYBB_PORT") or 5004), debug=os.getenv("ZDYBB_DEBUG") == "1")
