"""
统一错误处理工具
提供统一的错误处理和日志输出功能
"""

import logging
import traceback
from functools import wraps
from flask import jsonify

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s"
)


def handle_errors(operation_name="操作"):
    """
    统一错误处理装饰器

    Args:
        operation_name (str): 操作名称，用于日志记录

    Returns:
        decorator: 装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                logging.info(f"开始执行 {operation_name}")
                result = func(*args, **kwargs)
                logging.info(f"{operation_name} 执行成功")
                return result
            except ValueError as e:
                logging.error(f"{operation_name} 参数错误: {e}")
                return jsonify({
                    'success': False,
                    'message': f'参数错误: {str(e)}'
                }), 400
            except ConnectionError as e:
                logging.error(f"{operation_name} 连接错误: {e}")
                return jsonify({
                    'success': False,
                    'message': '网络连接失败，请稍后重试'
                }), 503
            except TimeoutError as e:
                logging.error(f"{operation_name} 超时错误: {e}")
                return jsonify({
                    'success': False,
                    'message': '操作超时，请稍后重试'
                }), 408
            except Exception as e:
                logging.error(f"{operation_name} 执行失败: {e}")
                logging.error(f"错误详情: {traceback.format_exc()}")
                return jsonify({
                    'success': False,
                    'message': f'{operation_name}失败: {str(e)}'
                }), 500
        return wrapper
    return decorator


def log_info(message: str):
    """统一的信息日志输出"""
    logging.info(message)


def log_error(message: str, exception: Exception = None):
    """统一的错误日志输出"""
    if exception:
        logging.error(f"{message}: {exception}")
        logging.error(f"错误详情: {traceback.format_exc()}")
    else:
        logging.error(message)


def log_warning(message: str):
    """统一的警告日志输出"""
    logging.warning(message)


def log_debug(message: str):
    """统一的调试日志输出"""
    logging.debug(message)