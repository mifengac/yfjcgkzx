from __future__ import annotations


class AppError(Exception):
    """
    业务可读异常基类。

    - message: 对用户可读
    - code: 便于前端/调用方定位
    - http_status: HTTP 状态码
    """

    def __init__(self, message: str, *, code: str = "APP_ERROR", http_status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class NotFoundError(AppError):
    def __init__(self, message: str = "资源不存在", *, code: str = "NOT_FOUND") -> None:
        super().__init__(message, code=code, http_status=404)


class ValidationError(AppError):
    def __init__(self, message: str, *, code: str = "VALIDATION_ERROR") -> None:
        super().__init__(message, code=code, http_status=400)


class SqlSecurityError(AppError):
    def __init__(self, message: str, *, code: str = "SQL_SECURITY_ERROR") -> None:
        super().__init__(message, code=code, http_status=400)


class QueryExecutionError(AppError):
    def __init__(self, message: str, *, code: str = "QUERY_EXECUTION_ERROR") -> None:
        super().__init__(message, code=code, http_status=500)

