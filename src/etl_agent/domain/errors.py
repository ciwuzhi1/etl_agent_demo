class DomainError(Exception):
    """可安全转换为 4xx 响应的领域错误。"""


class InvalidStateTransition(DomainError):
    pass


class PermissionDenied(DomainError):
    pass


class VersionConflict(DomainError):
    pass


class ResourceNotFound(DomainError):
    pass


class ExternalServiceUnavailable(DomainError):
    """外部依赖暂时不可用；API 映射 503，异步任务可按有限次数安全重试。"""
