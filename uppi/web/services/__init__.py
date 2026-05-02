"""Web-facing adapters and services for the additive shell."""

from .auth import authenticate_login, build_session_identity
from .prepare_adapter import PrepareSearchAdapter, PreparedSearchResult

__all__ = [
    "PrepareSearchAdapter",
    "PreparedSearchResult",
    "authenticate_login",
    "build_session_identity",
]
