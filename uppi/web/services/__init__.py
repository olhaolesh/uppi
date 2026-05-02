"""Web-facing adapters and services for the additive shell."""

from .auth import authenticate_login, build_session_identity

__all__ = ["authenticate_login", "build_session_identity"]
