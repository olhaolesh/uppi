"""Stage 2 auth helpers for the additive UPPI web shell."""

from __future__ import annotations

from secrets import compare_digest

from uppi.web.config import WebAuthConfig
from uppi.web.schemas.auth import AuthenticatedUser, LoginRequest


def authenticate_login(payload: LoginRequest, auth_config: WebAuthConfig) -> AuthenticatedUser | None:
    """Validates static Stage 2 credentials without touching business runtime code."""
    if not compare_digest(payload.username, auth_config.username):
        return None
    if not compare_digest(payload.password, auth_config.password):
        return None
    if not compare_digest(payload.pin, auth_config.pin):
        return None
    return AuthenticatedUser(username=auth_config.username)


def build_session_identity(user: AuthenticatedUser) -> dict[str, str]:
    """Serializes the current user into the minimal cookie-session payload."""
    return {"username": user.username}
