"""Reusable auth/session dependencies for future protected web routes."""

from __future__ import annotations

from fastapi import HTTPException, Request, status

from uppi.web.schemas.auth import AuthenticatedUser

SESSION_USER_KEY = "user"


def get_current_user(request: Request) -> AuthenticatedUser | None:
    """Returns the current authenticated user from the signed session cookie."""
    session = request.session
    payload = session.get(SESSION_USER_KEY)
    if not isinstance(payload, dict):
        return None

    username = payload.get("username")
    if not isinstance(username, str) or not username.strip():
        return None

    return AuthenticatedUser(username=username)


def require_authenticated_user(request: Request) -> AuthenticatedUser:
    """Raises 401 unless a valid signed cookie session already exists."""
    user = get_current_user(request)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    return user
