"""Auth/session endpoints for the additive Stage 2 web shell."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from uppi.web.config import WebAppConfig
from uppi.web.schemas.auth import (
    AuthStatusResponse,
    AuthenticatedUser,
    LoginRequest,
    LogoutResponse,
)
from uppi.web.security import SESSION_USER_KEY, require_authenticated_user
from uppi.web.services.auth import authenticate_login, build_session_identity

router = APIRouter(prefix="/auth", tags=["auth"])


def _web_config(request: Request) -> WebAppConfig:
    """Returns the app-scoped web config for auth/session endpoints."""
    return request.app.state.web_config


@router.post("/login", response_model=AuthStatusResponse)
def login(payload: LoginRequest, request: Request) -> AuthStatusResponse:
    """Creates a signed cookie session for the static MVP operator credentials."""
    user = authenticate_login(payload, _web_config(request).auth)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    request.session.clear()
    request.session[SESSION_USER_KEY] = build_session_identity(user)
    return AuthStatusResponse(authenticated=True, user=user)


@router.get("/me", response_model=AuthStatusResponse)
def me(user: AuthenticatedUser = Depends(require_authenticated_user)) -> AuthStatusResponse:
    """Returns the current user for an existing signed cookie session."""
    return AuthStatusResponse(authenticated=True, user=user)


@router.post("/logout", response_model=LogoutResponse)
def logout(request: Request) -> LogoutResponse:
    """Clears the web cookie session only."""
    request.session.clear()
    return LogoutResponse(authenticated=False)
