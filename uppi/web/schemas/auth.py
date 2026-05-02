"""Auth/session schemas for the additive Stage 2 web shell."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class LoginRequest(BaseModel):
    """Request payload for Stage 2 credential-based login."""

    username: str
    password: str
    pin: str


class AuthenticatedUser(BaseModel):
    """Minimal authenticated user shape for the MVP shell."""

    username: str


class AuthStatusResponse(BaseModel):
    """Response shape for successful login and current-user lookups."""

    authenticated: Literal[True]
    user: AuthenticatedUser


class LogoutResponse(BaseModel):
    """Response shape for logout."""

    authenticated: Literal[False]
