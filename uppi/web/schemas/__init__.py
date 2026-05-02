"""Pydantic schemas for the additive web shell."""

from .attestazioni import (
    AttestazioniGenerateRequest,
    AttestazioniGenerateResponse,
    AttestazioniSearchRequest,
    AttestazioniSearchResponse,
)
from .auth import AuthStatusResponse, AuthenticatedUser, LoginRequest, LogoutResponse
from .health import HealthStatusResponse

__all__ = [
    "AttestazioniGenerateRequest",
    "AttestazioniGenerateResponse",
    "AttestazioniSearchRequest",
    "AttestazioniSearchResponse",
    "AuthStatusResponse",
    "AuthenticatedUser",
    "HealthStatusResponse",
    "LoginRequest",
    "LogoutResponse",
]
