"""Pydantic schemas for the additive web shell."""

from .attestazioni import (
    AttestazioniGenerateRequest,
    AttestazioniGenerateResponse,
    AttestazioniSearchRequest,
    AttestazioniSearchResponse,
)
from .auth import AuthStatusResponse, AuthenticatedUser, LoginRequest, LogoutResponse
from .clients import ClientsBulkImportRequest, ClientsBulkImportResponse
from .health import HealthStatusResponse

__all__ = [
    "AttestazioniGenerateRequest",
    "AttestazioniGenerateResponse",
    "AttestazioniSearchRequest",
    "AttestazioniSearchResponse",
    "AuthStatusResponse",
    "AuthenticatedUser",
    "ClientsBulkImportRequest",
    "ClientsBulkImportResponse",
    "HealthStatusResponse",
    "LoginRequest",
    "LogoutResponse",
]
