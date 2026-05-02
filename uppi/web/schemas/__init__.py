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
from .jobs import JobDetailResponse, JobListItemResponse, JobsListResponse

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
    "JobDetailResponse",
    "JobListItemResponse",
    "JobsListResponse",
    "LoginRequest",
    "LogoutResponse",
]
