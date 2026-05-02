"""Response schemas for the additive health endpoints."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class HealthStatusResponse(BaseModel):
    """Shared response model for simple liveness/readiness checks."""

    status: Literal["ok"]
    check: Literal["live", "ready"]
    service: str
    version: str
    environment: str
