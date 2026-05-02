"""Health endpoints for the additive UPPI FastAPI shell."""

from __future__ import annotations

from fastapi import APIRouter, Request

from uppi.web.config import WebAppConfig
from uppi.web.schemas.health import HealthStatusResponse

router = APIRouter(prefix="/health", tags=["health"])


def _current_web_config(request: Request) -> WebAppConfig:
    """Returns the app-scoped web config without touching runtime business code."""
    return request.app.state.web_config


def _health_response(*, check: str, cfg: WebAppConfig) -> HealthStatusResponse:
    """Builds the shared health payload shape."""
    return HealthStatusResponse(
        status="ok",
        check=check,
        service=cfg.app_name,
        version=cfg.app_version,
        environment=cfg.environment,
    )


@router.get("/live", response_model=HealthStatusResponse)
def live(request: Request) -> HealthStatusResponse:
    """Reports that the isolated web shell process is alive."""
    return _health_response(check="live", cfg=_current_web_config(request))


@router.get("/ready", response_model=HealthStatusResponse)
def ready(request: Request) -> HealthStatusResponse:
    """Reports readiness of the isolated web shell only."""
    return _health_response(check="ready", cfg=_current_web_config(request))
