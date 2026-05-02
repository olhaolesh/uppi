"""Application factory for the additive FastAPI shell."""

from __future__ import annotations

from fastapi import FastAPI

from uppi.web.api import api_router
from uppi.web.config import WebAppConfig


def create_app(config: WebAppConfig | None = None) -> FastAPI:
    """Builds the isolated FastAPI shell without importing business runtime flows."""
    resolved_config = config or WebAppConfig.from_env()
    app = FastAPI(
        title=resolved_config.app_name,
        version=resolved_config.app_version,
        debug=resolved_config.debug,
    )
    app.state.web_config = resolved_config
    app.include_router(api_router)
    return app


app = create_app()
