"""Application factory for the additive FastAPI shell."""

from __future__ import annotations

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from uppi.web.api import api_router
from uppi.web.config import WebAppConfig


def create_app(
    config: WebAppConfig | None = None,
    *,
    prepare_search_adapter: object | None = None,
) -> FastAPI:
    """Builds the isolated FastAPI shell without importing business runtime flows."""
    resolved_config = config or WebAppConfig.from_env()
    app = FastAPI(
        title=resolved_config.app_name,
        version=resolved_config.app_version,
        debug=resolved_config.debug,
    )
    app.state.web_config = resolved_config
    app.add_middleware(
        SessionMiddleware,
        secret_key=resolved_config.session.secret,
        session_cookie=resolved_config.session.cookie_name,
        max_age=resolved_config.session.max_age_seconds,
        same_site=resolved_config.session.cookie_samesite,
        https_only=resolved_config.session.cookie_secure,
    )
    if prepare_search_adapter is not None:
        app.state.prepare_search_adapter = prepare_search_adapter
    app.include_router(api_router)
    return app


app = create_app()
