"""Tests for the additive FastAPI app factory."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI

from uppi.web.app import create_app
from uppi.web.app import app as module_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig


def test_create_app_returns_fastapi_and_uses_explicit_config():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = WebAppConfig(
        app_name="UPPI API Test",
        app_version="1.2.3",
        environment="test",
        debug=True,
        auth=WebAuthConfig(
            username="operator",
            password="secret-password",
            pin="1234",
        ),
        session=WebSessionConfig(
            secret="test-session-secret",
            cookie_name="uppi_web_session_test",
            cookie_secure=False,
            max_age_seconds=1800,
        ),
    )

    built = create_app(cfg)

    assert isinstance(built, FastAPI)
    assert built.title == "UPPI API Test"
    assert built.version == "1.2.3"
    assert built.debug is True
    assert built.state.web_config == cfg
    assert any(middleware.cls.__name__ == "SessionMiddleware" for middleware in built.user_middleware)
    assert {route.path for route in built.routes} >= {
        "/attestazioni/generate",
        "/attestazioni/search",
        "/auth/login",
        "/auth/logout",
        "/auth/me",
        "/health/live",
        "/health/ready",
    }


def test_module_level_app_is_fastapi_instance():
    """Перевіряє сценарій, описаний у назві тесту."""
    assert isinstance(module_app, FastAPI)


def test_web_shell_import_does_not_pull_business_runtime_modules():
    """Перевіряє сценарій, описаний у назві тесту."""
    repo_root = Path(__file__).resolve().parents[2]
    banned_modules = [
        "uppi.ae.auth",
        "uppi.ae.captcha",
        "uppi.ae.download",
        "uppi.ae.sister_navigation",
        "uppi.ae.uppi_selectors",
        "uppi.services.prepare_by_cf",
        "uppi.services.bulk_import_clients_csv",
        "uppi.services.generation_runner",
        "uppi.services.import_only_runner",
        "uppi.services.visura_processor",
        "uppi.services.visura_stages",
        "uppi.spiders.uppi_spider",
        "uppi.spiders.uppi_browser_spider",
        "uppi.spiders.uppi_import_spider",
    ]
    script = f"""
import json
import sys
import uppi.web.app

banned = {banned_modules!r}
loaded = sorted(name for name in sys.modules if name in banned)
print(json.dumps(loaded))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == []
