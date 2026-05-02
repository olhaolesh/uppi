"""Tests for Stage 2 cookie session behavior and auth dependency reuse."""

from __future__ import annotations

from fastapi import Depends
from fastapi.testclient import TestClient

from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.security import require_authenticated_user


def _make_auth_config() -> WebAppConfig:
    """Builds an explicit test config for session assertions."""
    return WebAppConfig(
        app_name="UPPI API",
        app_version="0.1.0",
        environment="test",
        debug=False,
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


def test_logout_clears_session_and_auth_me_becomes_unauthorized():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = TestClient(create_app(_make_auth_config()))
    login = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )
    assert login.status_code == 200

    logout = client.post("/auth/logout")

    assert logout.status_code == 200
    assert logout.json() == {"authenticated": False}
    assert client.get("/auth/me").status_code == 401


def test_require_authenticated_user_can_protect_future_routes():
    """Перевіряє сценарій, описаний у назві тесту."""
    app = create_app(_make_auth_config())

    @app.get("/_test/protected")
    def protected(user: AuthenticatedUser = Depends(require_authenticated_user)) -> dict[str, str]:
        return {"username": user.username}

    client = TestClient(app)

    unauthorized = client.get("/_test/protected")
    assert unauthorized.status_code == 401

    login = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )
    assert login.status_code == 200

    authorized = client.get("/_test/protected")

    assert authorized.status_code == 200
    assert authorized.json() == {"username": "operator"}
