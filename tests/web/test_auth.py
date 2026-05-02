"""Tests for Stage 2 auth endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig


def _make_auth_config() -> WebAppConfig:
    """Builds an explicit test config for auth/session assertions."""
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


def _make_client() -> tuple[TestClient, WebAppConfig]:
    """Creates a TestClient and returns it with the explicit test config."""
    cfg = _make_auth_config()
    return TestClient(create_app(cfg)), cfg


def test_login_success_sets_session_cookie_and_hides_credentials_from_response():
    """Перевіряє сценарій, описаний у назві тесту."""
    client, cfg = _make_client()

    response = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "authenticated": True,
        "user": {"username": "operator"},
    }
    assert "password" not in response.json()
    assert "pin" not in response.json()
    assert "secret-password" not in response.text
    assert "1234" not in response.text
    assert cfg.session.cookie_name in response.headers["set-cookie"]
    assert client.cookies.get(cfg.session.cookie_name) is not None


def test_login_with_wrong_password_returns_401():
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client()

    response = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "wrong-password",
            "pin": "1234",
        },
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}


def test_login_with_wrong_pin_returns_401():
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client()

    response = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "9999",
        },
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid credentials"}


def test_auth_me_requires_session_but_health_endpoints_stay_public():
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client()

    me_response = client.get("/auth/me")
    live_response = client.get("/health/live")
    ready_response = client.get("/health/ready")

    assert me_response.status_code == 401
    assert me_response.json() == {"detail": "Authentication required"}
    assert live_response.status_code == 200
    assert ready_response.status_code == 200


def test_auth_me_returns_current_user_after_login():
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client()
    login = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )
    assert login.status_code == 200

    response = client.get("/auth/me")

    assert response.status_code == 200
    assert response.json() == {
        "authenticated": True,
        "user": {"username": "operator"},
    }
