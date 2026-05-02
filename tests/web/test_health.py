"""Tests for the additive health endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from uppi.web.app import create_app
from uppi.web.config import WebAppConfig


def _make_client() -> TestClient:
    """Builds a TestClient with an explicit config for stable assertions."""
    app = create_app(
        WebAppConfig(
            app_name="UPPI API",
            app_version="0.1.0",
            environment="test",
            debug=False,
        )
    )
    return TestClient(app)


def test_get_health_live_returns_expected_shape_without_auth():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client()

    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "check": "live",
        "service": "UPPI API",
        "version": "0.1.0",
        "environment": "test",
    }


def test_get_health_ready_returns_expected_shape_without_auth():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client()

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "check": "ready",
        "service": "UPPI API",
        "version": "0.1.0",
        "environment": "test",
    }
