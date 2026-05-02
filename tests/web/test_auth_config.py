"""Tests for Stage 2 auth/session web config."""

from __future__ import annotations

import pytest

from uppi.web.config import WebAppConfig


def test_web_auth_and_session_defaults_are_available_in_test_environment(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_WEB_ENV", "test")
    monkeypatch.delenv("UPPI_WEB_AUTH_USERNAME", raising=False)
    monkeypatch.delenv("UPPI_WEB_AUTH_PASSWORD", raising=False)
    monkeypatch.delenv("UPPI_WEB_AUTH_PIN", raising=False)
    monkeypatch.delenv("UPPI_WEB_SESSION_SECRET", raising=False)
    monkeypatch.delenv("UPPI_WEB_SESSION_COOKIE_NAME", raising=False)
    monkeypatch.delenv("UPPI_WEB_SESSION_COOKIE_SECURE", raising=False)
    monkeypatch.delenv("UPPI_WEB_SESSION_MAX_AGE_SECONDS", raising=False)

    cfg = WebAppConfig.from_env()

    assert cfg.auth.username == "operator"
    assert cfg.auth.password == "changeme"
    assert cfg.auth.pin == "0000"
    assert cfg.session.secret == "uppi-web-dev-session-secret-not-for-production"
    assert cfg.session.cookie_name == "uppi_web_session"
    assert cfg.session.cookie_secure is False
    assert cfg.session.max_age_seconds == 28800


def test_web_auth_and_session_env_overrides_are_applied(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_WEB_ENV", "test")
    monkeypatch.setenv("UPPI_WEB_AUTH_USERNAME", "test-operator")
    monkeypatch.setenv("UPPI_WEB_AUTH_PASSWORD", "test-password")
    monkeypatch.setenv("UPPI_WEB_AUTH_PIN", "7777")
    monkeypatch.setenv("UPPI_WEB_SESSION_SECRET", "test-session-secret")
    monkeypatch.setenv("UPPI_WEB_SESSION_COOKIE_NAME", "uppi_test_session")
    monkeypatch.setenv("UPPI_WEB_SESSION_COOKIE_SECURE", "True")
    monkeypatch.setenv("UPPI_WEB_SESSION_MAX_AGE_SECONDS", "900")

    cfg = WebAppConfig.from_env()

    assert cfg.auth.username == "test-operator"
    assert cfg.auth.password == "test-password"
    assert cfg.auth.pin == "7777"
    assert cfg.session.secret == "test-session-secret"
    assert cfg.session.cookie_name == "uppi_test_session"
    assert cfg.session.cookie_secure is True
    assert cfg.session.max_age_seconds == 900


def test_production_like_config_without_explicit_session_secret_fails_fast(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_WEB_ENV", "production")
    monkeypatch.setenv("UPPI_WEB_AUTH_USERNAME", "operator")
    monkeypatch.setenv("UPPI_WEB_AUTH_PASSWORD", "secret-password")
    monkeypatch.setenv("UPPI_WEB_AUTH_PIN", "1234")
    monkeypatch.delenv("UPPI_WEB_SESSION_SECRET", raising=False)

    with pytest.raises(ValueError, match="UPPI_WEB_SESSION_SECRET"):
        WebAppConfig.from_env()
