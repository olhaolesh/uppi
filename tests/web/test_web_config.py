"""Tests for the isolated web-shell configuration."""

from __future__ import annotations

from uppi.web.config import WebAppConfig


def test_web_app_config_from_env_uses_defaults_when_env_is_absent(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.delenv("UPPI_WEB_APP_NAME", raising=False)
    monkeypatch.delenv("UPPI_WEB_APP_VERSION", raising=False)
    monkeypatch.delenv("UPPI_WEB_ENV", raising=False)
    monkeypatch.delenv("UPPI_WEB_DEBUG", raising=False)

    cfg = WebAppConfig.from_env()

    assert cfg == WebAppConfig(
        app_name="UPPI API",
        app_version="0.1.0",
        environment="local",
        debug=False,
    )


def test_web_app_config_from_env_uses_explicit_env_overrides(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("UPPI_WEB_APP_NAME", "UPPI API Test")
    monkeypatch.setenv("UPPI_WEB_APP_VERSION", "9.9.9")
    monkeypatch.setenv("UPPI_WEB_ENV", "test")
    monkeypatch.setenv("UPPI_WEB_DEBUG", "True")

    cfg = WebAppConfig.from_env()

    assert cfg == WebAppConfig(
        app_name="UPPI API Test",
        app_version="9.9.9",
        environment="test",
        debug=True,
    )
