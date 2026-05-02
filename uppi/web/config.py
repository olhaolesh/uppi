"""Web-shell configuration isolated from the current UPPI runtime surfaces."""

from __future__ import annotations

from dataclasses import dataclass

from decouple import config as env_config


def _parse_bool(value: str | bool | None, *, default: bool = False) -> bool:
    """Normalizes the current env-style boolean flags without side effects."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() == "true"


@dataclass(frozen=True)
class WebAppConfig:
    """Bootstrap config for the additive FastAPI shell."""

    app_name: str
    app_version: str
    environment: str
    debug: bool

    @classmethod
    def from_env(cls) -> "WebAppConfig":
        """Builds the web-shell config from dedicated env names only."""
        return cls(
            app_name=env_config("UPPI_WEB_APP_NAME", default="UPPI API").strip() or "UPPI API",
            app_version=env_config("UPPI_WEB_APP_VERSION", default="0.1.0").strip() or "0.1.0",
            environment=env_config("UPPI_WEB_ENV", default="local").strip() or "local",
            debug=_parse_bool(env_config("UPPI_WEB_DEBUG", default="False"), default=False),
        )
