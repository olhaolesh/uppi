"""Web-shell configuration isolated from the current UPPI runtime surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from decouple import config as env_config

_DEVELOPMENT_ENVIRONMENTS = {"local", "test", "testing", "dev", "development"}
_DEFAULT_AUTH_USERNAME = "operator"
_DEFAULT_AUTH_PASSWORD = "changeme"
_DEFAULT_AUTH_PIN = "0000"
_DEFAULT_SESSION_SECRET = "uppi-web-dev-session-secret-not-for-production"
_DEFAULT_SESSION_COOKIE_NAME = "uppi_web_session"
_DEFAULT_SESSION_MAX_AGE_SECONDS = 28800


def _parse_bool(value: str | bool | None, *, default: bool = False) -> bool:
    """Normalizes the current env-style boolean flags without side effects."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() == "true"


def _parse_int(value: str | int | None, *, default: int) -> int:
    """Parses int-like env values with a stable fallback."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    raw = str(value).strip()
    if not raw:
        return default
    return int(raw)


def _normalized_environment(value: str | None) -> str:
    """Normalizes the web environment name."""
    if value is None:
        return "local"
    normalized = value.strip()
    return normalized or "local"


def _is_development_environment(environment: str) -> bool:
    """Returns whether safe local/test defaults are allowed."""
    return environment.strip().lower() in _DEVELOPMENT_ENVIRONMENTS


def _get_optional_env(name: str) -> str | None:
    """Returns a non-empty env value or None without masking missing settings."""
    raw = os.getenv(name)
    if raw is None:
        return None
    return raw if raw.strip() else None


@dataclass(frozen=True)
class WebAuthConfig:
    """Static credentials for the Stage 2 web auth shell only."""

    username: str
    password: str
    pin: str

    @classmethod
    def local_default(cls) -> "WebAuthConfig":
        """Returns safe local/test defaults for the additive web shell."""
        return cls(
            username=_DEFAULT_AUTH_USERNAME,
            password=_DEFAULT_AUTH_PASSWORD,
            pin=_DEFAULT_AUTH_PIN,
        )

    @classmethod
    def from_env(cls, *, environment: str) -> "WebAuthConfig":
        """Loads auth credentials from env or fails fast in production-like envs."""
        username = _get_optional_env("UPPI_WEB_AUTH_USERNAME")
        password = _get_optional_env("UPPI_WEB_AUTH_PASSWORD")
        pin = _get_optional_env("UPPI_WEB_AUTH_PIN")

        if _is_development_environment(environment):
            return cls(
                username=(username.strip() if username is not None else _DEFAULT_AUTH_USERNAME),
                password=password or _DEFAULT_AUTH_PASSWORD,
                pin=pin or _DEFAULT_AUTH_PIN,
            )

        missing = [
            name
            for name, value in (
                ("UPPI_WEB_AUTH_USERNAME", username),
                ("UPPI_WEB_AUTH_PASSWORD", password),
                ("UPPI_WEB_AUTH_PIN", pin),
            )
            if value is None
        ]
        if missing:
            raise ValueError(
                "Production-like web auth config requires explicit values for: "
                + ", ".join(missing)
            )

        return cls(
            username=username.strip(),
            password=password,
            pin=pin,
        )


@dataclass(frozen=True)
class WebSessionConfig:
    """Cookie session config for the Stage 2 web shell."""

    secret: str
    cookie_name: str
    cookie_secure: bool
    max_age_seconds: int
    cookie_samesite: str = "lax"

    @classmethod
    def local_default(cls) -> "WebSessionConfig":
        """Returns safe local/test session defaults."""
        return cls(
            secret=_DEFAULT_SESSION_SECRET,
            cookie_name=_DEFAULT_SESSION_COOKIE_NAME,
            cookie_secure=False,
            max_age_seconds=_DEFAULT_SESSION_MAX_AGE_SECONDS,
        )

    @classmethod
    def from_env(cls, *, environment: str) -> "WebSessionConfig":
        """Loads cookie-session config from env or fails fast in production-like envs."""
        secret = _get_optional_env("UPPI_WEB_SESSION_SECRET")
        if secret is None:
            if _is_development_environment(environment):
                secret = _DEFAULT_SESSION_SECRET
            else:
                raise ValueError(
                    "Production-like web session config requires explicit "
                    "UPPI_WEB_SESSION_SECRET"
                )

        return cls(
            secret=secret,
            cookie_name=(
                env_config("UPPI_WEB_SESSION_COOKIE_NAME", default=_DEFAULT_SESSION_COOKIE_NAME).strip()
                or _DEFAULT_SESSION_COOKIE_NAME
            ),
            cookie_secure=_parse_bool(
                _get_optional_env("UPPI_WEB_SESSION_COOKIE_SECURE"),
                default=not _is_development_environment(environment),
            ),
            max_age_seconds=_parse_int(
                env_config(
                    "UPPI_WEB_SESSION_MAX_AGE_SECONDS",
                    default=str(_DEFAULT_SESSION_MAX_AGE_SECONDS),
                ),
                default=_DEFAULT_SESSION_MAX_AGE_SECONDS,
            ),
        )


@dataclass(frozen=True)
class WebAppConfig:
    """Bootstrap config for the additive FastAPI shell."""

    app_name: str
    app_version: str
    environment: str
    debug: bool
    auth: WebAuthConfig = field(default_factory=WebAuthConfig.local_default)
    session: WebSessionConfig = field(default_factory=WebSessionConfig.local_default)

    @classmethod
    def from_env(cls) -> "WebAppConfig":
        """Builds the web-shell config from dedicated env names only."""
        environment = _normalized_environment(env_config("UPPI_WEB_ENV", default="local"))
        return cls(
            app_name=env_config("UPPI_WEB_APP_NAME", default="UPPI API").strip() or "UPPI API",
            app_version=env_config("UPPI_WEB_APP_VERSION", default="0.1.0").strip() or "0.1.0",
            environment=environment,
            debug=_parse_bool(env_config("UPPI_WEB_DEBUG", default="False"), default=False),
            auth=WebAuthConfig.from_env(environment=environment),
            session=WebSessionConfig.from_env(environment=environment),
        )
