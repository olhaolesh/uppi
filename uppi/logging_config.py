"""Централізована конфігурація логування, formatters і redaction foundation проєкту."""

from __future__ import annotations

import logging
import os
import re
from logging import Formatter, Handler, Logger, StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable


DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE_NAME = "uppi.log"
LOG_ROTATION_MAX_BYTES = 200 * 1024 * 1024
LOG_ROTATION_BACKUP_COUNT = 5

_CONSOLE_HANDLER_NAME = "uppi_console"
_FILE_HANDLER_NAME = "uppi_rotating_file"
_REDACTION_FILTER_NAME = "uppi_redaction_filter"
_LOGGER_CONFIGURED_ATTR = "_uppi_logging_configured"
_LOGGER_FILE_PATH_ATTR = "_uppi_log_file_path"
_LOG_RECORD_RESERVED_FIELDS = set(logging.makeLogRecord({}).__dict__.keys())

_SECRET_PLACEHOLDER = "<secret:redacted>"
_TOKEN_PLACEHOLDER = "<token:redacted>"
_SESSION_PLACEHOLDER = "<session:redacted>"
_CAPTCHA_PLACEHOLDER = "<captcha:redacted>"
_CF_PLACEHOLDER = "<cf:redacted>"

_PASSWORD_KEYS = ("password", "passwd", "pwd", "pin")
_TOKEN_KEYS = ("token", "api_key", "apikey", "secret", "secret_key", "access_key", "authorization")
_SESSION_KEYS = ("session", "sessionid", "cookie", "cookies", "set-cookie", "storage_state", "storage-state")
_CAPTCHA_KEYS = ("captcha", "captcha_code", "captcha_solution", "two_captcha_key", "2captcha")
_CF_KEYS = ("locatore_cf", "conduttore_cf", "codice_fiscale", "codice fiscale", "cf")

_CODICE_FISCALE_RE = re.compile(
    r"\b[A-Z]{6}[0-9LMNPQRSTUV]{2}[A-EHLMPR-T][0-9LMNPQRSTUV]{2}[A-Z][0-9LMNPQRSTUV]{3}[A-Z]\b",
    re.IGNORECASE,
)


def _repo_root() -> Path:
    """Повертає корінь репозиторію для побудови локальних шляхів."""
    return Path(__file__).resolve().parents[1]


def default_log_dir() -> Path:
    """
    Повертає дефолтний каталог логів проекту.

    На цьому етапі не вводимо workspace abstraction і не змінюємо lifecycle
    інших локальних артефактів; лог лежить у простому локальному каталозі
    `logs/` у корені репозиторію.
    """
    override = os.getenv("UPPI_LOG_DIR", "").strip()
    if override:
        return Path(override).expanduser()
    return _repo_root() / "logs"


def default_log_file_path() -> Path:
    """Повертає шлях до стандартного лог-файлу проєкту."""
    override = os.getenv("UPPI_LOG_FILE", "").strip()
    if override:
        return Path(override).expanduser()
    return default_log_dir() / DEFAULT_LOG_FILE_NAME


def _resolve_log_level(level: str | int | None) -> int:
    """Нормалізує textual або numeric log level у значення logging."""
    if isinstance(level, int):
        return level
    raw = str(level or os.getenv("UPPI_LOG_LEVEL", DEFAULT_LOG_LEVEL)).strip().upper()
    return getattr(logging, raw, logging.INFO)


def _compile_quoted_pattern(keys: tuple[str, ...]) -> re.Pattern[str]:
    """Компілює regex для секретів у quoted key/value-парах."""
    joined = "|".join(re.escape(key) for key in keys)
    return re.compile(
        rf'(?i)(?<!<)(["\']?(?:{joined})["\']?\s*[:=]\s*)(["\'])(.*?)(\2)',
    )


def _compile_unquoted_pattern(keys: tuple[str, ...]) -> re.Pattern[str]:
    """Компілює regex для секретів у неquoted key/value-парах."""
    joined = "|".join(re.escape(key) for key in keys)
    return re.compile(
        rf'(?i)(?<!<)(["\']?(?:{joined})["\']?\s*[:=]\s*)([^,\s;\]\}}]+)',
    )


_KEY_VALUE_PATTERNS = (
    (_compile_quoted_pattern(_PASSWORD_KEYS), _SECRET_PLACEHOLDER),
    (_compile_unquoted_pattern(_PASSWORD_KEYS), _SECRET_PLACEHOLDER),
    (_compile_quoted_pattern(_TOKEN_KEYS), _TOKEN_PLACEHOLDER),
    (_compile_unquoted_pattern(_TOKEN_KEYS), _TOKEN_PLACEHOLDER),
    (_compile_quoted_pattern(_SESSION_KEYS), _SESSION_PLACEHOLDER),
    (_compile_unquoted_pattern(_SESSION_KEYS), _SESSION_PLACEHOLDER),
    (_compile_quoted_pattern(_CAPTCHA_KEYS), _CAPTCHA_PLACEHOLDER),
    (_compile_unquoted_pattern(_CAPTCHA_KEYS), _CAPTCHA_PLACEHOLDER),
    (_compile_quoted_pattern(_CF_KEYS), _CF_PLACEHOLDER),
    (_compile_unquoted_pattern(_CF_KEYS), _CF_PLACEHOLDER),
)


def sanitize_log_text(text: str) -> str:
    """Санітизує текст логу, маскуючи секрети, session-data і PII."""
    if not text:
        return text

    sanitized = str(text)

    sanitized = re.sub(
        r"(?i)(\bauthorization\b\s*[:=]\s*bearer\s+)([^\s,;]+)",
        rf"\1{_TOKEN_PLACEHOLDER}",
        sanitized,
    )
    sanitized = re.sub(
        r"(?i)([?&]captcha=)([^&\s]+)",
        rf"\1{_CAPTCHA_PLACEHOLDER}",
        sanitized,
    )

    for pattern, placeholder in _KEY_VALUE_PATTERNS:
        sanitized = pattern.sub(rf"\1{placeholder}", sanitized)

    sanitized = re.sub(
        r"(?i)(\bbearer\s+)([A-Za-z0-9._~+/=-]+)",
        rf"\1{_TOKEN_PLACEHOLDER}",
        sanitized,
    )
    sanitized = re.sub(
        r"(?i)([?&](?:token|session|sessionid|authorization|api_key|captcha)=)([^&\s]+)",
        rf"\1{_TOKEN_PLACEHOLDER}",
        sanitized,
    )
    sanitized = _CODICE_FISCALE_RE.sub(_CF_PLACEHOLDER, sanitized)
    return sanitized


def sanitize_log_value(value):
    """Рекурсивно санітизує колекції та окремі значення для логування."""
    if isinstance(value, str):
        return sanitize_log_text(value)
    if isinstance(value, dict):
        return {k: sanitize_log_value(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return tuple(sanitize_log_value(v) for v in value)
    if isinstance(value, list):
        return [sanitize_log_value(v) for v in value]
    if isinstance(value, set):
        return {sanitize_log_value(v) for v in value}
    return value


class SensitiveDataFilter(logging.Filter):
    """Фільтр логування, що застосовує redaction до record payload."""
    def __init__(self) -> None:
        """Створює фільтр із canonical ідентифікатором redaction-policy."""
        super().__init__()
        self.name = _REDACTION_FILTER_NAME

    def filter(self, record: logging.LogRecord) -> bool:
        """Санітизує msg, args і custom fields перед форматуванням запису."""
        if record.args:
            record.args = sanitize_log_value(record.args)
        else:
            record.msg = sanitize_log_value(record.msg)

        for key, value in list(record.__dict__.items()):
            if key in _LOG_RECORD_RESERVED_FIELDS:
                continue
            record.__dict__[key] = sanitize_log_value(value)

        return True


class SanitizingFormatter(logging.Formatter):
    """Formatter, який додатково санітизує вже сформований текст логу."""
    def format(self, record: logging.LogRecord) -> str:
        """Повертає відформатований і санітизований рядок логу."""
        return sanitize_log_text(super().format(record))


def _build_formatter() -> Formatter:
    """Створює стандартний formatter для console і file handler-ів."""
    return SanitizingFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _iter_uppi_handlers(logger: Logger) -> Iterable[Handler]:
    """Повертає лише handler-и, які належать centralized logging foundation."""
    for handler in logger.handlers:
        if getattr(handler, "name", "") in {_CONSOLE_HANDLER_NAME, _FILE_HANDLER_NAME}:
            yield handler


def _has_handler(logger: Logger, name: str) -> bool:
    """Перевіряє, чи logger уже містить handler з указаним іменем."""
    return any(getattr(handler, "name", "") == name for handler in logger.handlers)


def _has_redaction_filter(handler: Handler) -> bool:
    """Перевіряє, чи handler уже має redaction filter."""
    return any(getattr(filter_obj, "name", "") == _REDACTION_FILTER_NAME for filter_obj in handler.filters)


def _ensure_redaction_filter(handler: Handler) -> None:
    """Гарантує підключення redaction filter до handler-а."""
    if not _has_redaction_filter(handler):
        handler.addFilter(SensitiveDataFilter())


def _create_console_handler(formatter: Formatter) -> StreamHandler:
    """Створює console handler із formatter-ом і redaction filter."""
    handler = StreamHandler()
    handler.name = _CONSOLE_HANDLER_NAME
    handler.setFormatter(formatter)
    _ensure_redaction_filter(handler)
    return handler


def _create_rotating_file_handler(log_file_path: Path, formatter: Formatter) -> RotatingFileHandler:
    """Створює rotating file handler з потрібними межами ротації."""
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_file_path,
        maxBytes=LOG_ROTATION_MAX_BYTES,
        backupCount=LOG_ROTATION_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.name = _FILE_HANDLER_NAME
    handler.setFormatter(formatter)
    _ensure_redaction_filter(handler)
    return handler


def configure_uppi_logging(
    *,
    logger_name: str = "uppi",
    level: str | int | None = None,
    log_file_path: str | Path | None = None,
) -> Logger:
    """
    Централізована ініціалізація логування для логерів `uppi*`.

    Що робить:
    - додає console handler;
    - додає rotating file handler з rotation 200 MB;
    - не дублює власні handlers при повторній ініціалізації;
    - лишає конфігурацію достатньо простою для наступного кроку з redaction.
    """
    logger = logging.getLogger(logger_name)
    resolved_level = _resolve_log_level(level)
    resolved_file_path = Path(log_file_path).expanduser() if log_file_path else default_log_file_path()
    formatter = _build_formatter()

    logger.setLevel(resolved_level)
    logger.propagate = False

    configured_file_path = getattr(logger, _LOGGER_FILE_PATH_ATTR, None)
    already_configured = bool(getattr(logger, _LOGGER_CONFIGURED_ATTR, False))
    if already_configured and configured_file_path == str(resolved_file_path):
        for handler in _iter_uppi_handlers(logger):
            handler.setLevel(resolved_level)
            handler.setFormatter(formatter)
            _ensure_redaction_filter(handler)
        return logger

    if already_configured:
        for handler in list(_iter_uppi_handlers(logger)):
            logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    if not _has_handler(logger, _CONSOLE_HANDLER_NAME):
        console_handler = _create_console_handler(formatter)
        console_handler.setLevel(resolved_level)
        logger.addHandler(console_handler)

    if not _has_handler(logger, _FILE_HANDLER_NAME):
        try:
            file_handler = _create_rotating_file_handler(resolved_file_path, formatter)
        except OSError as exc:
            logger.warning("[LOGGING] Cannot initialize file handler at %s: %s", resolved_file_path, exc)
        else:
            file_handler.setLevel(resolved_level)
            logger.addHandler(file_handler)

    setattr(logger, _LOGGER_CONFIGURED_ATTR, True)
    setattr(logger, _LOGGER_FILE_PATH_ATTR, str(resolved_file_path))
    return logger
