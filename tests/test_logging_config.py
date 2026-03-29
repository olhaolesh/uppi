"""Тести для centralized logging config і redaction foundation."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from uppi.logging_config import (
    LOG_ROTATION_MAX_BYTES,
    SensitiveDataFilter,
    configure_uppi_logging,
    default_log_file_path,
    sanitize_log_text,
)


def _cleanup_logger(name: str) -> None:
    """Прибирає handlers і filters після тесту, щоб тести не впливали один на одного."""
    logger = logging.getLogger(name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    for attr in ("_uppi_logging_configured", "_uppi_log_file_path"):
        if hasattr(logger, attr):
            delattr(logger, attr)


def test_configure_uppi_logging_creates_console_and_rotating_file_handlers(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    logger_name = "uppi.test.logging.handlers"
    _cleanup_logger(logger_name)

    log_file_path = tmp_path / "logs" / "uppi.log"
    logger = configure_uppi_logging(logger_name=logger_name, log_file_path=log_file_path, level="DEBUG")

    console_handlers = [
        handler
        for handler in logger.handlers
        if getattr(handler, "name", "") == "uppi_console"
    ]
    file_handlers = [
        handler
        for handler in logger.handlers
        if getattr(handler, "name", "") == "uppi_rotating_file"
    ]

    assert logger.level == logging.DEBUG
    assert logger.propagate is False
    assert len(console_handlers) == 1
    assert len(file_handlers) == 1
    assert isinstance(file_handlers[0], RotatingFileHandler)
    assert file_handlers[0].maxBytes == LOG_ROTATION_MAX_BYTES
    assert log_file_path.exists()
    assert any(isinstance(filter_obj, SensitiveDataFilter) for filter_obj in console_handlers[0].filters)
    assert any(isinstance(filter_obj, SensitiveDataFilter) for filter_obj in file_handlers[0].filters)


def test_configure_uppi_logging_reinitialization_does_not_duplicate_handlers(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    logger_name = "uppi.test.logging.idempotent"
    _cleanup_logger(logger_name)

    log_file_path = tmp_path / "logs" / "uppi.log"
    logger = configure_uppi_logging(logger_name=logger_name, log_file_path=log_file_path, level="INFO")
    logger = configure_uppi_logging(logger_name=logger_name, log_file_path=log_file_path, level="WARNING")

    console_handlers = [
        handler
        for handler in logger.handlers
        if getattr(handler, "name", "") == "uppi_console"
    ]
    file_handlers = [
        handler
        for handler in logger.handlers
        if getattr(handler, "name", "") == "uppi_rotating_file"
    ]

    assert len(console_handlers) == 1
    assert len(file_handlers) == 1
    assert logger.level == logging.WARNING
    assert console_handlers[0].level == logging.WARNING
    assert file_handlers[0].level == logging.WARNING
    assert sum(isinstance(filter_obj, SensitiveDataFilter) for filter_obj in console_handlers[0].filters) == 1
    assert sum(isinstance(filter_obj, SensitiveDataFilter) for filter_obj in file_handlers[0].filters) == 1


def test_configure_uppi_logging_default_log_path_uses_env_override_directory(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    logger_name = "uppi.test.logging.default_path"
    _cleanup_logger(logger_name)

    log_dir = tmp_path / "custom-logs"
    monkeypatch.setenv("UPPI_LOG_DIR", str(log_dir))
    monkeypatch.delenv("UPPI_LOG_FILE", raising=False)

    expected_log_file = log_dir / "uppi.log"
    assert default_log_file_path() == expected_log_file

    logger = configure_uppi_logging(logger_name=logger_name)
    logger.info("hello from test")

    for handler in logger.handlers:
        handler.flush()

    assert expected_log_file.exists()
    assert "hello from test" in expected_log_file.read_text(encoding="utf-8")


def test_sanitize_log_text_redacts_secret_token_session_captcha_and_cf_values():
    """Перевіряє сценарій, описаний у назві тесту."""
    raw = (
        "password=supersecret "
        "pin='1234' "
        "token=abc123 "
        "authorization=Bearer very-secret-token "
        "cookie=sessionid=xyz; foo=bar "
        "captcha_solution=ABCD "
        "locatore_cf=RSSMRA80A01H501Z "
        "payload={'password': 'topsecret', 'conduttore_cf': 'BNCMRA80A01H501Z'} "
        "url=https://example.test/?token=qwerty"
    )

    sanitized = sanitize_log_text(raw)

    assert "supersecret" not in sanitized
    assert "1234" not in sanitized
    assert "abc123" not in sanitized
    assert "very-secret-token" not in sanitized
    assert "sessionid=xyz" not in sanitized
    assert "ABCD" not in sanitized
    assert "RSSMRA80A01H501Z" not in sanitized
    assert "BNCMRA80A01H501Z" not in sanitized
    assert "<secret:redacted>" in sanitized
    assert "<token:redacted>" in sanitized
    assert "<session:redacted>" in sanitized
    assert "<captcha:redacted>" in sanitized
    assert "<cf:redacted>" in sanitized


def test_sanitize_log_text_keeps_safe_non_sensitive_text_readable():
    """Перевіряє сценарій, описаний у назві тесту."""
    raw = "Loaded 3 clients from clients.yml for comune=PESCARA and category=A/2"

    sanitized = sanitize_log_text(raw)

    assert sanitized == raw


def test_configure_uppi_logging_redacts_logged_message_arguments_and_exception_text(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    logger_name = "uppi.test.logging.redaction_runtime"
    _cleanup_logger(logger_name)

    log_file_path = tmp_path / "logs" / "uppi.log"
    logger = configure_uppi_logging(logger_name=logger_name, log_file_path=log_file_path, level="INFO")

    logger.info(
        "payload=%s token=%s",
        {"password": "supersecret", "locatore_cf": "RSSMRA80A01H501Z"},
        "abc123token",
    )

    try:
        raise RuntimeError("captcha_solution=ABCD sessionid=xyz")
    except RuntimeError:
        logger.exception("operation failed for conduttore_cf=%s", "BNCMRA80A01H501Z")

    for handler in logger.handlers:
        handler.flush()

    contents = log_file_path.read_text(encoding="utf-8")

    assert "supersecret" not in contents
    assert "abc123token" not in contents
    assert "ABCD" not in contents
    assert "sessionid=xyz" not in contents
    assert "RSSMRA80A01H501Z" not in contents
    assert "BNCMRA80A01H501Z" not in contents
    assert "<secret:redacted>" in contents
    assert "<token:redacted>" in contents
    assert "<captcha:redacted>" in contents
    assert "<session:redacted>" in contents
    assert "<cf:redacted>" in contents
