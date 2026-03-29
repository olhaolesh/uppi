"""Тести для safe logging cleanup у ризикових модулях."""

from __future__ import annotations

import asyncio
import logging

import uppi.ae.captcha as captcha_module
from uppi.ae.captcha import _solve_captcha
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.utils.playwright_helpers import log_requests
from tests.test_visura_pdf_parser_baseline import _load_fixture, _patch_parser_io


class _FakeRoute:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.continued = False

    async def continue_(self):
        """Імітує продовження Playwright-route без реального браузера."""
        self.continued = True


class _FakeRequest:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, url: str, method: str = "GET"):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.url = url
        self.method = method


class _RecordingLogger:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, message: str, *args):
        """Зберігає одне повідомлення у тестовий лог для подальших assert-перевірок."""
        if args:
            message = message % args
        self.messages.append((level, message))

    def debug(self, message: str, *args):
        """Записує тестову лог-подію у внутрішню колекцію повідомлень."""
        self._record("debug", message, *args)

    def info(self, message: str, *args):
        """Записує тестову лог-подію у внутрішню колекцію повідомлень."""
        self._record("info", message, *args)

    def warning(self, message: str, *args):
        """Записує тестову лог-подію у внутрішню колекцію повідомлень."""
        self._record("warning", message, *args)

    def error(self, message: str, *args):
        """Записує тестову лог-подію у внутрішню колекцію повідомлень."""
        self._record("error", message, *args)

    def exception(self, message: str, *args):
        """Записує тестову лог-подію у внутрішню колекцію повідомлень."""
        self._record("exception", message, *args)


class _FakeCaptchaLocator:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    async def is_visible(self):
        """Повертає наперед визначений результат видимості тестового локатора."""
        return True

    async def screenshot(self, path: str, type: str):
        """Повертає тестовий байтовий вміст замість реального screenshot."""
        assert type == "png"
        return b"fake-captcha-bytes"


class _FakeCaptchaPage:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def locator(self, _selector: str):
        """Повертає тестовий locator для перевірки captcha-сценарію."""
        return _FakeCaptchaLocator()

    async def wait_for_timeout(self, _ms: int):
        """Імітує очікування без реального browser timing."""
        return None


class _FakeTwoCaptcha:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, _solver_key: str):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        pass

    def normal(self, _captcha_base64: str):
        """Повертає контрольовану відповідь fake CAPTCHA-solver-а."""
        return {"code": "ABCD", "captchaId": "123456"}


def test_playwright_request_logging_uses_metadata_only(caplog):
    """Перевіряє сценарій, описаний у назві тесту."""
    caplog.set_level(logging.DEBUG, logger="uppi.utils.playwright_helpers")
    route = _FakeRoute()
    request = _FakeRequest("https://example.test/private/path?token=supersecret&foo=bar", method="POST")

    asyncio.run(log_requests(route, request))

    assert route.continued is True
    assert "POST" in caplog.text
    assert "example.test/private/path" in caplog.text
    assert "token=supersecret" not in caplog.text
    assert "?token=" not in caplog.text


def test_captcha_solver_logging_does_not_emit_raw_result_or_solution(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setattr(captcha_module, "TwoCaptcha", _FakeTwoCaptcha)
    monkeypatch.setattr(captcha_module.os, "makedirs", lambda *args, **kwargs: None)
    logger = _RecordingLogger()

    code = asyncio.run(
        _solve_captcha(
            playwright_page=_FakeCaptchaPage(),
            solver_key="api-key",
            codice_fiscale="RSSMRA80A01H501Z",
            img_captcha_selector="#captcha",
            logger=logger,
        )
    )

    joined = "\n".join(message for _, message in logger.messages)

    assert code == "ABCD"
    assert "ABCD" not in joined
    assert "{'code': 'ABCD'" not in joined
    assert "captchaId" in joined
    assert "CAPTCHA solved successfully" in joined
    assert "Screenshot captured for solver (bytes=18)" in joined


def test_visura_parser_logging_uses_summary_instead_of_raw_immobili_dump(monkeypatch, caplog):
    """Перевіряє сценарій, описаний у назві тесту."""
    fixture = _load_fixture("happy_path.json")
    _patch_parser_io(monkeypatch, fixture)
    caplog.set_level(logging.DEBUG, logger="uppi.parsers.visura_pdf_parser")

    result = VisuraParser().parse("dummy.pdf")

    assert len(result) == 1
    assert "Summary: count=1" in caplog.text
    assert "sample_fields=" in caplog.text
    assert "All immobili information is" not in caplog.text
    assert "VIA ROMA 10 SCALA A INTERNO 2 P. 3" not in caplog.text
