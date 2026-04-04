"""Characterization tests для protected `state.json` lifecycle contract."""

from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

import uppi.settings as settings_module
import uppi.spiders.uppi_spider as spider_module
from uppi.ae.auth import authenticate_user
from uppi.ae.uppi_selectors import UppiSelectors
from uppi.config.workspace import (
    bind_existing_state_json_storage_state,
    delete_state_json_if_present,
    get_state_json_path,
    save_state_json_snapshot,
)


class RecordingLogger:
    """Збирає lifecycle-логи для assert-перевірок без побічних ефектів."""

    def __init__(self) -> None:
        """Ініціалізує порожній записувач логів."""
        self.records: list[tuple[str, str]] = []

    def _record(self, level: str, message: str, *args) -> None:
        """Додає відформатований запис у локальний список."""
        if args:
            message = message % args
        self.records.append((level, message))

    def debug(self, message: str, *args) -> None:
        """Записує debug-повідомлення."""
        self._record("debug", message, *args)

    def info(self, message: str, *args) -> None:
        """Записує info-повідомлення."""
        self._record("info", message, *args)

    def warning(self, message: str, *args) -> None:
        """Записує warning-повідомлення."""
        self._record("warning", message, *args)

    def error(self, message: str, *args) -> None:
        """Записує error-повідомлення."""
        self._record("error", message, *args)

    def exception(self, message: str, *args) -> None:
        """Записує exception-повідомлення як formatted message."""
        self._record("exception", message, *args)


class RecordingBrowserContext:
    """Фіксує виклик `storage_state(...)` для wrapper characterization."""

    def __init__(self) -> None:
        """Ініціалізує порожній записувач викликів."""
        self.storage_state_paths: list[str] = []

    async def storage_state(self, *, path: str) -> None:
        """Запам'ятовує path, з яким було викликано збереження state."""
        self.storage_state_paths.append(path)


class AuthFailurePage:
    """Мінімальний Playwright double для фейлу на PROFILE_INFO."""

    def __init__(self) -> None:
        """Ініціалізує тестовий double для login-flow characterization."""
        self.filled: dict[str, str] = {}
        self.clicked: list[str] = []

    async def wait_for_selector(self, selector: str, timeout: int | None = None) -> None:
        """Емітує успішні waits до кроку перевірки PROFILE_INFO."""
        if selector == UppiSelectors.PROFILE_INFO:
            raise PlaywrightTimeoutError("PROFILE_INFO missing")

    async def click(self, selector: str) -> None:
        """Запам'ятовує selector кліку без зміни інших side effects."""
        self.clicked.append(selector)

    async def wait_for_timeout(self, timeout: int) -> None:
        """Імітує стабілізаційний wait без поведінкових змін."""
        return None

    async def fill(self, selector: str, value: str) -> None:
        """Запам'ятовує заповнені значення для тестового double."""
        self.filled[selector] = value


async def _consume(async_iterable) -> list:
    """Повністю вичитує async iterable для deterministic asserts."""
    return [item async for item in async_iterable]


def test_bind_existing_state_json_storage_state_preserves_current_load_contract(monkeypatch, tmp_path):
    """Перевіряє, що bind helper не переносить і не розширює semantics reuse."""
    monkeypatch.chdir(tmp_path)
    contexts = {"default": {}}

    assert bind_existing_state_json_storage_state(contexts) is False
    assert "storage_state" not in contexts["default"]

    Path("state.json").write_text("{}", encoding="utf-8")

    assert bind_existing_state_json_storage_state(contexts) is True
    assert contexts["default"]["storage_state"] == "state.json"


def test_delete_state_json_if_present_uses_metadata_only_logging_and_current_relative_path(monkeypatch, tmp_path):
    """Перевіряє, що cleanup wrapper зберігає поточний delete contract."""
    monkeypatch.chdir(tmp_path)
    logger = RecordingLogger()
    Path("state.json").write_text("{\"session\":\"secret\"}", encoding="utf-8")

    assert delete_state_json_if_present(logger=logger, reason="fresh_session_start") is True
    assert not get_state_json_path().exists()
    assert delete_state_json_if_present(logger=logger, reason="fresh_session_start") is False

    messages = [message for _, message in logger.records]
    assert any("state.json deleted" in message for message in messages)
    assert any("cleanup skipped" in message for message in messages)
    assert all("secret" not in message for message in messages)


def test_save_state_json_snapshot_preserves_direct_sister_save_path(monkeypatch, tmp_path):
    """Перевіряє, що save wrapper пише рівно в current `state.json` path."""
    monkeypatch.chdir(tmp_path)
    logger = RecordingLogger()
    context = RecordingBrowserContext()

    saved_path = asyncio.run(
        save_state_json_snapshot(
            context,
            logger=logger,
            reason="direct_sister_transition",
        )
    )

    assert saved_path == Path("state.json")
    assert context.storage_state_paths == ["state.json"]
    assert logger.records == [
        ("info", "[STATE] state.json saved (reason=direct_sister_transition, path=state.json)")
    ]


def test_settings_module_uses_state_json_only_when_present(monkeypatch, tmp_path):
    """Фіксує current settings-level load point для direct SISTER transition."""
    monkeypatch.chdir(tmp_path)

    reloaded = importlib.reload(settings_module)
    assert "storage_state" not in reloaded.PLAYWRIGHT_CONTEXTS["default"]

    Path("state.json").write_text("{}", encoding="utf-8")

    reloaded = importlib.reload(settings_module)
    assert reloaded.PLAYWRIGHT_CONTEXTS["default"]["storage_state"] == "state.json"

    Path("state.json").unlink()
    reloaded = importlib.reload(settings_module)
    assert "storage_state" not in reloaded.PLAYWRIGHT_CONTEXTS["default"]


def test_authenticate_user_failed_login_deletes_invalid_state_json(monkeypatch, tmp_path):
    """Фіксує current invalidation path: failed login triggers cleanup of current state."""
    monkeypatch.chdir(tmp_path)
    Path("state.json").write_text("{}", encoding="utf-8")
    logger = RecordingLogger()

    login_ok = asyncio.run(
        authenticate_user(
            page=AuthFailurePage(),
            ae_username="user",
            ae_password="pass",
            ae_pin="1234",
            logger=logger,
        )
    )

    assert login_ok is False
    assert not Path("state.json").exists()
    assert any("state.json deleted" in message for _, message in logger.records)


def test_spider_start_removes_stale_state_json_before_loading_clients(monkeypatch, tmp_path):
    """Фіксує current fresh-session contract: stale state чиститься до load_clients."""
    monkeypatch.chdir(tmp_path)
    Path("state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "captcha_images").mkdir()
    seen: dict[str, bool] = {}

    def fake_load_clients():
        """Перевіряє, що stale state уже видалений до читання input."""
        seen["state_exists_before_load"] = Path("state.json").exists()
        return []

    monkeypatch.setattr(spider_module, "load_clients", fake_load_clients)

    spider = spider_module.UppiSpider()
    asyncio.run(_consume(spider.start()))

    assert seen["state_exists_before_load"] is False
    assert not Path("state.json").exists()
