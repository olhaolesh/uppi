"""Unit tests for the prepare-facing import-only launcher."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from uppi.domain.exceptions import ImportOnlyRunnerFailedError
from uppi.services.import_only_runner import ScrapyImportOnlyRunner


class _StaticTempDir:
    """Context manager that keeps a predictable temp directory for inspection."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> str:
        self.path.mkdir(parents=True, exist_ok=True)
        return str(self.path)

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_scrapy_import_only_runner_writes_single_client_source_and_launches_internal_spider(tmp_path):
    """The launcher should reuse the internal import-only spider through a narrow API."""
    captured = {}

    def fake_subprocess_runner(cmd, *, cwd, env, text, capture_output):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        captured["text"] = text
        captured["capture_output"] = capture_output
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    runner = ScrapyImportOnlyRunner(
        repo_root=tmp_path / "repo",
        python_executable="/usr/bin/python-test",
        subprocess_runner=fake_subprocess_runner,
        temp_dir_factory=lambda: _StaticTempDir(tmp_path / "runtime"),
    )

    runner.run_for_cf("rssmra80a01h501z", force_update_visura=True)

    clients_path = Path(captured["env"]["UPPI_CLIENTS_YAML"])
    payload = yaml.safe_load(clients_path.read_text(encoding="utf-8"))

    assert captured["cmd"] == ["/usr/bin/python-test", "-m", "scrapy", "crawl", "uppi_import"]
    assert captured["cwd"] == str(tmp_path / "repo")
    assert captured["text"] is True
    assert captured["capture_output"] is True
    assert payload == [
        {
            "LOCATORE_CF": "RSSMRA80A01H501Z",
            "FORCE_UPDATE_VISURA": True,
        }
    ]


def test_scrapy_import_only_runner_raises_typed_error_on_nonzero_exit(tmp_path):
    """Service modes must receive an explicit import failure when the spider exits non-zero."""
    def fake_subprocess_runner(cmd, *, cwd, env, text, capture_output):
        return subprocess.CompletedProcess(cmd, 1, stdout="bad", stderr="boom")

    runner = ScrapyImportOnlyRunner(
        repo_root=tmp_path,
        subprocess_runner=fake_subprocess_runner,
        temp_dir_factory=lambda: _StaticTempDir(tmp_path / "runtime"),
    )

    with pytest.raises(ImportOnlyRunnerFailedError) as exc_info:
        runner.run_for_cf("RSSMRA80A01H501Z", force_update_visura=True)

    assert exc_info.value.details["returncode"] == 1
    assert exc_info.value.details["stdout"] == "bad"
    assert exc_info.value.details["stderr"] == "boom"
