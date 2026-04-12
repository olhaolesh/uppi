"""Launcher for the internal import-only spider used by service-mode workflows."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, ContextManager

import yaml

from uppi.config.app_config import project_root
from uppi.domain.exceptions import ImportOnlyRunnerFailedError


def _default_temp_dir_factory() -> ContextManager[str]:
    """Provides a temporary workspace for the transitional import source file."""
    return tempfile.TemporaryDirectory(prefix="uppi-import-only-")


class ScrapyImportOnlyRunner:
    """Run the internal import-only spider for exactly one CF."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        python_executable: str | None = None,
        subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        temp_dir_factory: Callable[[], ContextManager[str]] = _default_temp_dir_factory,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.python_executable = python_executable or sys.executable
        self.subprocess_runner = subprocess_runner
        self.temp_dir_factory = temp_dir_factory

    def run_for_cf(self, locatore_cf: str, *, force_update_visura: bool) -> None:
        """Reuse the protected browser/import path through the internal spider."""
        normalized_cf = str(locatore_cf or "").strip().upper()
        command = [self.python_executable, "-m", "scrapy", "crawl", "uppi_import"]

        try:
            with self.temp_dir_factory() as temp_dir:
                clients_path = Path(temp_dir) / "import-clients.yml"
                clients_path.write_text(
                    yaml.safe_dump(
                        [
                            {
                                "LOCATORE_CF": normalized_cf,
                                "FORCE_UPDATE_VISURA": bool(force_update_visura),
                            }
                        ],
                        sort_keys=False,
                        allow_unicode=True,
                    ),
                    encoding="utf-8",
                )

                env = os.environ.copy()
                env["UPPI_CLIENTS_YAML"] = str(clients_path)

                result = self.subprocess_runner(
                    command,
                    cwd=str(self.repo_root),
                    env=env,
                    text=True,
                    capture_output=True,
                )
        except ImportOnlyRunnerFailedError:
            raise
        except Exception as exc:
            raise ImportOnlyRunnerFailedError(
                f"Failed to launch import-only runner for LOCATORE_CF={normalized_cf}.",
                details={
                    "locatore_cf": normalized_cf,
                    "command": command,
                },
            ) from exc

        if result.returncode != 0:
            raise ImportOnlyRunnerFailedError(
                f"Import-only runner failed for LOCATORE_CF={normalized_cf}.",
                details={
                    "locatore_cf": normalized_cf,
                    "command": command,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            )
