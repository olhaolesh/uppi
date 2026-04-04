"""Workspace і local-artifacts policy з незмінними default paths на першому проході."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from decouple import config

from uppi.config.app_config import project_root


def _normalize_optional_path(value: str | Path | None, *, base_dir: Path | None = None) -> Path | None:
    """Нормалізує optional path без зайвої магії й без зміни default fallbacks."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


@dataclass(frozen=True)
class WorkspaceConfig:
    """Описує repo-local workspace root і canonical локальні artifact paths."""

    root: Path
    downloads_dir: Path
    captcha_images_dir: Path
    logs_dir: Path
    state_file: Path

    @classmethod
    def from_env(
        cls,
        *,
        repo_root: Path | None = None,
        workspace_root: str | Path | None = None,
    ) -> "WorkspaceConfig":
        """Повертає workspace config з env override або canonical repo-local defaults."""
        resolved_repo_root = repo_root or project_root()
        resolved_root = _normalize_optional_path(workspace_root, base_dir=resolved_repo_root)
        if resolved_root is None:
            resolved_root = _normalize_optional_path(
                config("UPPI_WORKSPACE_ROOT", default=""),
                base_dir=resolved_repo_root,
            )
        resolved_root = resolved_root or resolved_repo_root

        # `state.json` навмисно лишається в current relative location, щоб не
        # переносити create/load/delete semantics у browser-critical flow.
        return cls(
            root=resolved_root,
            downloads_dir=resolved_root / "downloads",
            captcha_images_dir=resolved_root / "captcha_images",
            logs_dir=resolved_root / "logs",
            state_file=Path("state.json"),
        )

    @classmethod
    def default(cls) -> "WorkspaceConfig":
        """Повертає current default workspace config."""
        return cls.from_env()


@dataclass(frozen=True)
class ArtifactCleanupPolicy:
    """Документує current cleanup contract для одного локального artifact family."""

    cleanup_trigger: str
    auto_cleanup: bool
    browser_critical: bool
    notes: str


def get_workspace_config(*, workspace_root: str | Path | None = None) -> WorkspaceConfig:
    """Повертає canonical workspace config для поточного process scope."""
    return WorkspaceConfig.from_env(workspace_root=workspace_root)


def get_downloads_dir(*, workspace_root: str | Path | None = None) -> Path:
    """Повертає current downloads root з optional workspace override."""
    return get_workspace_config(workspace_root=workspace_root).downloads_dir


def get_captcha_images_dir(*, workspace_root: str | Path | None = None) -> Path:
    """Повертає current captcha-images root з optional workspace override."""
    return get_workspace_config(workspace_root=workspace_root).captcha_images_dir


def get_captcha_client_dir(
    client_cf: str | None,
    *,
    workspace_root: str | Path | None = None,
) -> Path:
    """Повертає директорію для локальних CAPTCHA screenshot-ів одного клієнта."""
    safe_client_cf = str(client_cf or "unknown_cf").strip() or "unknown_cf"
    return get_captcha_images_dir(workspace_root=workspace_root) / safe_client_cf


def get_state_json_path() -> Path:
    """Повертає current path для `state.json` без зміни його lifecycle contract."""
    return get_workspace_config().state_file


def bind_existing_state_json_storage_state(playwright_contexts: dict[str, dict[str, object]]) -> bool:
    """Підхоплює current `state.json` у Playwright context без зміни load semantics."""
    state_path = get_state_json_path()
    if not state_path.exists():
        return False
    playwright_contexts.setdefault("default", {})["storage_state"] = str(state_path)
    return True


def delete_state_json_if_present(*, logger: Any | None = None, reason: str) -> bool:
    """Видаляє current `state.json` у вже визначеній lifecycle-точці caller-а."""
    state_path = get_state_json_path()
    if not state_path.exists():
        if logger is not None:
            logger.debug(
                "[STATE] state.json cleanup skipped (reason=%s, present=False, path=%s)",
                reason,
                state_path.name,
            )
        return False

    state_path.unlink()
    if logger is not None:
        logger.info(
            "[STATE] state.json deleted (reason=%s, path=%s)",
            reason,
            state_path.name,
        )
    return True


async def save_state_json_snapshot(
    browser_context: Any,
    *,
    logger: Any | None = None,
    reason: str,
) -> Path:
    """Зберігає `storage_state` у current `state.json` path без переносу ownership."""
    state_path = get_state_json_path()
    await browser_context.storage_state(path=str(state_path))
    if logger is not None:
        logger.info(
            "[STATE] state.json saved (reason=%s, path=%s)",
            reason,
            state_path.name,
        )
    return state_path


def default_artifact_cleanup_policies() -> dict[str, ArtifactCleanupPolicy]:
    """Повертає documented cleanup contract для current local artifacts surface."""
    return {
        "state_json": ArtifactCleanupPolicy(
            cleanup_trigger="fresh_session_start_and_failed_login",
            auto_cleanup=True,
            browser_critical=True,
            notes="Path intentionally stays `state.json`; save/load/delete semantics are protected.",
        ),
        "captcha_images": ArtifactCleanupPolicy(
            cleanup_trigger="fresh_session_start",
            auto_cleanup=True,
            browser_critical=True,
            notes="Old CAPTCHA screenshots are removed at spider start before a new browser session.",
        ),
        "local_visura_pdf": ArtifactCleanupPolicy(
            cleanup_trigger="post_commit_if_delete_local_visura_after_upload",
            auto_cleanup=False,
            browser_critical=False,
            notes="Cleanup exists only after outer commit and only under runtime flag.",
        ),
        "local_attestazione_docx": ArtifactCleanupPolicy(
            cleanup_trigger="none",
            auto_cleanup=False,
            browser_critical=False,
            notes="Current flow does not aggressively clean generated DOCX artifacts.",
        ),
    }
