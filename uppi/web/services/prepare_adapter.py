"""Adapter seam between the Stage 3 web API and the current prepare owner path."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from uppi.config.app_config import project_root
from uppi.config.immobili import ImmobiliDocumentConfig


class PrepareServiceResultProtocol(Protocol):
    """Minimal shape expected from the current prepare-by-CF service result."""

    locatore_cf: str
    output_path: Path
    decision: str
    db_hit_before_import: bool
    import_performed: bool


class PrepareServiceProtocol(Protocol):
    """Minimal callable surface required from the current prepare owner service."""

    def prepare(
        self,
        locatore_cf: str,
        *,
        force_update_visura: bool = False,
        output_path: str | Path | None = None,
    ) -> PrepareServiceResultProtocol:
        """Runs the current prepare flow and returns its typed result."""


def _default_document_loader(path: Path) -> ImmobiliDocumentConfig:
    """Loads the prepared canonical single-client YAML document."""
    from uppi.domain.immobili_document import load_immobili_document

    return load_immobili_document(path=path)


@dataclass(frozen=True)
class PreparedSearchResult:
    """Typed web-facing bundle after current prepare completed successfully."""

    locatore_cf: str
    output_path: Path
    output_path_relative: str
    decision: str
    db_hit_before_import: bool
    import_performed: bool
    source: str
    document: ImmobiliDocumentConfig
    messages: tuple[str, ...] = ()


class PrepareSearchAdapter:
    """Thin adapter that delegates all prepare decisions to the current owner path."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        prepare_service_factory: Callable[[], PrepareServiceProtocol] | None = None,
        document_loader: Callable[[Path], ImmobiliDocumentConfig] = _default_document_loader,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.prepare_service_factory = prepare_service_factory
        self.document_loader = document_loader

    def prepare_search(
        self,
        locatore_cf: str,
        *,
        force_update_visura: bool = False,
    ) -> PreparedSearchResult:
        """Runs current prepare-by-CF and loads the prepared YAML for response mapping."""
        normalized_cf = str(locatore_cf or "").strip().upper()
        output_path = self._build_output_path(normalized_cf)
        prepare_service = self._build_prepare_service()
        prepare_result = prepare_service.prepare(
            normalized_cf,
            force_update_visura=bool(force_update_visura),
            output_path=output_path,
        )
        resolved_output_path = Path(prepare_result.output_path)
        document = self.document_loader(resolved_output_path)
        return PreparedSearchResult(
            locatore_cf=prepare_result.locatore_cf,
            output_path=resolved_output_path,
            output_path_relative=self._to_relative_path(resolved_output_path),
            decision=prepare_result.decision,
            db_hit_before_import=prepare_result.db_hit_before_import,
            import_performed=prepare_result.import_performed,
            source=self._resolve_source(import_performed=prepare_result.import_performed),
            document=document,
        )

    def _build_prepare_service(self) -> PrepareServiceProtocol:
        """Constructs the current prepare owner service lazily to avoid app-import coupling."""
        if self.prepare_service_factory is not None:
            return self.prepare_service_factory()

        from uppi.services.prepare_by_cf import PrepareByCfService

        return PrepareByCfService(repo_root=self.repo_root)

    def _build_output_path(self, locatore_cf: str) -> Path:
        """Returns the deterministic web-specific YAML output path."""
        return self.repo_root / "clients" / "web_prepare" / locatore_cf / "immobili.yml"

    def _to_relative_path(self, path: Path) -> str:
        """Returns repo-relative path when possible for a frontend-friendly response."""
        try:
            return path.relative_to(self.repo_root).as_posix()
        except ValueError:
            return str(path)

    @staticmethod
    def _resolve_source(*, import_performed: Any) -> str:
        """Maps the current prepare result into a conservative web-facing source label."""
        if import_performed is True:
            return "sister"
        if import_performed is False:
            return "db"
        return "unknown"
