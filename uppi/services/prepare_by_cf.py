"""Prepare-by-CF orchestration that owns fetch/update decisions for one client."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from uppi.config.app_config import AppConfig, project_root
from uppi.domain.db import get_pg_connection
from uppi.domain.exceptions import (
    ImmobiliDocumentNotFoundError,
    PrepareGenerationFailedError,
    PrepareImportFailedError,
    PrepareInputError,
    PrepareNoDataError,
    PrepareOutputWriteError,
)
from uppi.services.immobili_yaml_generator import ImmobiliYamlGeneratorService
from uppi.services.import_only_runner import ScrapyImportOnlyRunner
from uppi.services.repositories.prepare_document_repo import (
    PrepareDocumentPresence,
    db_load_prepare_document_presence,
)


PREPARE_DECISION_DB_HIT = "db_hit_no_force"
PREPARE_DECISION_DB_MISS = "db_miss_imported"
PREPARE_DECISION_FORCE_REFRESH = "force_refresh_imported"


@dataclass(frozen=True)
class PrepareByCfResult:
    """Operator-facing result for one completed prepare run."""

    locatore_cf: str
    output_path: Path
    decision: str
    db_hit_before_import: bool
    import_performed: bool


class PrepareByCfService:
    """Owns the fetch/update decision tree and stops after writing `immobili.yml`."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], Any] = get_pg_connection,
        presence_loader: Callable[[Any, str], PrepareDocumentPresence] = db_load_prepare_document_presence,
        import_runner: ScrapyImportOnlyRunner | None = None,
        yaml_generator: ImmobiliYamlGeneratorService | None = None,
        app_config_loader: Callable[[], AppConfig] = AppConfig.from_env,
        repo_root: Path | None = None,
    ) -> None:
        self.connection_factory = connection_factory
        self.presence_loader = presence_loader
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.import_runner = import_runner or ScrapyImportOnlyRunner(repo_root=self.repo_root)
        self.yaml_generator = yaml_generator or ImmobiliYamlGeneratorService(connection_factory=connection_factory)
        self.app_config_loader = app_config_loader

    def prepare(
        self,
        locatore_cf: str,
        *,
        force_update_visura: bool = False,
        output_path: str | Path | None = None,
    ) -> PrepareByCfResult:
        """Run the prepare decision tree and produce one ready `immobili.yml` file."""
        normalized_cf = _normalize_locatore_cf(locatore_cf)
        resolved_output_path = self._resolve_output_path(output_path)
        initial_presence = self._load_presence(normalized_cf)

        if force_update_visura:
            decision = PREPARE_DECISION_FORCE_REFRESH
            import_performed = True
        elif initial_presence.is_hit:
            decision = PREPARE_DECISION_DB_HIT
            import_performed = False
        else:
            decision = PREPARE_DECISION_DB_MISS
            import_performed = True

        if import_performed:
            # Prepare owns the fetch/update decision logic, so once it decides to
            # enter the import path it requests a forced refresh for that one CF.
            self._run_import_only_path(normalized_cf)
            post_import_presence = self._load_presence(normalized_cf)
            if not post_import_presence.is_hit:
                raise PrepareNoDataError(
                    f"Prepare import finished but no DB-backed immobili were found for LOCATORE_CF={normalized_cf}.",
                    details={
                        "locatore_cf": normalized_cf,
                        "root_found": post_import_presence.root_found,
                        "immobili_count": post_import_presence.immobili_count,
                        "decision": decision,
                    },
                )

        try:
            written_path = self.yaml_generator.write_yaml(normalized_cf, resolved_output_path)
        except ImmobiliDocumentNotFoundError as exc:
            raise PrepareNoDataError(
                f"Prepare could not build `immobili.yml` for LOCATORE_CF={normalized_cf}.",
                details={
                    "locatore_cf": normalized_cf,
                    "decision": decision,
                    "output_path": str(resolved_output_path),
                },
            ) from exc
        except OSError as exc:
            raise PrepareOutputWriteError(
                f"Prepare could not write `immobili.yml` to {resolved_output_path}.",
                details={
                    "locatore_cf": normalized_cf,
                    "decision": decision,
                    "output_path": str(resolved_output_path),
                },
            ) from exc
        except Exception as exc:
            raise PrepareGenerationFailedError(
                f"Prepare failed while generating `immobili.yml` for LOCATORE_CF={normalized_cf}.",
                details={
                    "locatore_cf": normalized_cf,
                    "decision": decision,
                    "output_path": str(resolved_output_path),
                },
            ) from exc

        return PrepareByCfResult(
            locatore_cf=normalized_cf,
            output_path=written_path,
            decision=decision,
            db_hit_before_import=initial_presence.is_hit,
            import_performed=import_performed,
        )

    def _load_presence(self, locatore_cf: str) -> PrepareDocumentPresence:
        """Reads the explicit DB hit/miss criterion for prepare decisions."""
        conn = self.connection_factory()
        try:
            return self.presence_loader(conn, locatore_cf)
        finally:
            conn.close()

    def _run_import_only_path(self, locatore_cf: str) -> None:
        """Enter the import-only path and propagate explicit prepare-level failures."""
        try:
            self.import_runner.run_for_cf(locatore_cf, force_update_visura=True)
        except PrepareImportFailedError:
            raise
        except Exception as exc:
            raise PrepareImportFailedError(
                f"Prepare could not refresh visura for LOCATORE_CF={locatore_cf}.",
                details={"locatore_cf": locatore_cf},
            ) from exc

    def _resolve_output_path(self, output_path: str | Path | None) -> Path:
        """Resolve the final output path without reading `immobili.yml` as input."""
        if output_path is None:
            return self.app_config_loader().immobili.immobili_file

        resolved_path = Path(output_path).expanduser()
        if not resolved_path.is_absolute():
            resolved_path = self.repo_root / resolved_path
        return resolved_path


def _normalize_locatore_cf(value: str) -> str:
    """Validate and normalize one prepare-by-CF input value."""
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise PrepareInputError("`--cf` is required and cannot be empty.")
    if len(normalized) != 16 or not normalized.isalnum():
        raise PrepareInputError(
            f"`--cf` must be a 16-character alphanumeric Codice Fiscale, got {normalized!r}.",
            details={"locatore_cf": normalized},
        )
    return normalized
