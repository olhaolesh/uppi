"""Protected Stage 5 adapter for bulk CSV import-only web requests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Protocol
from uuid import uuid4

from uppi.config.app_config import project_root

if TYPE_CHECKING:
    from uppi.services.bulk_import_clients_csv import BulkImportRunResult
    from uppi.web.schemas.clients import ClientsBulkImportRequest


class BulkImportServiceProtocol(Protocol):
    """Minimal bulk import owner surface required by the web adapter."""

    def run(
        self,
        csv_path: str | Path,
        *,
        force_update_visura: bool = False,
        fail_fast: bool = False,
    ) -> "BulkImportRunResult":
        """Runs the current import-only CSV orchestration."""


class BulkImportCsvWriteError(RuntimeError):
    """Raised when the web layer cannot persist its repo-local CSV input."""


class BulkImportNoUsableRowsError(ValueError):
    """Raised when the provided CSV has no valid unique client rows to import."""


@dataclass(frozen=True)
class BulkImportWebRowResult:
    """Frontend-facing per-row result for valid or deduped CSV rows."""

    row_number: int
    locatore_cf: str
    status: str
    message: str


@dataclass(frozen=True)
class BulkImportWebInvalidRow:
    """Frontend-facing invalid-row diagnostic."""

    row_number: int
    code: str | None
    message: str


@dataclass(frozen=True)
class BulkImportWebResult:
    """Structured synchronous result returned by the Stage 5 bulk-import API."""

    status: str
    run_id: str
    clients_csv_path: Path
    clients_csv_path_relative: str
    force_update_visura: bool
    fail_fast: bool
    total_rows: int
    valid_rows: int
    invalid_rows_count: int
    unique_clients: int
    imported_count: int
    failed_count: int
    skipped_count: int
    results: tuple[BulkImportWebRowResult, ...]
    invalid_rows: tuple[BulkImportWebInvalidRow, ...]
    messages: tuple[str, ...] = ()


class BulkImportAdapter:
    """Writes a safe web-run CSV file and delegates to current bulk import owner logic."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        bulk_import_service_factory: Callable[[], BulkImportServiceProtocol] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.bulk_import_service_factory = bulk_import_service_factory
        self.run_id_factory = run_id_factory or (lambda: uuid4().hex)

    def import_clients(
        self,
        payload: "ClientsBulkImportRequest",
        *,
        run_id: str | None = None,
    ) -> BulkImportWebResult:
        """Stores one web-run CSV file and reuses current bulk CSV import-only orchestration."""
        resolved_run_id = str(run_id or self.run_id_factory())
        clients_csv_path = self._build_clients_csv_path(resolved_run_id)
        self._write_clients_csv(clients_csv_path, payload.csv_content)
        service = self._build_bulk_import_service()
        run_result = service.run(
            clients_csv_path,
            force_update_visura=bool(payload.force_update_visura),
            fail_fast=bool(payload.fail_fast),
        )
        if run_result.deduped_cf_count <= 0:
            raise BulkImportNoUsableRowsError(
                "Bulk import CSV does not contain any valid unique LOCATORE_CF rows."
            )

        invalid_rows = tuple(
            BulkImportWebInvalidRow(
                row_number=item.row_number,
                code=item.code,
                message=item.message,
            )
            for item in run_result.item_results
            if item.status == "skipped_invalid"
        )
        results = tuple(
            BulkImportWebRowResult(
                row_number=item.row_number,
                locatore_cf=item.normalized_locatore_cf or item.raw_locatore_cf or "",
                status="skipped_duplicate" if item.status == "skipped_duplicate" else item.status,
                message=item.message,
            )
            for item in run_result.item_results
            if item.status != "skipped_invalid"
        )
        messages: list[str] = []
        if run_result.aborted:
            messages.append(
                "Bulk import aborted after the first import-only failure because fail_fast=true."
            )

        return BulkImportWebResult(
            status="aborted" if run_result.aborted else "completed",
            run_id=resolved_run_id,
            clients_csv_path=clients_csv_path,
            clients_csv_path_relative=self._to_relative_path(clients_csv_path),
            force_update_visura=run_result.force_update_visura,
            fail_fast=run_result.fail_fast,
            total_rows=run_result.total_rows,
            valid_rows=run_result.valid_rows,
            invalid_rows_count=run_result.skipped_invalid,
            unique_clients=run_result.deduped_cf_count,
            imported_count=run_result.imported_successfully,
            failed_count=run_result.failed,
            skipped_count=run_result.skipped_duplicate,
            results=results,
            invalid_rows=invalid_rows,
            messages=tuple(messages),
        )

    def _build_bulk_import_service(self) -> BulkImportServiceProtocol:
        if self.bulk_import_service_factory is not None:
            return self.bulk_import_service_factory()

        from uppi.services.bulk_import_clients_csv import BulkImportClientsCsvService

        return BulkImportClientsCsvService()

    def _build_clients_csv_path(self, run_id: str) -> Path:
        return self.repo_root / "clients" / "web_bulk_import" / run_id / "clients.csv"

    def _write_clients_csv(self, path: Path, csv_content: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(csv_content, encoding="utf-8")
        except Exception as exc:
            raise BulkImportCsvWriteError(
                "Could not persist the web-run clients.csv input."
            ) from exc

    def _to_relative_path(self, path: Path) -> str:
        try:
            return path.relative_to(self.repo_root).as_posix()
        except ValueError:
            return str(path)


__all__ = [
    "BulkImportAdapter",
    "BulkImportCsvWriteError",
    "BulkImportNoUsableRowsError",
    "BulkImportWebResult",
]
