"""Bulk CSV import-only orchestration for refreshing DB state without generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from uppi.config.clients_csv import BulkClientCsvRow, BulkClientsCsvLoadResult
from uppi.domain.clients_csv import load_clients_csv_with_issues
from uppi.domain.exceptions import BulkImportCsvLoadError, ImportOnlyRunnerFailedError
from uppi.services.import_only_runner import ScrapyImportOnlyRunner


BULK_IMPORT_STATUS_IMPORTED = "imported"
BULK_IMPORT_STATUS_FAILED = "failed"
BULK_IMPORT_STATUS_SKIPPED_INVALID = "skipped_invalid"
BULK_IMPORT_STATUS_SKIPPED_DUPLICATE = "skipped_duplicate"


@dataclass(frozen=True)
class BulkImportItemResult:
    """Per-row result for the bulk import-only execution report."""

    row_number: int
    raw_locatore_cf: str
    normalized_locatore_cf: str | None
    status: str
    message: str


@dataclass(frozen=True)
class BulkImportRunResult:
    """Summary plus per-row details for one bulk CSV import-only run."""

    source_csv: Path
    total_rows: int
    valid_rows: int
    deduped_cf_count: int
    imported_successfully: int
    failed: int
    skipped_invalid: int
    skipped_duplicate: int
    fail_fast: bool
    force_update_visura: bool
    aborted: bool
    item_results: tuple[BulkImportItemResult, ...]

    @property
    def has_failures(self) -> bool:
        """Returns `True` when the run contains any failed or invalid entries."""
        return self.failed > 0 or self.skipped_invalid > 0 or self.aborted


class BulkImportClientsCsvService:
    """Runs the existing import-only boundary for many CFs and stops before generation."""

    def __init__(
        self,
        *,
        csv_loader: Callable[..., BulkClientsCsvLoadResult] | None = None,
        import_runner: ScrapyImportOnlyRunner | None = None,
    ) -> None:
        self.csv_loader = csv_loader or load_clients_csv_with_issues
        self.import_runner = import_runner or ScrapyImportOnlyRunner()

    def run(
        self,
        csv_path: str | Path,
        *,
        force_update_visura: bool = False,
        fail_fast: bool = False,
    ) -> BulkImportRunResult:
        """Load CSV input, normalize/dedupe CFs, and reuse the import-only runner."""
        resolved_csv_path = Path(csv_path).expanduser()
        load_result = self._load_csv(resolved_csv_path)

        item_results: list[BulkImportItemResult] = []
        skipped_invalid = 0
        skipped_duplicate = 0
        valid_rows = 0
        execution_plan: list[tuple[int, str, str]] = []
        seen_cf_rows: dict[str, int] = {}

        for invalid_row in load_result.invalid_rows:
            skipped_invalid += 1
            raw_cf = invalid_row.values.get("LOCATORE_CF") or invalid_row.values.get("CODICE_FISCALE") or ""
            item_results.append(
                BulkImportItemResult(
                    row_number=invalid_row.row_number,
                    raw_locatore_cf=raw_cf,
                    normalized_locatore_cf=None,
                    status=BULK_IMPORT_STATUS_SKIPPED_INVALID,
                    message=invalid_row.message,
                )
            )

        for row in load_result.rows:
            normalized_cf = _normalize_bulk_locatore_cf(row.locatore_cf)
            if normalized_cf is None:
                skipped_invalid += 1
                item_results.append(
                    BulkImportItemResult(
                        row_number=row.row_number,
                        raw_locatore_cf=row.locatore_cf,
                        normalized_locatore_cf=None,
                        status=BULK_IMPORT_STATUS_SKIPPED_INVALID,
                        message=f"Row {row.row_number} has an invalid Codice Fiscale value.",
                    )
                )
                continue

            valid_rows += 1
            if normalized_cf in seen_cf_rows:
                skipped_duplicate += 1
                item_results.append(
                    BulkImportItemResult(
                        row_number=row.row_number,
                        raw_locatore_cf=row.locatore_cf,
                        normalized_locatore_cf=normalized_cf,
                        status=BULK_IMPORT_STATUS_SKIPPED_DUPLICATE,
                        message=(
                            f"Duplicate LOCATORE_CF skipped; first occurrence was row {seen_cf_rows[normalized_cf]}."
                        ),
                    )
                )
                continue

            seen_cf_rows[normalized_cf] = row.row_number
            execution_plan.append((row.row_number, row.locatore_cf, normalized_cf))

        imported_successfully = 0
        failed = 0
        aborted = False

        # Bulk mode is intentionally import-only: it reuses the existing boundary
        # and never invokes YAML generation or later generation stages.
        for row_number, raw_cf, normalized_cf in execution_plan:
            try:
                self.import_runner.run_for_cf(
                    normalized_cf,
                    force_update_visura=bool(force_update_visura),
                )
            except ImportOnlyRunnerFailedError as exc:
                failed += 1
                item_results.append(
                    BulkImportItemResult(
                        row_number=row_number,
                        raw_locatore_cf=raw_cf,
                        normalized_locatore_cf=normalized_cf,
                        status=BULK_IMPORT_STATUS_FAILED,
                        message=str(exc),
                    )
                )
                if fail_fast:
                    aborted = True
                    break
            except Exception as exc:
                failed += 1
                item_results.append(
                    BulkImportItemResult(
                        row_number=row_number,
                        raw_locatore_cf=raw_cf,
                        normalized_locatore_cf=normalized_cf,
                        status=BULK_IMPORT_STATUS_FAILED,
                        message=f"Unexpected import-only runner failure: {exc}",
                    )
                )
                if fail_fast:
                    aborted = True
                    break
            else:
                imported_successfully += 1
                item_results.append(
                    BulkImportItemResult(
                        row_number=row_number,
                        raw_locatore_cf=raw_cf,
                        normalized_locatore_cf=normalized_cf,
                        status=BULK_IMPORT_STATUS_IMPORTED,
                        message="Import-only runner completed successfully.",
                    )
                )

        return BulkImportRunResult(
            source_csv=resolved_csv_path,
            total_rows=load_result.total_rows,
            valid_rows=valid_rows,
            deduped_cf_count=len(execution_plan),
            imported_successfully=imported_successfully,
            failed=failed,
            skipped_invalid=skipped_invalid,
            skipped_duplicate=skipped_duplicate,
            fail_fast=bool(fail_fast),
            force_update_visura=bool(force_update_visura),
            aborted=aborted,
            item_results=tuple(sorted(item_results, key=lambda item: item.row_number)),
        )

    def _load_csv(self, csv_path: Path) -> BulkClientsCsvLoadResult:
        """Use the existing CSV loading layer and surface typed bulk-load errors."""
        try:
            return self.csv_loader(path=csv_path)
        except BulkImportCsvLoadError:
            raise
        except Exception as exc:
            raise BulkImportCsvLoadError(
                f"Could not load clients.csv from {csv_path}.",
                details={"csv_path": str(csv_path)},
            ) from exc


def format_bulk_import_summary(result: BulkImportRunResult) -> str:
    """Build a deterministic operator-facing summary for stdout reporting."""
    lines = [
        f"Bulk import-only summary for {result.source_csv}",
        f"total_rows={result.total_rows}",
        f"valid_rows={result.valid_rows}",
        f"deduped_cf_count={result.deduped_cf_count}",
        f"imported_successfully={result.imported_successfully}",
        f"failed={result.failed}",
        f"skipped_invalid={result.skipped_invalid}",
        f"skipped_duplicate={result.skipped_duplicate}",
        f"aborted={result.aborted}",
    ]

    for item in result.item_results:
        cf_display = item.normalized_locatore_cf or item.raw_locatore_cf or "<missing>"
        lines.append(
            f"row={item.row_number} status={item.status} cf={cf_display} message={item.message}"
        )

    return "\n".join(lines)


def _normalize_bulk_locatore_cf(value: str) -> str | None:
    """Normalize one CSV CF value using the same deterministic policy as prepare."""
    normalized = str(value or "").strip().upper()
    if not normalized:
        return None
    if len(normalized) != 16 or not normalized.isalnum():
        return None
    return normalized
