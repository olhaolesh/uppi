"""Tests for the Stage 5 bulk-import adapter seam."""

from __future__ import annotations

from pathlib import Path

import pytest

from uppi.services.bulk_import_clients_csv import (
    BULK_IMPORT_STATUS_FAILED,
    BULK_IMPORT_STATUS_IMPORTED,
    BULK_IMPORT_STATUS_SKIPPED_DUPLICATE,
    BULK_IMPORT_STATUS_SKIPPED_INVALID,
    BulkImportItemResult,
    BulkImportRunResult,
)
from uppi.web.schemas.clients import ClientsBulkImportRequest
from uppi.web.services.bulk_import_adapter import (
    BulkImportAdapter,
    BulkImportNoUsableRowsError,
)


class _FakeBulkImportService:
    """Records one adapter call and returns a fixed current bulk result."""

    def __init__(self, result: BulkImportRunResult) -> None:
        self.result = result
        self.calls: list[tuple[Path, bool, bool]] = []

    def run(
        self,
        csv_path: str | Path,
        *,
        force_update_visura: bool = False,
        fail_fast: bool = False,
    ) -> BulkImportRunResult:
        self.calls.append((Path(csv_path), force_update_visura, fail_fast))
        return self.result


def _make_request(**overrides) -> ClientsBulkImportRequest:
    payload = {
        "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
        "force_update_visura": False,
        "fail_fast": False,
    }
    payload.update(overrides)
    return ClientsBulkImportRequest.model_validate(payload)


def test_bulk_import_adapter_writes_safe_web_run_csv_and_delegates_to_current_owner_path(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    result = BulkImportRunResult(
        source_csv=tmp_path / "clients" / "web_bulk_import" / "run-123" / "clients.csv",
        total_rows=3,
        valid_rows=2,
        deduped_cf_count=2,
        imported_successfully=1,
        failed=0,
        skipped_invalid=1,
        skipped_duplicate=1,
        fail_fast=False,
        force_update_visura=True,
        aborted=False,
        item_results=(
            BulkImportItemResult(
                row_number=2,
                raw_locatore_cf="RSSMRA80A01H501Z",
                normalized_locatore_cf="RSSMRA80A01H501Z",
                status=BULK_IMPORT_STATUS_IMPORTED,
                message="Imported successfully",
                code="imported",
            ),
            BulkImportItemResult(
                row_number=3,
                raw_locatore_cf="BNCLGU85C01G482K",
                normalized_locatore_cf="BNCLGU85C01G482K",
                status=BULK_IMPORT_STATUS_SKIPPED_DUPLICATE,
                message="Duplicate row",
                code="duplicate_locatore_cf",
            ),
            BulkImportItemResult(
                row_number=4,
                raw_locatore_cf="",
                normalized_locatore_cf=None,
                status=BULK_IMPORT_STATUS_SKIPPED_INVALID,
                message="clients.csv row 4 is missing LOCATORE_CF",
                code="missing_locatore_cf",
            ),
        ),
    )
    fake_service = _FakeBulkImportService(result)
    adapter = BulkImportAdapter(
        repo_root=tmp_path,
        bulk_import_service_factory=lambda: fake_service,
        run_id_factory=lambda: "run-123",
    )

    web_result = adapter.import_clients(
        _make_request(force_update_visura=True)
    )

    expected_csv_path = tmp_path / "clients" / "web_bulk_import" / "run-123" / "clients.csv"
    assert fake_service.calls == [(expected_csv_path, True, False)]
    assert expected_csv_path.read_text(encoding="utf-8") == "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n"
    assert not (tmp_path / "clients" / "clients.csv").exists()
    assert web_result.status == "completed"
    assert web_result.clients_csv_path_relative == "clients/web_bulk_import/run-123/clients.csv"
    assert web_result.valid_rows == 2
    assert web_result.invalid_rows_count == 1
    assert web_result.unique_clients == 2
    assert web_result.imported_count == 1
    assert web_result.skipped_count == 1
    assert web_result.results[0].status == "imported"
    assert web_result.results[1].status == "skipped_duplicate"
    assert web_result.invalid_rows[0].code == "missing_locatore_cf"


def test_bulk_import_adapter_surfaces_aborted_summary_and_rejects_no_usable_rows(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    aborted_service = _FakeBulkImportService(
        BulkImportRunResult(
            source_csv=tmp_path / "clients" / "web_bulk_import" / "run-124" / "clients.csv",
            total_rows=1,
            valid_rows=1,
            deduped_cf_count=1,
            imported_successfully=0,
            failed=1,
            skipped_invalid=0,
            skipped_duplicate=0,
            fail_fast=True,
            force_update_visura=False,
            aborted=True,
            item_results=(
                BulkImportItemResult(
                    row_number=2,
                    raw_locatore_cf="RSSMRA80A01H501Z",
                    normalized_locatore_cf="RSSMRA80A01H501Z",
                    status=BULK_IMPORT_STATUS_FAILED,
                    message="Runner failed",
                    code="import_only_runner_failed",
                ),
            ),
        )
    )
    aborted_adapter = BulkImportAdapter(
        repo_root=tmp_path,
        bulk_import_service_factory=lambda: aborted_service,
        run_id_factory=lambda: "run-124",
    )

    aborted_result = aborted_adapter.import_clients(
        _make_request(fail_fast=True)
    )

    assert aborted_result.status == "aborted"
    assert aborted_result.failed_count == 1
    assert aborted_result.messages == (
        "Bulk import aborted after the first import-only failure because fail_fast=true.",
    )

    no_rows_service = _FakeBulkImportService(
        BulkImportRunResult(
            source_csv=tmp_path / "clients" / "web_bulk_import" / "run-125" / "clients.csv",
            total_rows=1,
            valid_rows=0,
            deduped_cf_count=0,
            imported_successfully=0,
            failed=0,
            skipped_invalid=1,
            skipped_duplicate=0,
            fail_fast=False,
            force_update_visura=False,
            aborted=False,
            item_results=(),
        )
    )
    no_rows_adapter = BulkImportAdapter(
        repo_root=tmp_path,
        bulk_import_service_factory=lambda: no_rows_service,
        run_id_factory=lambda: "run-125",
    )

    with pytest.raises(BulkImportNoUsableRowsError):
        no_rows_adapter.import_clients(_make_request(csv_content="LOCATORE_CF\n"))
