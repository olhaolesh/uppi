"""Focused tests for bulk CSV import-only orchestration."""

from __future__ import annotations

from pathlib import Path

from uppi.config.clients_csv import BulkClientCsvInvalidRow, BulkClientCsvRow, BulkClientsCsvLoadResult
from uppi.domain.exceptions import BulkImportCsvLoadError, ImportOnlyRunnerFailedError
from uppi.services.bulk_import_clients_csv import (
    BULK_IMPORT_STATUS_FAILED,
    BULK_IMPORT_STATUS_IMPORTED,
    BULK_IMPORT_STATUS_SKIPPED_DUPLICATE,
    BULK_IMPORT_STATUS_SKIPPED_INVALID,
    BulkImportClientsCsvService,
)


def test_bulk_service_reuses_loader_and_runner_with_normalization_dedupe_and_continue_on_error(monkeypatch, tmp_path):
    """Bulk mode should reuse CSV loading and import-only execution without prepare semantics."""
    from uppi.services import bulk_import_clients_csv as bulk_module

    csv_path = tmp_path / "clients.csv"
    load_calls = []
    import_calls = []

    def fake_loader(*, path):
        load_calls.append(path)
        return BulkClientsCsvLoadResult(
            rows=(
                BulkClientCsvRow(row_number=3, locatore_cf=" rssmra80a01h501z ", values={"LOCATORE_CF": " rssmra80a01h501z "}),
                BulkClientCsvRow(row_number=4, locatore_cf="RSSMRA80A01H501Z", values={"LOCATORE_CF": "RSSMRA80A01H501Z"}),
                BulkClientCsvRow(row_number=5, locatore_cf="SHORT", values={"LOCATORE_CF": "SHORT"}),
                BulkClientCsvRow(row_number=6, locatore_cf="BNCMRA80A01H501Z", values={"LOCATORE_CF": "BNCMRA80A01H501Z"}),
                BulkClientCsvRow(row_number=7, locatore_cf="VRDLGI80A01H501Z", values={"LOCATORE_CF": "VRDLGI80A01H501Z"}),
            ),
            invalid_rows=(
                BulkClientCsvInvalidRow(
                    row_number=2,
                    values={"NOTE": "missing"},
                    message="clients.csv row 2 is missing LOCATORE_CF",
                    code="missing_locatore_cf",
                ),
            ),
            total_rows=6,
        )

    class FakeImportRunner:
        """Record per-CF import invocations and fail for one target CF."""

        def run_for_cf(self, locatore_cf: str, *, force_update_visura: bool) -> None:
            import_calls.append((locatore_cf, force_update_visura))
            if locatore_cf == "BNCMRA80A01H501Z":
                raise ImportOnlyRunnerFailedError("runner failed for BNCMRA80A01H501Z")

    monkeypatch.setattr(bulk_module, "load_clients_csv_with_issues", fake_loader)
    monkeypatch.setattr(bulk_module, "ScrapyImportOnlyRunner", FakeImportRunner)

    service = bulk_module.BulkImportClientsCsvService()
    result = service.run(csv_path, force_update_visura=False, fail_fast=False)

    assert load_calls == [csv_path]
    assert import_calls == [
        ("RSSMRA80A01H501Z", False),
        ("BNCMRA80A01H501Z", False),
        ("VRDLGI80A01H501Z", False),
    ]
    assert result.total_rows == 6
    assert result.valid_rows == 4
    assert result.deduped_cf_count == 3
    assert result.imported_successfully == 2
    assert result.failed == 1
    assert result.skipped_invalid == 2
    assert result.skipped_duplicate == 1
    assert result.aborted is False
    assert [item.status for item in result.item_results] == [
        BULK_IMPORT_STATUS_SKIPPED_INVALID,
        BULK_IMPORT_STATUS_IMPORTED,
        BULK_IMPORT_STATUS_SKIPPED_DUPLICATE,
        BULK_IMPORT_STATUS_SKIPPED_INVALID,
        BULK_IMPORT_STATUS_FAILED,
        BULK_IMPORT_STATUS_IMPORTED,
    ]


def test_bulk_service_fail_fast_stops_after_first_import_failure(tmp_path):
    """Fail-fast mode should stop the per-CF import loop on the first failure."""
    csv_path = tmp_path / "clients.csv"
    import_calls = []

    class FakeImportRunner:
        """Fail immediately for the first CF and record calls."""

        def run_for_cf(self, locatore_cf: str, *, force_update_visura: bool) -> None:
            import_calls.append((locatore_cf, force_update_visura))
            raise ImportOnlyRunnerFailedError("first CF failed")

    service = BulkImportClientsCsvService(
        csv_loader=lambda *, path: BulkClientsCsvLoadResult(
            rows=(
                BulkClientCsvRow(row_number=2, locatore_cf="RSSMRA80A01H501Z", values={"LOCATORE_CF": "RSSMRA80A01H501Z"}),
                BulkClientCsvRow(row_number=3, locatore_cf="BNCMRA80A01H501Z", values={"LOCATORE_CF": "BNCMRA80A01H501Z"}),
            ),
            invalid_rows=(),
            total_rows=2,
        ),
        import_runner=FakeImportRunner(),
    )

    result = service.run(csv_path, force_update_visura=True, fail_fast=True)

    assert import_calls == [("RSSMRA80A01H501Z", True)]
    assert result.deduped_cf_count == 2
    assert result.imported_successfully == 0
    assert result.failed == 1
    assert result.aborted is True
    assert [item.status for item in result.item_results] == [BULK_IMPORT_STATUS_FAILED]


def test_bulk_service_wraps_csv_load_failures(tmp_path):
    """CSV loading failures should surface as typed bulk-mode errors."""
    csv_path = tmp_path / "clients.csv"
    service = BulkImportClientsCsvService(
        csv_loader=lambda *, path: (_ for _ in ()).throw(FileNotFoundError("missing csv")),
        import_runner=object(),
    )

    try:
        service.run(csv_path)
    except BulkImportCsvLoadError as exc:
        assert "Could not load clients.csv" in str(exc)
    else:
        raise AssertionError("BulkImportCsvLoadError expected")
