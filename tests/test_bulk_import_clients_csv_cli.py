"""CLI tests for bulk CSV import-only mode."""

from __future__ import annotations

from pathlib import Path

from uppi.domain.exceptions import BulkImportCsvLoadError
from uppi.services.bulk_import_clients_csv import BulkImportRunResult


def test_bulk_import_cli_calls_service_and_prints_summary(monkeypatch, capsys, tmp_path):
    """The CLI should be a thin wrapper around the bulk import service."""
    from uppi.cli import bulk_import_clients_csv as cli_module

    captured = {}

    class FakeService:
        """Record the CLI call and return a clean bulk result."""

        def run(self, csv_path, *, force_update_visura=False, fail_fast=False):
            captured["csv_path"] = csv_path
            captured["force_update_visura"] = force_update_visura
            captured["fail_fast"] = fail_fast
            return BulkImportRunResult(
                source_csv=tmp_path / "clients.csv",
                total_rows=1,
                valid_rows=1,
                deduped_cf_count=1,
                imported_successfully=1,
                failed=0,
                skipped_invalid=0,
                skipped_duplicate=0,
                fail_fast=False,
                force_update_visura=True,
                aborted=False,
                item_results=(),
            )

    monkeypatch.setattr(cli_module, "BulkImportClientsCsvService", lambda: FakeService())
    monkeypatch.setattr(cli_module, "format_bulk_import_summary", lambda result: "bulk summary")

    exit_code = cli_module.main(
        [
            "--csv",
            "input/clients.csv",
            "--force-update-visura",
        ]
    )

    out = capsys.readouterr().out

    assert exit_code == 0
    assert captured["csv_path"] == Path("input/clients.csv")
    assert captured["force_update_visura"] is True
    assert captured["fail_fast"] is False
    assert "bulk summary" in out


def test_bulk_import_cli_returns_failure_exit_code_for_partial_failures(monkeypatch, capsys, tmp_path):
    """Mixed bulk runs should still print a summary but return a non-zero exit code."""
    from uppi.cli import bulk_import_clients_csv as cli_module

    class FakeService:
        """Return a bulk result with one failed CF."""

        def run(self, *args, **kwargs):
            return BulkImportRunResult(
                source_csv=tmp_path / "clients.csv",
                total_rows=2,
                valid_rows=2,
                deduped_cf_count=2,
                imported_successfully=1,
                failed=1,
                skipped_invalid=0,
                skipped_duplicate=0,
                fail_fast=False,
                force_update_visura=False,
                aborted=False,
                item_results=(),
            )

    monkeypatch.setattr(cli_module, "BulkImportClientsCsvService", lambda: FakeService())
    monkeypatch.setattr(cli_module, "format_bulk_import_summary", lambda result: "partial summary")

    exit_code = cli_module.main(["--csv", "clients.csv"])

    out = capsys.readouterr().out

    assert exit_code == 1
    assert "partial summary" in out


def test_bulk_import_cli_returns_runtime_error_code(monkeypatch, capsys):
    """CSV load failures should map to a non-zero CLI exit code."""
    from uppi.cli import bulk_import_clients_csv as cli_module

    class FakeService:
        """Raise a typed bulk runtime error."""

        def run(self, *args, **kwargs):
            raise BulkImportCsvLoadError("csv missing")

    monkeypatch.setattr(cli_module, "BulkImportClientsCsvService", lambda: FakeService())

    exit_code = cli_module.main(["--csv", "missing.csv", "--fail-fast"])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Bulk import failed: csv missing" in captured.err
