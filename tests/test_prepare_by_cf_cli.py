"""CLI tests for the prepare-by-CF entry point."""

from __future__ import annotations

from pathlib import Path

from uppi.domain.exceptions import PrepareImportFailedError, PrepareInputError
from uppi.services.prepare_by_cf import PrepareByCfResult


def test_prepare_by_cf_cli_calls_service_and_prints_success(monkeypatch, capsys, tmp_path):
    """The CLI should be a thin wrapper around the prepare service."""
    from uppi.cli import prepare_by_cf as cli_module

    captured = {}

    class FakeService:
        """Record one prepare call and return a stable result."""

        def prepare(self, locatore_cf, *, force_update_visura=False, output_path=None):
            captured["locatore_cf"] = locatore_cf
            captured["force_update_visura"] = force_update_visura
            captured["output_path"] = output_path
            return PrepareByCfResult(
                locatore_cf="RSSMRA80A01H501Z",
                output_path=tmp_path / "clients" / "immobili.yml",
                decision="db_hit_no_force",
                db_hit_before_import=True,
                import_performed=False,
            )

    monkeypatch.setattr(cli_module, "PrepareByCfService", lambda: FakeService())

    exit_code = cli_module.main(
        [
            "--cf",
            "rssmra80a01h501z",
            "--force-update-visura",
            "--output",
            "out/generated-immobili.yml",
        ]
    )

    out = capsys.readouterr().out

    assert exit_code == 0
    assert captured["locatore_cf"] == "rssmra80a01h501z"
    assert captured["force_update_visura"] is True
    assert captured["output_path"] == Path("out/generated-immobili.yml")
    assert "Prepared LOCATORE_CF=RSSMRA80A01H501Z" in out


def test_prepare_by_cf_cli_returns_input_error_code(monkeypatch, capsys):
    """Operator input errors should map to exit code 2."""
    from uppi.cli import prepare_by_cf as cli_module

    class FakeService:
        """Raise a typed prepare input error."""

        def prepare(self, *args, **kwargs):
            raise PrepareInputError("bad cf")

    monkeypatch.setattr(cli_module, "PrepareByCfService", lambda: FakeService())

    exit_code = cli_module.main(["--cf", "bad"])

    captured = capsys.readouterr()

    assert exit_code == 2
    assert "Prepare input error: bad cf" in captured.err


def test_prepare_by_cf_cli_returns_runtime_error_code(monkeypatch, capsys):
    """Prepare runtime failures should map to exit code 1."""
    from uppi.cli import prepare_by_cf as cli_module

    class FakeService:
        """Raise a typed prepare runtime error."""

        def prepare(self, *args, **kwargs):
            raise PrepareImportFailedError("import failed")

    monkeypatch.setattr(cli_module, "PrepareByCfService", lambda: FakeService())

    exit_code = cli_module.main(["--cf", "RSSMRA80A01H501Z"])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "Prepare failed: import failed" in captured.err
