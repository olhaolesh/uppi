"""Focused tests for prepare-by-CF orchestration decisions."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from uppi.domain.exceptions import (
    ImmobiliDocumentNotFoundError,
    PrepareGenerationFailedError,
    PrepareImportFailedError,
    PrepareInputError,
    PrepareNoDataError,
    PrepareOutputWriteError,
)
from uppi.services.prepare_by_cf import (
    PREPARE_DECISION_DB_HIT,
    PREPARE_DECISION_DB_MISS,
    PREPARE_DECISION_FORCE_REFRESH,
    PrepareByCfService,
)
from uppi.services.repositories.prepare_document_repo import PrepareDocumentPresence


class _FakeConnection:
    """Tiny connection double for prepare presence checks."""

    def __init__(self, close_calls: list[str]) -> None:
        self._close_calls = close_calls

    def close(self) -> None:
        self._close_calls.append("close")


class _RecordingPresenceLoader:
    """Return explicit DB presence states in a deterministic sequence."""

    def __init__(self, statuses: list[PrepareDocumentPresence]) -> None:
        self.statuses = list(statuses)
        self.calls: list[str] = []

    def __call__(self, conn, locatore_cf: str) -> PrepareDocumentPresence:
        self.calls.append(locatore_cf)
        if not self.statuses:
            raise AssertionError("Unexpected extra DB presence check")
        return self.statuses.pop(0)


class _RecordingImportRunner:
    """Record prepare import invocations without launching Scrapy."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, bool]] = []

    def run_for_cf(self, locatore_cf: str, *, force_update_visura: bool) -> None:
        self.calls.append((locatore_cf, force_update_visura))
        if self.error is not None:
            raise self.error


class _RecordingYamlGenerator:
    """Record YAML writes and optionally fail with a chosen error."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, Path]] = []

    def write_yaml(self, locatore_cf: str, path: str | Path) -> Path:
        resolved_path = Path(path)
        self.calls.append((locatore_cf, resolved_path))
        if self.error is not None:
            raise self.error
        return resolved_path


def _make_service(
    tmp_path,
    *,
    statuses: list[PrepareDocumentPresence],
    import_runner=None,
    yaml_generator=None,
    repo_root: Path | None = None,
):
    """Create a prepare service with explicit seams for narrow tests."""
    close_calls: list[str] = []
    presence_loader = _RecordingPresenceLoader(statuses)

    service = PrepareByCfService(
        connection_factory=lambda: _FakeConnection(close_calls),
        presence_loader=presence_loader,
        import_runner=import_runner or _RecordingImportRunner(),
        yaml_generator=yaml_generator or _RecordingYamlGenerator(),
        app_config_loader=lambda: SimpleNamespace(
            immobili=SimpleNamespace(immobili_file=tmp_path / "clients" / "immobili.yml")
        ),
        repo_root=repo_root or tmp_path,
    )
    return service, presence_loader, close_calls


def test_prepare_case_a_db_hit_without_force_skips_import_and_writes_default_output(tmp_path):
    """Case A: prepare must stay in DB-only mode when the hit criterion is satisfied."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=2)
    import_runner = _RecordingImportRunner()
    yaml_generator = _RecordingYamlGenerator()
    service, presence_loader, close_calls = _make_service(
        tmp_path,
        statuses=[hit],
        import_runner=import_runner,
        yaml_generator=yaml_generator,
    )

    result = service.prepare("rssmra80a01h501z")

    assert result.locatore_cf == "RSSMRA80A01H501Z"
    assert result.decision == PREPARE_DECISION_DB_HIT
    assert result.db_hit_before_import is True
    assert result.import_performed is False
    assert result.output_path == tmp_path / "clients" / "immobili.yml"
    assert import_runner.calls == []
    assert yaml_generator.calls == [("RSSMRA80A01H501Z", tmp_path / "clients" / "immobili.yml")]
    assert presence_loader.calls == ["RSSMRA80A01H501Z"]
    assert close_calls == ["close"]


def test_prepare_case_b_db_miss_runs_import_then_writes_yaml(tmp_path):
    """Case B: prepare must refresh through the import-only path before generating YAML."""
    miss = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=False, immobili_count=0)
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    import_runner = _RecordingImportRunner()
    yaml_generator = _RecordingYamlGenerator()
    service, presence_loader, close_calls = _make_service(
        tmp_path,
        statuses=[miss, hit],
        import_runner=import_runner,
        yaml_generator=yaml_generator,
    )

    result = service.prepare("RSSMRA80A01H501Z")

    assert result.decision == PREPARE_DECISION_DB_MISS
    assert result.db_hit_before_import is False
    assert result.import_performed is True
    assert import_runner.calls == [("RSSMRA80A01H501Z", True)]
    assert yaml_generator.calls == [("RSSMRA80A01H501Z", tmp_path / "clients" / "immobili.yml")]
    assert presence_loader.calls == ["RSSMRA80A01H501Z", "RSSMRA80A01H501Z"]
    assert close_calls == ["close", "close"]


def test_prepare_case_c_force_refresh_runs_import_even_on_db_hit(tmp_path):
    """Case C: the operator force flag must always route prepare through import first."""
    hit_before = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=2)
    hit_after = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=2)
    import_runner = _RecordingImportRunner()
    yaml_generator = _RecordingYamlGenerator()
    service, presence_loader, close_calls = _make_service(
        tmp_path,
        statuses=[hit_before, hit_after],
        import_runner=import_runner,
        yaml_generator=yaml_generator,
    )

    result = service.prepare("RSSMRA80A01H501Z", force_update_visura=True)

    assert result.decision == PREPARE_DECISION_FORCE_REFRESH
    assert result.db_hit_before_import is True
    assert result.import_performed is True
    assert import_runner.calls == [("RSSMRA80A01H501Z", True)]
    assert yaml_generator.calls == [("RSSMRA80A01H501Z", tmp_path / "clients" / "immobili.yml")]
    assert presence_loader.calls == ["RSSMRA80A01H501Z", "RSSMRA80A01H501Z"]
    assert close_calls == ["close", "close"]


def test_prepare_resolves_relative_output_paths_from_repo_root(tmp_path):
    """Relative `--output` values should resolve deterministically from the repo root."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    yaml_generator = _RecordingYamlGenerator()
    repo_root = tmp_path / "repo"
    service, _, _ = _make_service(
        tmp_path,
        statuses=[hit],
        yaml_generator=yaml_generator,
        repo_root=repo_root,
    )

    result = service.prepare("RSSMRA80A01H501Z", output_path="out/generated-immobili.yml")

    assert result.output_path == repo_root / "out" / "generated-immobili.yml"
    assert yaml_generator.calls == [("RSSMRA80A01H501Z", repo_root / "out" / "generated-immobili.yml")]


def test_prepare_accepts_default_output_path_even_when_no_input_immobili_yaml_exists(tmp_path):
    """Prepare uses the configured output path only and does not require an input YAML file."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    yaml_generator = _RecordingYamlGenerator()
    service, _, _ = _make_service(
        tmp_path,
        statuses=[hit],
        yaml_generator=yaml_generator,
    )

    result = service.prepare("RSSMRA80A01H501Z")

    assert result.output_path == tmp_path / "clients" / "immobili.yml"
    assert yaml_generator.calls == [("RSSMRA80A01H501Z", tmp_path / "clients" / "immobili.yml")]


def test_prepare_rejects_empty_or_malformed_cf(tmp_path):
    """Prepare should fail fast on invalid operator input."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    service, _, _ = _make_service(tmp_path, statuses=[hit])

    with pytest.raises(PrepareInputError):
        service.prepare("   ")

    with pytest.raises(PrepareInputError):
        service.prepare("SHORT")


def test_prepare_wraps_import_failures_and_skips_generation(tmp_path):
    """Generator must not run when the import-only runner fails."""
    miss = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=False, immobili_count=0)
    import_runner = _RecordingImportRunner(error=RuntimeError("browser failed"))
    yaml_generator = _RecordingYamlGenerator()
    service, _, _ = _make_service(
        tmp_path,
        statuses=[miss],
        import_runner=import_runner,
        yaml_generator=yaml_generator,
    )

    with pytest.raises(PrepareImportFailedError):
        service.prepare("RSSMRA80A01H501Z")

    assert import_runner.calls == [("RSSMRA80A01H501Z", True)]
    assert yaml_generator.calls == []


def test_prepare_raises_no_data_when_import_completes_but_db_stays_empty(tmp_path):
    """Prepare should fail explicitly when import finishes without producing DB hit state."""
    miss_before = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=False, immobili_count=0)
    miss_after = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=0)
    import_runner = _RecordingImportRunner()
    yaml_generator = _RecordingYamlGenerator()
    service, _, _ = _make_service(
        tmp_path,
        statuses=[miss_before, miss_after],
        import_runner=import_runner,
        yaml_generator=yaml_generator,
    )

    with pytest.raises(PrepareNoDataError):
        service.prepare("RSSMRA80A01H501Z")

    assert import_runner.calls == [("RSSMRA80A01H501Z", True)]
    assert yaml_generator.calls == []


def test_prepare_wraps_not_found_generator_failures_after_import(tmp_path):
    """Prepare should surface a typed no-data error when generation still cannot build the document."""
    miss = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=False, immobili_count=0)
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    generator_error = ImmobiliDocumentNotFoundError("missing generated document")
    service, _, _ = _make_service(
        tmp_path,
        statuses=[miss, hit],
        import_runner=_RecordingImportRunner(),
        yaml_generator=_RecordingYamlGenerator(error=generator_error),
    )

    with pytest.raises(PrepareNoDataError):
        service.prepare("RSSMRA80A01H501Z")


def test_prepare_wraps_generic_generator_failures(tmp_path):
    """Unexpected generator errors should surface as explicit prepare generation failures."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    service, _, _ = _make_service(
        tmp_path,
        statuses=[hit],
        yaml_generator=_RecordingYamlGenerator(error=RuntimeError("generator exploded")),
    )

    with pytest.raises(PrepareGenerationFailedError):
        service.prepare("RSSMRA80A01H501Z")


def test_prepare_wraps_output_write_failures(tmp_path):
    """Filesystem write problems should surface as explicit prepare output errors."""
    hit = PrepareDocumentPresence("RSSMRA80A01H501Z", root_found=True, immobili_count=1)
    service, _, _ = _make_service(
        tmp_path,
        statuses=[hit],
        yaml_generator=_RecordingYamlGenerator(error=OSError("disk full")),
    )

    with pytest.raises(PrepareOutputWriteError):
        service.prepare("RSSMRA80A01H501Z")
