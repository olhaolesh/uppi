"""Tests for the Stage 4 web generation adapter seam."""

from __future__ import annotations

from pathlib import Path

import pytest

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.domain.exceptions import GenerationPrepareRequiredError
from uppi.domain.failure_registry import FailureRecord, FailureStage
from uppi.services.generation_runner import GenerationArtifactRef, GenerationRunnerResult
from uppi.web.schemas.attestazioni import AttestazioniGenerateRequest
from uppi.web.services.generation_adapter import GenerationAdapter, GenerationRunFailedError
from uppi.web.services.generation_yaml_builder import BuiltGenerationYaml


class _FakeYamlBuilder:
    """Returns a prebuilt generation input without touching the filesystem."""

    def __init__(self, built: BuiltGenerationYaml) -> None:
        self.built = built
        self.calls: list[tuple[AttestazioniGenerateRequest, str | None]] = []

    def build(
        self,
        payload: AttestazioniGenerateRequest,
        *,
        run_id: str | None = None,
    ) -> BuiltGenerationYaml:
        self.calls.append((payload, run_id))
        if run_id is None:
            return self.built
        return BuiltGenerationYaml(
            run_id=run_id,
            locatore_cf=self.built.locatore_cf,
            prepared_output_path=self.built.prepared_output_path,
            generation_output_path=self.built.generation_output_path.parent.parent / run_id / "immobili.yml",
            requested_count=self.built.requested_count,
            document=self.built.document,
        )


class _FakeRunner:
    """Returns a fixed runner result and records the delegated call."""

    def __init__(self, result: GenerationRunnerResult) -> None:
        self.result = result
        self.calls: list[tuple[Path, str]] = []

    def run_yaml(self, yaml_path: Path, *, run_id: str | None = None) -> GenerationRunnerResult:
        self.calls.append((yaml_path, str(run_id)))
        return self.result


def _make_request() -> AttestazioniGenerateRequest:
    return AttestazioniGenerateRequest.model_validate(
        {
            "locatore_cf": "rssmra80a01h501z",
            "prepared_immobili_yaml_path": "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
            "client_updates": {"locatore_via": "VIA ROMA"},
            "immobili": [
                {
                    "index": 1,
                    "enabled": True,
                    "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                    "editable": {"immobile_via": "VIA ROMA"},
                    "run_only": {"conduttore_nome": "Mario Rossi"},
                    "elements": {"a1": "X"},
                }
            ],
        }
    )


def _make_built_yaml(repo_root: Path) -> BuiltGenerationYaml:
    return BuiltGenerationYaml(
        run_id="run-123",
        locatore_cf="RSSMRA80A01H501Z",
        prepared_output_path=repo_root / "clients" / "web_prepare" / "RSSMRA80A01H501Z" / "immobili.yml",
        generation_output_path=repo_root / "clients" / "web_generation" / "RSSMRA80A01H501Z" / "run-123" / "immobili.yml",
        requested_count=1,
        document=ImmobiliDocumentConfig(locatore_cf="RSSMRA80A01H501Z"),
    )


def test_generation_adapter_delegates_to_yaml_builder_and_current_runner(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    built = _make_built_yaml(tmp_path)
    fake_builder = _FakeYamlBuilder(built)
    fake_runner = _FakeRunner(
        GenerationRunnerResult(
            run_id="run-123",
            locatore_cf="RSSMRA80A01H501Z",
            requested_count=1,
            generated_count=1,
            failed_count=0,
            artifacts=(
                GenerationArtifactRef(
                    index=1,
                    foglio="12",
                    numero="345",
                    sub="7",
                    kind="attestazione_docx",
                    local_path=str(tmp_path / "downloads" / "doc.docx"),
                    bucket="attestazioni",
                    object_key="attestazioni/RSSMRA80A01H501Z/81.docx",
                ),
            ),
        )
    )
    adapter = GenerationAdapter(
        repo_root=tmp_path,
        yaml_builder=fake_builder,
        generation_runner_factory=lambda: fake_runner,
    )

    result = adapter.generate(_make_request())

    assert fake_builder.calls[0][0].locatore_cf == "RSSMRA80A01H501Z"
    assert fake_runner.calls == [((tmp_path / "clients" / "web_generation" / "RSSMRA80A01H501Z" / "run-123" / "immobili.yml"), "run-123")]
    assert result.prepared_output_path_relative == "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml"
    assert result.generation_output_path_relative == "clients/web_generation/RSSMRA80A01H501Z/run-123/immobili.yml"
    assert result.generated_count == 1
    assert result.artifacts[0].object_key == "attestazioni/RSSMRA80A01H501Z/81.docx"


def test_generation_adapter_raises_prepare_required_when_runner_reports_strict_match_failure(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    built = _make_built_yaml(tmp_path)
    fake_builder = _FakeYamlBuilder(built)
    failure_record = FailureRecord.from_error(
        run_id="run-123",
        client_cf="RSSMRA80A01H501Z",
        stage=FailureStage.PIPELINE_FATAL,
        error=GenerationPrepareRequiredError("prepare again"),
        retryable=True,
    )
    fake_runner = _FakeRunner(
        GenerationRunnerResult(
            run_id="run-123",
            locatore_cf="RSSMRA80A01H501Z",
            requested_count=1,
            generated_count=0,
            failed_count=1,
            artifacts=(),
            failure_records=(failure_record,),
        )
    )
    adapter = GenerationAdapter(
        repo_root=tmp_path,
        yaml_builder=fake_builder,
        generation_runner_factory=lambda: fake_runner,
    )

    with pytest.raises(GenerationPrepareRequiredError):
        adapter.generate(_make_request())


def test_generation_adapter_raises_when_no_artifact_can_be_produced(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    built = _make_built_yaml(tmp_path)
    fake_builder = _FakeYamlBuilder(built)
    fake_runner = _FakeRunner(
        GenerationRunnerResult(
            run_id="run-123",
            locatore_cf="RSSMRA80A01H501Z",
            requested_count=1,
            generated_count=0,
            failed_count=1,
            artifacts=(),
        )
    )
    adapter = GenerationAdapter(
        repo_root=tmp_path,
        yaml_builder=fake_builder,
        generation_runner_factory=lambda: fake_runner,
    )

    with pytest.raises(GenerationRunFailedError):
        adapter.generate(_make_request())
