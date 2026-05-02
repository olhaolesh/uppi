"""Tests for the additive programmatic generation runner seam."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from itemadapter import ItemAdapter

from uppi.config.immobili import ImmobileConfig, ImmobiliDocumentConfig
from uppi.domain.exceptions import GenerationPrepareRequiredError
from uppi.domain.failure_registry import FailureStage
from uppi.services.generation_runner import GenerationRunner


class _FakeStorage:
    """Minimal storage surface required by the recording document-stage wrapper."""

    def __init__(self) -> None:
        self.cfg = SimpleNamespace(attestazioni_bucket="attestazioni")

    def attestazione_object_name(self, cf: str, contract_id: int) -> str:
        return f"attestazioni/{cf}/{contract_id}.docx"


class _FakeDocumentStageService:
    """Records document-stage calls and returns a deterministic output path."""

    def __init__(self) -> None:
        self.storage = _FakeStorage()
        self.calls: list[tuple[str, str]] = []

    def run(
        self,
        conn,
        spider,
        adapter: ItemAdapter,
        *,
        run_id: str,
        imm,
        contract_ctx: dict,
        contract_id: int,
        immobile_id: int,
        locatore_cf: str,
        canone_result_snapshot: dict | None,
    ) -> Path | None:
        self.calls.append((locatore_cf, str(contract_id)))
        return Path(f"/tmp/{locatore_cf}_{contract_id}.docx")


class _FakeProcessor:
    """Simulates one successful item and one prepare-required failure."""

    def __init__(self, failure_registry_recorder) -> None:
        self.failure_registry_recorder = failure_registry_recorder
        self.document_stage_service = _FakeDocumentStageService()
        self.calls: list[tuple[str, str, str]] = []

    def process_generation_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.calls.append(
            (
                str(adapter.get("locatore_cf")),
                str(adapter.get("foglio")),
                str(adapter.get("run_id")),
            )
        )
        if str(adapter.get("foglio")) == "13":
            self.failure_registry_recorder.record_failure(
                run_id=str(adapter.get("run_id")),
                client_cf=str(adapter.get("locatore_cf")),
                stage=FailureStage.PIPELINE_FATAL,
                error=GenerationPrepareRequiredError("prepare again"),
            )
            return item

        self.document_stage_service.run(
            None,
            spider,
            adapter,
            run_id=str(adapter.get("run_id")),
            imm=SimpleNamespace(
                foglio=adapter.get("foglio"),
                numero=adapter.get("numero"),
                sub=adapter.get("sub"),
            ),
            contract_ctx={},
            contract_id=81,
            immobile_id=41,
            locatore_cf=str(adapter.get("locatore_cf")),
            canone_result_snapshot=None,
        )
        return item


def test_generation_runner_uses_existing_loader_mapper_and_processor_seams_without_db_or_browser():
    """Перевіряє сценарій, описаний у назві тесту."""
    document = ImmobiliDocumentConfig(
        locatore_cf="RSSMRA80A01H501Z",
        immobili=(
            ImmobileConfig(enabled=True, foglio="12", numero="345", sub="7"),
            ImmobileConfig(enabled=True, foglio="13", numero="99", sub="1"),
        ),
    )
    fake_processors: list[_FakeProcessor] = []

    def _processor_factory(failure_registry_recorder):
        processor = _FakeProcessor(failure_registry_recorder)
        fake_processors.append(processor)
        return processor

    runner = GenerationRunner(
        document_loader=lambda _: document,
        processor_factory=_processor_factory,
    )

    result = runner.run_yaml("clients/web_generation/RSSMRA80A01H501Z/run-1/immobili.yml", run_id="run-1")

    assert result.run_id == "run-1"
    assert result.locatore_cf == "RSSMRA80A01H501Z"
    assert result.requested_count == 2
    assert result.generated_count == 1
    assert result.failed_count == 1
    assert result.artifacts[0].index == 1
    assert result.artifacts[0].object_key == "attestazioni/RSSMRA80A01H501Z/81.docx"
    assert result.failure_records[0].error_type == "GenerationPrepareRequiredError"
    assert result.messages == ("1 generation item(s) failed.",)
    assert fake_processors[0].calls == [
        ("RSSMRA80A01H501Z", "12", "run-1"),
        ("RSSMRA80A01H501Z", "13", "run-1"),
    ]
