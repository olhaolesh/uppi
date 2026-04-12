"""Focused tests for the import-only orchestration boundary."""

from __future__ import annotations

from types import SimpleNamespace

import uppi.pipelines as pipelines_module
import uppi.services.visura_processor as processor_module
from uppi.domain.immobile import Immobile
from uppi.services.failure_registry import FailureRegistryRecorder
from uppi.services.visura_processor import VisuraProcessor
from uppi.services.visura_stages import (
    ContractSyncResult,
    ImmobileSyncResult,
    PersonSyncResult,
    VisuraIngestResult,
)
from uppi.spiders.uppi_import_spider import UppiImportSpider
from uppi.spiders.uppi_spider import UppiSpider


class RecordingFailureStorage:
    """Simple in-memory storage for failure recorder assertions."""

    def __init__(self) -> None:
        self.records = []

    def append(self, record) -> None:
        self.records.append(record)


class RecordingLogger:
    """Minimal logger double for processor boundary tests."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def warning(self, msg, *args) -> None:
        self.records.append(("warning", msg % args if args else msg))

    def exception(self, msg, *args) -> None:
        self.records.append(("exception", msg % args if args else msg))

    def error(self, msg, *args) -> None:
        self.records.append(("error", msg % args if args else msg))


class FakeConnection:
    """Minimal DB double for import/generation orchestration tests."""

    def __init__(self) -> None:
        self.commit_called = False
        self.rollback_called = False
        self.close_called = False

    def commit(self) -> None:
        self.commit_called = True

    def rollback(self) -> None:
        self.rollback_called = True

    def close(self) -> None:
        self.close_called = True


class RecordingPersonSyncService:
    """Record the person sync stage and return a stable result."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def sync(self, conn, adapter, *, run_id, locatore_cf, cond_cf):
        self.calls.append("person_sync")
        return PersonSyncResult(loc_addr_id=51, cond_addr_id=None)


class RecordingVisuraIngestService:
    """Record the visura ingest stage and return a stable result."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def ingest(self, conn, adapter, *, run_id, locatore_cf):
        self.calls.append("visura_ingest")
        return VisuraIngestResult(
            visura_db_id=81,
            fetched_now=True,
            pdf_path=None,
            pdf_to_delete=None,
        )


class RecordingImmobileSyncService:
    """Record the immobile sync stage and return a stable result."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def sync(self, conn, spider, adapter, *, run_id, locatore_cf, loc_addr_id, visura_ingest):
        self.calls.append("immobile_sync")
        return ImmobileSyncResult(keep_ids=[71])


class RecordingContractSyncService:
    """Record the contract sync stage for full pipeline regression tests."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def sync(self, conn, adapter, *, run_id, client_cf, immobile_id):
        self.calls.append("contract_sync")
        return ContractSyncResult(contract_id=91, contract_ctx={"contract": {"id": 91}})


class RecordingCanoneStageService:
    """Record the canone stage for full pipeline regression tests."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def run(self, conn, spider, adapter, *, run_id, locatore_cf, imm, contract_id, contract_ctx):
        self.calls.append("canone_stage")
        return SimpleNamespace(contract_ctx=contract_ctx, canone_result_snapshot=None)


class RecordingDocumentStageService:
    """Record the document stage for full pipeline regression tests."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def run(
        self,
        conn,
        spider,
        adapter,
        *,
        run_id,
        imm,
        contract_ctx,
        contract_id,
        immobile_id,
        locatore_cf,
        canone_result_snapshot,
    ):
        self.calls.append("document_stage")
        return None


class GuardContractSyncService:
    """Fail immediately if import-only orchestration leaks into generation."""

    def sync(self, *args, **kwargs):
        raise AssertionError("contract sync must not run on the import-only boundary")


class GuardCanoneStageService:
    """Fail immediately if import-only orchestration leaks into generation."""

    def run(self, *args, **kwargs):
        raise AssertionError("canone stage must not run on the import-only boundary")


class GuardDocumentStageService:
    """Fail immediately if import-only orchestration leaks into generation."""

    def run(self, *args, **kwargs):
        raise AssertionError("document stage must not run on the import-only boundary")


class GuardAuditStageService:
    """Fail immediately if import-only orchestration leaks into audit logic."""

    def log_generated(self, *args, **kwargs):
        raise AssertionError("audit stage must not run on the import-only boundary")

    def log_failed(self, *args, **kwargs):
        raise AssertionError("audit stage must not run on the import-only boundary")


def _make_processor(connection, recorder, *, calls: list[str], generation_guards: bool = False) -> VisuraProcessor:
    """Build a processor with explicit seams for narrow orchestration tests."""
    contract_sync_service = GuardContractSyncService() if generation_guards else RecordingContractSyncService(calls)
    canone_stage_service = GuardCanoneStageService() if generation_guards else RecordingCanoneStageService(calls)
    document_stage_service = GuardDocumentStageService() if generation_guards else RecordingDocumentStageService(calls)
    audit_stage_service = GuardAuditStageService() if generation_guards else None

    return VisuraProcessor(
        connection_factory=lambda: connection,
        failure_registry_recorder=recorder,
        person_sync_service=RecordingPersonSyncService(calls),
        visura_ingest_service=RecordingVisuraIngestService(calls),
        immobile_sync_service=RecordingImmobileSyncService(calls),
        contract_sync_service=contract_sync_service,
        canone_stage_service=canone_stage_service,
        document_stage_service=document_stage_service,
        audit_stage_service=audit_stage_service,
    )


def test_process_import_item_stops_after_immobile_sync(monkeypatch):
    """The import-only entry point must stop before generation services."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    conn = FakeConnection()
    calls: list[str] = []

    def fail_db_load_immobili(conn, cf):
        raise AssertionError("db_load_immobili must not run on the import-only boundary")

    def fail_filter_immobiles_by_yaml(immobili, adapter):
        raise AssertionError("filter_immobiles_by_yaml must not run on the import-only boundary")

    monkeypatch.setattr(processor_module, "db_load_immobili", fail_db_load_immobili)
    monkeypatch.setattr(processor_module, "filter_immobiles_by_yaml", fail_filter_immobiles_by_yaml)

    processor = _make_processor(conn, recorder, calls=calls, generation_guards=True)
    item = {"run_id": "run-import-001", "locatore_cf": "RSSMRA80A01H501Z"}
    spider = SimpleNamespace(logger=RecordingLogger())

    returned = processor.process_import_item(item, spider)

    assert returned is item
    assert calls == ["person_sync", "visura_ingest", "immobile_sync"]
    assert conn.commit_called is True
    assert conn.rollback_called is False
    assert conn.close_called is True
    assert storage.records == []


def test_process_item_keeps_existing_generation_continuation(monkeypatch):
    """The full processor path must still continue past the import boundary."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    conn = FakeConnection()
    calls: list[str] = []

    def fake_db_load_immobili(conn, cf):
        calls.append("db_load_immobili")
        return [(71, Immobile(foglio="12", numero="345", sub="7"))]

    def fake_filter_immobiles_by_yaml(immobili, adapter):
        calls.append("filter_immobiles_by_yaml")
        return immobili

    monkeypatch.setattr(processor_module, "db_load_immobili", fake_db_load_immobili)
    monkeypatch.setattr(processor_module, "filter_immobiles_by_yaml", fake_filter_immobiles_by_yaml)

    processor = _make_processor(conn, recorder, calls=calls)
    item = {"run_id": "run-full-001", "locatore_cf": "RSSMRA80A01H501Z"}
    spider = SimpleNamespace(logger=RecordingLogger())

    returned = processor.process_item(item, spider)

    assert returned is item
    assert calls == [
        "person_sync",
        "visura_ingest",
        "immobile_sync",
        "db_load_immobili",
        "filter_immobiles_by_yaml",
        "contract_sync",
        "canone_stage",
        "document_stage",
    ]
    assert conn.commit_called is True
    assert conn.rollback_called is False
    assert conn.close_called is True
    assert storage.records == []


def test_import_pipeline_delegates_to_process_import_item(monkeypatch):
    """The import-only pipeline must call the explicit import processor entry point."""
    calls: list[str] = []

    class FakeProcessor:
        """Minimal processor double for pipeline delegation tests."""

        def process_import_item(self, item, spider):
            calls.append("process_import_item")
            return {"result": "import-only"}

        def process_item(self, item, spider):
            raise AssertionError("full process_item must not be used by UppiImportPipeline")

    monkeypatch.setattr(pipelines_module, "configure_uppi_logging", lambda: None)
    monkeypatch.setattr(pipelines_module, "VisuraProcessor", FakeProcessor)

    pipeline = pipelines_module.UppiImportPipeline()

    returned = pipeline.process_item({"locatore_cf": "RSSMRA80A01H501Z"}, object())

    assert returned == {"result": "import-only"}
    assert calls == ["process_import_item"]


def test_default_pipeline_keeps_full_processor_entry_point(monkeypatch):
    """The existing production pipeline must keep using the full processor path."""
    calls: list[str] = []

    class FakeProcessor:
        """Minimal processor double for pipeline delegation tests."""

        def process_import_item(self, item, spider):
            raise AssertionError("import boundary must not replace the default pipeline")

        def process_item(self, item, spider):
            calls.append("process_item")
            return {"result": "full-pipeline"}

    monkeypatch.setattr(pipelines_module, "configure_uppi_logging", lambda: None)
    monkeypatch.setattr(pipelines_module, "VisuraProcessor", FakeProcessor)

    pipeline = pipelines_module.UppiPipeline()

    returned = pipeline.process_item({"locatore_cf": "RSSMRA80A01H501Z"}, object())

    assert returned == {"result": "full-pipeline"}
    assert calls == ["process_item"]


def test_import_spider_uses_import_only_pipeline_boundary():
    """The internal import spider must reuse the browser flow with a different pipeline."""
    assert issubclass(UppiImportSpider, UppiSpider)
    assert UppiImportSpider.name == "uppi_import"
    assert UppiImportSpider.custom_settings == {
        "ITEM_PIPELINES": {
            "uppi.pipelines.UppiImportPipeline": 300,
        },
    }
