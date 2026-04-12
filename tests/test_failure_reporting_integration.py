"""Integration-style тести для stage-level failure reporting."""

from __future__ import annotations

from types import SimpleNamespace

import uppi.services.visura_processor as processor_module
import uppi.services.visura_stages as stage_module
from uppi.domain.failure_registry import FailureStage
from uppi.domain.immobile import Immobile
from uppi.services.failure_registry import FailureRegistryRecorder
from uppi.services.validation.models import ValidationResult
from uppi.services.visura_processor import VisuraProcessor
from uppi.services.visura_stages import (
    CanoneStageService,
    ContractSyncResult,
    DocumentStageService,
    ImmobileSyncResult,
    PersonSyncResult,
    VisuraIngestResult,
)


class RecordingFailureStorage:
    """Простий in-memory storage для перевірки append-only failure reporting."""

    def __init__(self) -> None:
        """Ініціалізує порожній список записів."""
        self.records = []

    def append(self, record) -> None:
        """Запам'ятовує record без side effects."""
        self.records.append(record)

    def list_records(self, *, run_id=None, client_cf=None):
        """Повертає відфільтровані in-memory records."""
        records = list(self.records)
        if run_id is not None:
            records = [record for record in records if record.run_id == run_id]
        if client_cf is not None:
            records = [record for record in records if record.client_cf == client_cf]
        return records


class RecordingLogger:
    """Мінімальний logger-double для перевірки безпечної інтеграції."""

    def __init__(self) -> None:
        """Ініціалізує збирач логів."""
        self.records: list[tuple[str, str]] = []

    def warning(self, msg, *args) -> None:
        """Запам'ятовує warning-log."""
        self.records.append(("warning", msg % args if args else msg))

    def exception(self, msg, *args) -> None:
        """Запам'ятовує exception-log."""
        self.records.append(("exception", msg % args if args else msg))

    def error(self, msg, *args) -> None:
        """Запам'ятовує error-log."""
        self.records.append(("error", msg % args if args else msg))


class FakeConnection:
    """Мінімальний DB-double для processor integration tests."""

    def __init__(self) -> None:
        """Ініціалізує флаги commit/rollback/close."""
        self.commit_called = False
        self.rollback_called = False
        self.close_called = False

    def commit(self) -> None:
        """Фіксує commit без зовнішніх side effects."""
        self.commit_called = True

    def rollback(self) -> None:
        """Фіксує rollback без зовнішніх side effects."""
        self.rollback_called = True

    def close(self) -> None:
        """Фіксує close без зовнішніх side effects."""
        self.close_called = True


class NoopPersonSyncService:
    """Повертає стабільний результат без падіння."""

    def sync(self, conn, adapter, *, run_id, locatore_cf, cond_cf):
        """Імітує успішний person sync."""
        return PersonSyncResult(loc_addr_id=None, cond_addr_id=None)


class NoopVisuraIngestService:
    """Повертає ingest result без фактичного upload path."""

    def ingest(self, conn, adapter, *, run_id, locatore_cf):
        """Імітує відсутність нового PDF без помилки."""
        return VisuraIngestResult(
            visura_db_id=None,
            fetched_now=False,
            pdf_path=None,
            pdf_to_delete=None,
        )


class NoopImmobileSyncService:
    """Імітує успішний immobile sync без нових записів."""

    def sync(self, conn, spider, adapter, *, run_id, locatore_cf, loc_addr_id, visura_ingest):
        """Повертає порожній результат без помилки."""
        return ImmobileSyncResult(keep_ids=[])


class NoopContractSyncService:
    """Імітує успішний contract sync."""

    def sync(self, conn, adapter, *, run_id, client_cf, immobile_id, imm):
        """Повертає мінімальний contract context."""
        return ContractSyncResult(contract_id=81, contract_ctx={"contract": {"id": 81}})


class NoopCanoneStageService:
    """Імітує успішний canone stage без побічних ефектів."""

    def run(self, conn, spider, adapter, *, run_id, locatore_cf, imm, contract_id, contract_ctx):
        """Повертає незмінений context."""
        return SimpleNamespace(contract_ctx=contract_ctx, canone_result_snapshot=None)


class NoopDocumentStageService:
    """Імітує успішний document stage."""

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
        """Нічого не робить і повертає `None`, як безпечний no-op."""
        return None


def _make_processor(monkeypatch, recorder, connection, **overrides):
    """Створює `VisuraProcessor` з injected seams для вузьких integration tests."""
    monkeypatch.setattr(
        processor_module,
        "db_load_immobile_by_identity",
        lambda conn, owner_cf, foglio, numero, sub: (71, Immobile(foglio=foglio, numero=numero, sub=sub)),
    )
    return VisuraProcessor(
        connection_factory=lambda: connection,
        failure_registry_recorder=recorder,
        person_sync_service=overrides.get("person_sync_service", NoopPersonSyncService()),
        visura_ingest_service=overrides.get("visura_ingest_service", NoopVisuraIngestService()),
        immobile_sync_service=overrides.get("immobile_sync_service", NoopImmobileSyncService()),
        contract_sync_service=overrides.get("contract_sync_service", NoopContractSyncService()),
        canone_stage_service=overrides.get("canone_stage_service", NoopCanoneStageService()),
        document_stage_service=overrides.get("document_stage_service", NoopDocumentStageService()),
    )


def test_processor_records_stage_failure_for_person_sync_without_pipeline_duplicate(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    conn = FakeConnection()

    def failing_upsert_person(conn, cf, *, surname, name, address_id):
        """Імітує stage-level failure під час upsert персони."""
        raise RuntimeError("token=abc123 person sync failed")

    monkeypatch.setattr(stage_module, "db_upsert_person", failing_upsert_person)
    processor = _make_processor(
        monkeypatch,
        recorder,
        conn,
        person_sync_service=stage_module.PersonSyncService(failure_recorder=recorder),
    )

    item = {"run_id": "run-001", "locatore_cf": "RSSMRA80A01H501Z"}
    spider = SimpleNamespace(logger=RecordingLogger())

    returned = processor.process_import_item(item, spider)

    assert returned is item
    assert conn.rollback_called is True
    assert conn.close_called is True
    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.PERSON_SYNC
    assert storage.records[0].run_id == "run-001"
    assert storage.records[0].client_cf == "RSSMRA80A01H501Z"
    assert storage.records[0].retryable is False
    assert "<token:redacted>" in storage.records[0].message_redacted


def test_processor_records_pipeline_fatal_for_non_stage_failure(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    conn = FakeConnection()
    processor = _make_processor(monkeypatch, recorder, conn)
    monkeypatch.setattr(
        processor_module,
        "db_load_immobile_by_identity",
        lambda conn, owner_cf, foglio, numero, sub: (_ for _ in ()).throw(RuntimeError("session=abc load failed")),
    )

    item = {
        "run_id": "run-002",
        "locatore_cf": "RSSMRA80A01H501Z",
        "foglio": "12",
        "numero": "345",
        "sub": "7",
    }
    spider = SimpleNamespace(logger=RecordingLogger())

    returned = processor.process_generation_item(item, spider)

    assert returned is item
    assert conn.rollback_called is True
    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.PIPELINE_FATAL
    assert storage.records[0].run_id == "run-002"
    assert "<session:redacted>" in storage.records[0].message_redacted


def test_processor_success_path_does_not_create_failure_records(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    conn = FakeConnection()
    processor = _make_processor(monkeypatch, recorder, conn)

    item = {
        "run_id": "run-003",
        "locatore_cf": "RSSMRA80A01H501Z",
        "foglio": "12",
        "numero": "345",
        "sub": "7",
    }
    spider = SimpleNamespace(logger=RecordingLogger())

    returned = processor.process_generation_item(item, spider)

    assert returned is item
    assert conn.commit_called is True
    assert conn.rollback_called is False
    assert conn.close_called is True
    assert storage.records == []


def test_visura_ingest_stage_records_retryable_infra_failure(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    pdf_path = tmp_path / "DOC_TEST.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")

    class FailingStorageService:
        """Імітує transient upload failure без зміни ingest semantics."""

        def upload_file(self, *args, **kwargs):
            """Кидає transient infra-like помилку для перевірки retry flag."""
            raise ConnectionError("temporary network timeout")

    stage = stage_module.VisuraIngestService(
        storage=processor_module.ObjectStorage(),
        storage_service=FailingStorageService(),
        pdf_lookup=lambda cf, adapter: pdf_path,
        sha256_file_fn=lambda path: "deadbeef",
        failure_recorder=recorder,
    )

    try:
        stage.ingest(
            object(),
            {"visura_source": "sister", "visura_downloaded": True},
            run_id="run-003a",
            locatore_cf="RSSMRA80A01H501Z",
        )
    except ConnectionError:
        pass
    else:
        raise AssertionError("ConnectionError expected")

    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.VISURA_INGEST
    assert storage.records[0].retryable is True
    assert any(ref.kind == "local_visura_pdf" for ref in storage.records[0].artifact_refs)


def test_canone_stage_records_non_retryable_validation_failure_without_raising(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    result = ValidationResult()
    result.add_error("broken_canone", "token=abc123 broken canone input")

    monkeypatch.setattr(stage_module, "validate_canone_input", lambda candidate: result)

    stage = CanoneStageService(failure_recorder=recorder)
    returned = stage.run(
        object(),
        SimpleNamespace(logger=RecordingLogger()),
        {"contract_kind": "CONCORDATO"},
        run_id="run-004",
        locatore_cf="RSSMRA80A01H501Z",
        imm=Immobile(superficie_totale=80.0, micro_zona="1", foglio="12", categoria="A/2", classe="3"),
        contract_id=81,
        contract_ctx={"elements": {}, "contract": {}, "immobile": {}},
    )

    assert returned.contract_ctx == {"elements": {}, "contract": {}, "immobile": {}}
    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.CANONE_STAGE
    assert storage.records[0].retryable is False
    assert storage.records[0].artifact_refs[0].ref == "81"
    assert "<token:redacted>" in storage.records[0].message_redacted


def test_document_stage_records_safe_failure_record_and_calls_failed_audit(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)
    audit_calls: list[tuple] = []

    class RecordingAuditStage:
        """Фіксує failed-audit call без додаткових side effects."""

        def log_generated(self, *args, **kwargs):
            """Не має викликатися на current document failure path."""
            raise AssertionError("log_generated should not be called")

        def log_failed(self, conn, contract_id, error, *, run_id, client_cf):
            """Фіксує failed-audit call для подальших assert-перевірок."""
            audit_calls.append((contract_id, str(error), run_id, client_cf))

    monkeypatch.setattr(stage_module, "build_template_params", lambda adapter, imm, contract_ctx: {"LOCATORE_CF": "RSSMRA80A01H501Z"})
    monkeypatch.setattr(
        stage_module,
        "get_attestazione_path",
        lambda cf, contract_id, imm: tmp_path / "attestazioni" / "RSSMRA80A01H501Z_91.docx",
    )

    def fake_fill_template(**kwargs):
        """Імітує шаблонну помилку з чутливими фрагментами для redaction test."""
        raise RuntimeError("token=abc123 password=secret fill failed")

    monkeypatch.setattr(stage_module, "fill_attestazione_template", fake_fill_template)

    stage = DocumentStageService(
        storage=processor_module.ObjectStorage(),
        storage_service=SimpleNamespace(upload_file=lambda *args, **kwargs: None),
        runtime_config=processor_module.default_visura_processor_runtime_config(template_path=tmp_path / "template.docx"),
        template_path=tmp_path / "template.docx",
        audit_stage=RecordingAuditStage(),
        failure_recorder=recorder,
    )

    returned = stage.run(
        object(),
        SimpleNamespace(logger=RecordingLogger()),
        {"LOCATORE_CF": "RSSMRA80A01H501Z"},
        run_id="run-005",
        imm=Immobile(foglio="12", numero="345", sub="7"),
        contract_ctx={"contract": {"id": 91}, "immobile": {}},
        contract_id=91,
        immobile_id=31,
        locatore_cf="RSSMRA80A01H501Z",
        canone_result_snapshot=None,
    )

    assert returned is None
    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.DOCUMENT_STAGE
    assert storage.records[0].retryable is False
    assert "<token:redacted>" in storage.records[0].message_redacted
    assert "<secret:redacted>" in storage.records[0].message_redacted
    assert any(ref.kind == "storage_object" for ref in storage.records[0].artifact_refs)
    assert audit_calls == [(91, "token=abc123 password=secret fill failed", "run-005", "RSSMRA80A01H501Z")]


def test_audit_stage_records_failure_when_audit_write_itself_breaks(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage)

    def fake_insert_attestazione_log(*args, **kwargs):
        """Імітує помилку запису audit row після document failure."""
        raise RuntimeError("token=abc123 audit failed")

    monkeypatch.setattr(stage_module, "db_insert_attestazione_log", fake_insert_attestazione_log)

    audit = stage_module.AuditStageService(
        runtime_config=processor_module.default_visura_processor_runtime_config(template_path=tmp_path / "template.docx"),
        failure_recorder=recorder,
    )

    try:
        audit.log_failed(
            object(),
            91,
            RuntimeError("docx failed"),
            run_id="run-006",
            client_cf="RSSMRA80A01H501Z",
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("AuditStageService.log_failed should re-raise current audit failure")

    assert len(storage.records) == 1
    assert storage.records[0].stage is FailureStage.AUDIT_STAGE
    assert storage.records[0].retryable is False
    assert "<token:redacted>" in storage.records[0].message_redacted
    assert storage.records[0].artifact_refs[1].ref == "failed"
