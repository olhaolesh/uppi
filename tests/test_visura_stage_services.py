"""Service-level тести для extracted stage boundaries `VisuraProcessor`."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from itemadapter import ItemAdapter

import uppi.services.visura_stages as stage_module
from uppi.config.app_config import VisuraProcessorRuntimeConfig
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage, ObjectStorageConfig
from uppi.services.visura_processor import default_visura_processor_runtime_config
from uppi.services.visura_stages import (
    AuditStageService,
    CanoneStageService,
    ContractSyncService,
    DocumentStageService,
    ImmobileSyncService,
    PersonSyncService,
    VisuraIngestResult,
    VisuraIngestService,
)


class RecordingLogger:
    """Мінімальний logger-double для перевірки stage service behavior."""

    def __init__(self) -> None:
        """Ініціалізує тестовий збирач логів."""
        self.records: list[tuple[str, str]] = []

    def warning(self, msg, *args) -> None:
        """Запам'ятовує warning без side effects."""
        self.records.append(("warning", msg % args if args else msg))

    def exception(self, msg, *args) -> None:
        """Запам'ятовує exception-log без side effects."""
        self.records.append(("exception", msg % args if args else msg))

    def error(self, msg, *args) -> None:
        """Запам'ятовує error-log без side effects."""
        self.records.append(("error", msg % args if args else msg))


def _make_storage() -> ObjectStorage:
    """Створює стабільний storage-адаптер для stage-level тестів."""
    return ObjectStorage(
        ObjectStorageConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
            visure_bucket="visure-bucket",
            attestazioni_bucket="attestazioni-bucket",
        )
    )


def _make_runtime_config(tmp_path: Path) -> VisuraProcessorRuntimeConfig:
    """Повертає runtime config із тимчасовим template path."""
    return default_visura_processor_runtime_config(template_path=tmp_path / "template.docx")


def test_person_sync_service_preserves_current_locatore_and_conduttore_order(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []

    def fake_upsert_address(conn, payload):
        calls.append(("address", payload["comune"], payload["via_full"]))
        return 10 + len([c for c in calls if c[0] == "address"])

    def fake_upsert_person(conn, cf, *, surname, name, address_id):
        calls.append(("person", cf, surname, name, address_id))

    monkeypatch.setattr(stage_module, "db_upsert_address", fake_upsert_address)
    monkeypatch.setattr(stage_module, "db_upsert_person", fake_upsert_person)

    adapter = ItemAdapter(
        {
            "locatore_comune_res": "Pescara",
            "locatore_via": "Via Roma",
            "locatore_civico": "10",
            "locatore_surname": "Rossi",
            "locatore_name": "Mario",
            "conduttore_comune": "Pescara",
            "conduttore_via": "Via Test 12",
            "conduttore_nome": "Mario Bianchi",
        }
    )

    result = PersonSyncService().sync(
        object(),
        adapter,
        locatore_cf="RSSMRA80A01H501Z",
        cond_cf="BNCMRA80A01H501Z",
    )

    assert result.loc_addr_id == 11
    assert result.cond_addr_id == 12
    assert calls == [
        ("address", "Pescara", "Via Roma"),
        ("person", "RSSMRA80A01H501Z", "Rossi", "Mario", 11),
        ("address", "Pescara", "Via Test 12"),
        ("person", "BNCMRA80A01H501Z", "Mario", "Bianchi", 12),
    ]


def test_visura_ingest_service_preserves_current_upload_and_registration_contract(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    pdf_path = tmp_path / "DOC_TEST.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")
    calls: list[tuple] = []

    class RecordingStorageService:
        """Фіксує upload calls для assert-перевірок."""

        def upload_file(self, bucket, object_name, path, content_type):
            calls.append(("upload", bucket, object_name, Path(path), content_type))

    def fake_lookup(cf, adapter):
        calls.append(("lookup", cf))
        return pdf_path

    def fake_sha256(path):
        calls.append(("sha256", Path(path)))
        return "deadbeef"

    def fake_db_upsert_visura(conn, cf, bucket, obj_name, checksum, *, fetched_now):
        calls.append(("db_upsert_visura", cf, bucket, obj_name, checksum, fetched_now))
        return 41

    monkeypatch.setattr(stage_module, "db_upsert_visura", fake_db_upsert_visura)

    service = VisuraIngestService(
        storage=_make_storage(),
        storage_service=RecordingStorageService(),
        pdf_lookup=fake_lookup,
        sha256_file_fn=fake_sha256,
    )

    result = service.ingest(
        object(),
        ItemAdapter({"visura_source": "sister", "visura_downloaded": True}),
        locatore_cf="RSSMRA80A01H501Z",
    )

    assert result.visura_db_id == 41
    assert result.fetched_now is True
    assert result.pdf_path == pdf_path
    assert result.pdf_to_delete == pdf_path
    assert calls == [
        ("lookup", "RSSMRA80A01H501Z"),
        ("sha256", pdf_path),
        ("upload", "visure-bucket", "visure/RSSMRA80A01H501Z.pdf", pdf_path, "application/pdf"),
        ("db_upsert_visura", "RSSMRA80A01H501Z", "visure-bucket", "visure/RSSMRA80A01H501Z.pdf", "deadbeef", True),
    ]


def test_immobile_sync_service_preserves_current_parse_person_and_prune_order(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []
    parsed_rows = [
        {
            "locatore_name": "Mario",
            "locatore_surname": "Rossi",
            "immobile_comune": "PESCARA",
            "via_name": "VIA ROMA",
            "indirizzo_raw": "VIA ROMA 10",
            "via_num": "10",
            "piano": "3",
            "interno": "4",
            "scala": "A",
            "foglio": "12",
            "numero": "345",
            "sub": "7",
        }
    ]

    class FakeParser:
        """Повертає контрольований parsed output."""

        def parse(self, path):
            calls.append(("parse", Path(path)))
            return parsed_rows

    def fake_upsert_person(conn, cf, *, surname, name, address_id):
        calls.append(("person", cf, surname, name, address_id))

    def fake_upsert_address(conn, payload):
        calls.append(("address", payload["comune"], payload["via_full"]))
        return 21

    def fake_immobile_from_parsed_dict(parsed):
        calls.append(("immobile_from_parsed_dict", parsed["foglio"], parsed["numero"], parsed["sub"]))
        return Immobile(foglio="12", numero="345", sub="7")

    def fake_upsert_immobile(conn, cf, imm, *, visura_addr_id, source_visura_id):
        calls.append(("immobile", cf, visura_addr_id, source_visura_id, imm.foglio, imm.numero, imm.sub))
        return 31

    def fake_prune(conn, cf, keep_ids, enabled):
        calls.append(("prune", cf, keep_ids, enabled))

    monkeypatch.setattr(stage_module, "db_upsert_person", fake_upsert_person)
    monkeypatch.setattr(stage_module, "db_upsert_address", fake_upsert_address)
    monkeypatch.setattr(stage_module, "immobile_from_parsed_dict", fake_immobile_from_parsed_dict)
    monkeypatch.setattr(stage_module, "db_upsert_immobile", fake_upsert_immobile)
    monkeypatch.setattr(stage_module, "db_prune_old_immobili_without_contracts", fake_prune)

    service = ImmobileSyncService(
        parser_factory=FakeParser,
        prune_old_immobili_without_contracts=True,
    )

    result = service.sync(
        object(),
        SimpleNamespace(logger=RecordingLogger()),
        ItemAdapter({}),
        locatore_cf="RSSMRA80A01H501Z",
        loc_addr_id=11,
        visura_ingest=VisuraIngestResult(
            visura_db_id=41,
            fetched_now=True,
            pdf_path=tmp_path / "DOC_TEST.pdf",
            pdf_to_delete=tmp_path / "DOC_TEST.pdf",
        ),
    )

    assert result.keep_ids == [31]
    assert calls == [
        ("parse", tmp_path / "DOC_TEST.pdf"),
        ("person", "RSSMRA80A01H501Z", "Rossi", "Mario", 11),
        ("address", "PESCARA", "VIA ROMA"),
        ("immobile_from_parsed_dict", "12", "345", "7"),
        ("immobile", "RSSMRA80A01H501Z", 21, 41, "12", "345", "7"),
        ("prune", "RSSMRA80A01H501Z", [31], True),
    ]


def test_contract_sync_service_preserves_current_real_address_elements_contract_chain(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []

    def fake_upsert_address(conn, payload):
        calls.append(("address", payload["comune"], payload["via_full"], payload["civico"]))
        return 51

    def fake_update_real_address(conn, immobile_id, *, real_address_id, energy_class):
        calls.append(("update_real_address", immobile_id, real_address_id, energy_class))

    def fake_upsert_elements(conn, immobile_id, adapter):
        calls.append(("elements", immobile_id))

    def fake_upsert_contract(conn, immobile_id, adapter):
        calls.append(("contract", immobile_id))
        return 61

    def fake_load_contract_context(conn, contract_id):
        calls.append(("context", contract_id))
        return {"contract": {"id": contract_id}}

    monkeypatch.setattr(stage_module, "db_upsert_address", fake_upsert_address)
    monkeypatch.setattr(stage_module, "db_update_immobile_real_address", fake_update_real_address)
    monkeypatch.setattr(stage_module, "db_upsert_immobile_elements", fake_upsert_elements)
    monkeypatch.setattr(stage_module, "db_upsert_contract", fake_upsert_contract)
    monkeypatch.setattr(stage_module, "db_load_contract_context", fake_load_contract_context)

    adapter = ItemAdapter(
        {
            "immobile_comune": "Pescara",
            "immobile_via": "Corso Roma",
            "immobile_civico": "12",
            "immobile_piano": "3",
            "immobile_interno": "4",
            "energy_class": "B",
        }
    )

    result = ContractSyncService().sync(object(), adapter, immobile_id=71)

    assert result.contract_id == 61
    assert result.contract_ctx == {"contract": {"id": 61}}
    assert calls == [
        ("address", "Pescara", "Corso Roma", "12"),
        ("update_real_address", 71, 51, "B"),
        ("elements", 71),
        ("contract", 71),
        ("context", 61),
    ]


def test_canone_stage_service_preserves_current_insert_then_reload_contract_context(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []

    def fake_compute_base_canone(can_in):
        calls.append(("compute", can_in.contract_kind.name, can_in.energy_class, can_in.durata_anni))
        return SimpleNamespace(canone_finale_mensile=650.0, marker="ok")

    def fake_insert_canone_calc(conn, contract_id, strategy, *, inputs, result_mensile):
        calls.append(("insert_calc", contract_id, strategy, result_mensile, inputs["canone_input"]["durata_anni"]))

    def fake_load_contract_context(conn, contract_id):
        calls.append(("reload_context", contract_id))
        return {"contract": {"id": contract_id}, "elements": {"a1": "X"}, "immobile": {"energy_class": "B"}}

    monkeypatch.setattr(stage_module, "compute_base_canone", fake_compute_base_canone)
    monkeypatch.setattr(stage_module, "db_insert_canone_calc", fake_insert_canone_calc)
    monkeypatch.setattr(stage_module, "db_load_contract_context", fake_load_contract_context)

    result = CanoneStageService().run(
        object(),
        SimpleNamespace(logger=RecordingLogger()),
        ItemAdapter(
            {
                "contract_kind": "TRANSITORIO",
                "energy_class": "B",
                "durata_anni": "4",
                "arredato": "0.1",
                "istat": "5",
                "ignore_surcharges": "yes",
            }
        ),
        imm=Immobile(
            superficie_totale=80.0,
            micro_zona="1",
            foglio="12",
            categoria="A/2",
            classe="3",
        ),
        contract_id=81,
        contract_ctx={
            "elements": {"a1": "X", "b2": "X"},
            "contract": {
                "istat_rate": 2.0,
                "durata_anni": 3,
                "arredato_pct": 0.0,
                "ignore_surcharges": False,
            },
            "immobile": {"energy_class": "C"},
        },
    )

    assert result.contract_ctx["contract"]["id"] == 81
    assert result.canone_snapshot["durata_anni"] == 4
    assert result.canone_result_snapshot["canone_finale_mensile"] == 650.0
    assert calls == [
        ("compute", "TRANSITORIO", "B", 4),
        ("insert_calc", 81, "pescara2018_base", 650.0, 4),
        ("reload_context", 81),
    ]


def test_document_stage_service_preserves_current_generate_upload_audit_order(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []

    class RecordingStorageService:
        """Фіксує upload calls для assert-перевірок."""

        def upload_file(self, bucket, object_name, path, content_type):
            calls.append(("upload", bucket, object_name, Path(path).name, content_type))

    class RecordingAuditStage:
        """Фіксує audit calls для assert-перевірок."""

        def log_generated(self, conn, contract_id, out_bucket, out_obj, params_snapshot):
            calls.append(("audit_generated", contract_id, out_bucket, out_obj, params_snapshot["template_version"]))

        def log_failed(self, conn, contract_id, error):
            calls.append(("audit_failed", contract_id, str(error)))

    output_path = tmp_path / "attestazioni" / "doc.docx"

    def fake_build_template_params(adapter, imm, contract_ctx):
        calls.append(("build_params", contract_ctx["contract"]["id"]))
        return {"LOCATORE_CF": "RSSMRA80A01H501Z"}

    def fake_get_attestazione_path(cf, contract_id, imm):
        calls.append(("resolve_path", cf, contract_id))
        return output_path

    def fake_fill_template(*, template_path, output_folder, filename, params, underscored):
        calls.append(("fill_template", Path(template_path).name, Path(output_folder), filename, params["LOCATORE_CF"]))
        return output_path

    monkeypatch.setattr(stage_module, "build_template_params", fake_build_template_params)
    monkeypatch.setattr(stage_module, "get_attestazione_path", fake_get_attestazione_path)
    monkeypatch.setattr(stage_module, "fill_attestazione_template", fake_fill_template)

    stage = DocumentStageService(
        storage=_make_storage(),
        storage_service=RecordingStorageService(),
        runtime_config=_make_runtime_config(tmp_path),
        template_path=tmp_path / "template.docx",
        audit_stage=RecordingAuditStage(),
    )

    result = stage.run(
        object(),
        SimpleNamespace(logger=RecordingLogger()),
        ItemAdapter({"LOCATORE_CF": "RSSMRA80A01H501Z"}),
        imm=Immobile(foglio="12", numero="345", sub="7"),
        contract_ctx={"contract": {"id": 91}, "immobile": {"energy_class": "B"}},
        contract_id=91,
        immobile_id=31,
        locatore_cf="RSSMRA80A01H501Z",
        canone_result_snapshot={"canone_finale_mensile": 650.0},
    )

    assert result == output_path
    assert calls == [
        ("build_params", 91),
        ("resolve_path", "RSSMRA80A01H501Z", 91),
        ("fill_template", "template.docx", output_path.parent, output_path.name, "RSSMRA80A01H501Z"),
        (
            "upload",
            "attestazioni-bucket",
            "attestazioni/RSSMRA80A01H501Z/91.docx",
            "doc.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        ("audit_generated", 91, "attestazioni-bucket", "attestazioni/RSSMRA80A01H501Z/91.docx", "pescara2018_v2"),
    ]


def test_audit_stage_service_preserves_current_generated_and_failed_log_shape(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls: list[tuple] = []

    def fake_insert_attestazione_log(
        conn,
        contract_id,
        status,
        bucket,
        object_name,
        params_snapshot=None,
        error=None,
        author_login_masked=None,
        author_login_sha256=None,
        template_version=None,
    ):
        calls.append((status, contract_id, bucket, object_name, error, author_login_masked, bool(author_login_sha256), template_version, params_snapshot))

    monkeypatch.setattr(stage_module, "db_insert_attestazione_log", fake_insert_attestazione_log)

    audit = AuditStageService(runtime_config=_make_runtime_config(tmp_path))
    audit.log_generated(object(), 101, "bucket-a", "obj-a", {"template_version": "pescara2018_v2"})
    audit.log_failed(object(), 102, RuntimeError("boom"))

    assert calls[0][0:4] == ("generated", 101, "bucket-a", "obj-a")
    assert calls[1][0:4] == ("failed", 102, "", "")
    assert calls[1][4] == "boom"
