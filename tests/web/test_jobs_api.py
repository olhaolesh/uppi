"""Tests for the Stage 9 protected jobs API and job recording hooks."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.domain.exceptions import ImportOnlyRunnerFailedError, PrepareImportFailedError
from uppi.services.generation_runner import GenerationArtifactRef
from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig
from uppi.web.services.bulk_import_adapter import (
    BulkImportWebInvalidRow,
    BulkImportWebResult,
    BulkImportWebRowResult,
)
from uppi.web.services.generation_adapter import GeneratedRunResult
from uppi.web.services.job_registry import JobRegistry
from uppi.web.services.prepare_adapter import PreparedSearchResult


OWNER_CF = "RSSMRA80A01H501Z"


def _make_auth_config() -> WebAppConfig:
    return WebAppConfig(
        app_name="UPPI API",
        app_version="0.1.0",
        environment="test",
        debug=False,
        auth=WebAuthConfig(
            username="operator",
            password="secret-password",
            pin="1234",
        ),
        session=WebSessionConfig(
            secret="test-session-secret",
            cookie_name="uppi_web_session_test",
            cookie_secure=False,
            max_age_seconds=1800,
        ),
    )


def _utc(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _search_document() -> ImmobiliDocumentConfig:
    return ImmobiliDocumentConfig.from_raw(
        {
            "LOCATORE_CF": OWNER_CF,
            "COMUNE": "PESCARA",
            "TIPO_CATASTO": "F",
            "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
            "LOCATORE_COMUNE_RES": "",
            "LOCATORE_VIA": "",
            "LOCATORE_CIVICO": "",
            "immobili": [
                {
                    "enabled": True,
                    "FOGLIO": "12",
                    "NUMERO": "345",
                    "SUB": "7",
                    "RENDITA": "EUR 123.45",
                    "SUPERFICIE_TOTALE": 98.7,
                    "CATEGORIA": "A/2",
                    "VISURA_COMUNE": "PESCARA",
                    "VISURA_VIA": "VIA ROMA",
                    "VISURA_CIVICO": "10",
                    "IMMOBILE_COMUNE": "",
                    "IMMOBILE_VIA": "",
                    "IMMOBILE_CIVICO": "",
                    "IMMOBILE_PIANO": "",
                    "IMMOBILE_INTERNO": "",
                    "ENERGY_CLASS": "",
                    "ARREDATO": "",
                    "ISTAT": "",
                    "IGNORE_SURCHARGES": "",
                    "CONTRACT_KIND": "",
                    "CONDUTTORE_NOME": "",
                    "CONDUTTORE_CF": "",
                    "CONDUTTORE_COMUNE": "",
                    "CONDUTTORE_VIA": "",
                    "CONTRATTO_DATA": "",
                    "DECORRENZA_DATA": "",
                    "REGISTRAZIONE_DATA": "",
                    "REGISTRAZIONE_NUM": "",
                    "AGENZIA_ENTRATE_SEDE": "",
                    "CANONE_CONTRATTUALE_MENSILE": "",
                    "DURATA_ANNI": "",
                    "A1": "",
                    "B1": "",
                    "C1": "",
                    "D1": "",
                }
            ],
        }
    )


def _prepared_search_result() -> PreparedSearchResult:
    return PreparedSearchResult(
        locatore_cf=OWNER_CF,
        output_path=Path("/tmp/immobili.yml"),
        output_path_relative=f"clients/web_prepare/{OWNER_CF}/immobili.yml",
        decision="db_hit_no_force",
        db_hit_before_import=True,
        import_performed=False,
        source="db",
        document=_search_document(),
    )


def _generated_result() -> GeneratedRunResult:
    return GeneratedRunResult(
        run_id="placeholder-run-id",
        locatore_cf=OWNER_CF,
        prepared_output_path_relative=f"clients/web_prepare/{OWNER_CF}/immobili.yml",
        generation_output_path_relative=(
            f"clients/web_generation/{OWNER_CF}/placeholder-run-id/immobili.yml"
        ),
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
                local_path=f"downloads/{OWNER_CF}/ATTESTAZIONE_{OWNER_CF}_81_F12_N345_S7.docx",
                bucket="attestazioni",
                object_key=f"attestazioni/{OWNER_CF}/81.docx",
            ),
        ),
        messages=("Generated synchronously.",),
    )


def _bulk_result(status: str = "completed") -> BulkImportWebResult:
    return BulkImportWebResult(
        status=status,
        run_id="placeholder-run-id",
        clients_csv_path=Path("/tmp/clients.csv"),
        clients_csv_path_relative="clients/web_bulk_import/placeholder-run-id/clients.csv",
        force_update_visura=False,
        fail_fast=False,
        total_rows=3,
        valid_rows=2,
        invalid_rows_count=1,
        unique_clients=2,
        imported_count=2,
        failed_count=0,
        skipped_count=0,
        results=(
            BulkImportWebRowResult(
                row_number=2,
                locatore_cf=OWNER_CF,
                status="imported",
                message="Imported successfully",
            ),
        ),
        invalid_rows=(
            BulkImportWebInvalidRow(
                row_number=4,
                code="missing_locatore_cf",
                message="clients.csv row 4 is missing LOCATORE_CF",
            ),
        ),
        messages=(
            ("Bulk import aborted after the first import-only failure because fail_fast=true.",)
            if status == "aborted"
            else ()
        ),
    )


class _FakePrepareAdapter:
    def __init__(self, *, result: PreparedSearchResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error

    def prepare_search(self, locatore_cf: str, *, force_update_visura: bool = False) -> PreparedSearchResult:
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Missing fake prepare result")
        return self.result


class _FakeGenerationAdapter:
    def __init__(self, *, result: GeneratedRunResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error

    def generate(self, payload, *, run_id: str | None = None) -> GeneratedRunResult:
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Missing fake generation result")
        resolved_run_id = run_id or self.result.run_id
        return replace(
            self.result,
            run_id=resolved_run_id,
            generation_output_path_relative=(
                f"clients/web_generation/{OWNER_CF}/{resolved_run_id}/immobili.yml"
            ),
        )


class _FakeBulkImportAdapter:
    def __init__(self, *, result: BulkImportWebResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error

    def import_clients(self, payload, *, run_id: str | None = None) -> BulkImportWebResult:
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Missing fake bulk result")
        resolved_run_id = run_id or self.result.run_id
        return replace(
            self.result,
            run_id=resolved_run_id,
            clients_csv_path_relative=f"clients/web_bulk_import/{resolved_run_id}/clients.csv",
        )


def _make_registry(tmp_path: Path, *, run_ids: list[str] | None = None) -> JobRegistry:
    timestamps = iter(
        [
            _utc(2026, 5, 2, 12, 0, 0),
            _utc(2026, 5, 2, 12, 0, 10),
            _utc(2026, 5, 2, 12, 1, 0),
            _utc(2026, 5, 2, 12, 1, 10),
            _utc(2026, 5, 2, 12, 2, 0),
            _utc(2026, 5, 2, 12, 2, 10),
            _utc(2026, 5, 2, 12, 3, 0),
            _utc(2026, 5, 2, 12, 3, 10),
            _utc(2026, 5, 2, 12, 4, 0),
            _utc(2026, 5, 2, 12, 4, 10),
        ]
    )
    run_id_factory = iter(run_ids or ["job-1", "job-2", "job-3", "job-4"]).__next__
    return JobRegistry(
        storage_path=tmp_path / "clients" / "web_jobs" / "jobs.json",
        now_factory=lambda: next(timestamps),
        run_id_factory=run_id_factory,
    )


def _make_client(
    *,
    tmp_path: Path,
    registry: JobRegistry | None = None,
    prepare_adapter: _FakePrepareAdapter | None = None,
    generation_adapter: _FakeGenerationAdapter | None = None,
    bulk_import_adapter: _FakeBulkImportAdapter | None = None,
) -> tuple[TestClient, JobRegistry]:
    resolved_registry = registry or _make_registry(tmp_path)
    client = TestClient(
        create_app(
            _make_auth_config(),
            prepare_search_adapter=prepare_adapter,
            generation_adapter=generation_adapter,
            bulk_import_adapter=bulk_import_adapter,
            job_registry=resolved_registry,
        )
    )
    return client, resolved_registry


def _login(client: TestClient) -> None:
    response = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )
    assert response.status_code == 200


def _valid_generate_payload() -> dict:
    return {
        "locatore_cf": OWNER_CF,
        "prepared_immobili_yaml_path": f"clients/web_prepare/{OWNER_CF}/immobili.yml",
        "client_updates": {
            "locatore_comune_res": "PESCARA",
            "locatore_via": "VIA ROMA",
            "locatore_civico": "10",
        },
        "immobili": [
            {
                "index": 1,
                "enabled": True,
                "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                "editable": {"contract_kind": "ordinario"},
                "run_only": {"conduttore_nome": "Mario Rossi"},
                "elements": {"a1": "X"},
            }
        ],
    }


def test_jobs_endpoints_require_active_session(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        prepare_adapter=_FakePrepareAdapter(result=_prepared_search_result()),
        generation_adapter=_FakeGenerationAdapter(result=_generated_result()),
        bulk_import_adapter=_FakeBulkImportAdapter(result=_bulk_result()),
    )

    assert client.get("/jobs").status_code == 401
    assert client.get("/jobs/job-1").status_code == 401


def test_jobs_api_lists_and_filters_newest_first_and_returns_detail(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    registry = _make_registry(tmp_path, run_ids=["run-1", "run-2", "run-3"])
    first = registry.start_job(
        job_type="attestazioni_search",
        actor_username="operator",
        input_metadata={"locatore_cf": OWNER_CF},
        started_message="Search started",
    )
    registry.complete_job(first.run_id, summary={"immobili_count": 1}, completion_message="Search completed")
    second = registry.start_job(
        job_type="clients_bulk_import",
        actor_username="operator",
        input_metadata={"fail_fast": True},
        started_message="Bulk import started",
    )
    registry.complete_job(
        second.run_id,
        status="aborted",
        summary={"failed_count": 1},
        completion_message="Bulk import aborted",
        event_level="warning",
    )
    third = registry.start_job(
        job_type="attestazioni_generate",
        actor_username="operator",
        input_metadata={"locatore_cf": OWNER_CF},
        started_message="Generation started",
    )
    registry.fail_job(third.run_id, safe_message="Generation failed before any artifact could be produced.")

    client, _ = _make_client(tmp_path=tmp_path, registry=registry)
    _login(client)

    listing = client.get("/jobs")
    assert listing.status_code == 200
    assert [job["run_id"] for job in listing.json()["jobs"]] == ["run-3", "run-2", "run-1"]

    limited = client.get("/jobs", params={"limit": 1})
    assert limited.json()["jobs"] == [listing.json()["jobs"][0]]

    filtered = client.get("/jobs", params={"type": "clients_bulk_import", "status": "aborted"})
    assert [job["run_id"] for job in filtered.json()["jobs"]] == ["run-2"]

    detail = client.get("/jobs/run-2")
    assert detail.status_code == 200
    assert detail.json()["status"] == "aborted"

    missing = client.get("/jobs/unknown-run")
    assert missing.status_code == 404
    assert missing.json() == {"detail": "Requested job run was not found."}


def test_attestazioni_search_records_completed_job_with_safe_artifact_and_metadata(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        prepare_adapter=_FakePrepareAdapter(result=_prepared_search_result()),
    )
    _login(client)

    response = client.post("/attestazioni/search", json={"locatore_cf": OWNER_CF})

    assert response.status_code == 200
    jobs = client.get("/jobs").json()["jobs"]
    assert jobs[0]["type"] == "attestazioni_search"
    detail = client.get(f"/jobs/{jobs[0]['run_id']}")
    payload = detail.json()
    assert payload["status"] == "completed"
    assert payload["input"] == {
        "locatore_cf": OWNER_CF,
        "force_update_visura": False,
    }
    assert payload["summary"] == {
        "immobili_count": 1,
        "active_count": 1,
        "source": "db",
    }
    assert payload["artifacts"] == [
        {
            "kind": "prepared_immobili_yaml",
            "label": "Prepared immobili.yml",
            "local_path": f"clients/web_prepare/{OWNER_CF}/immobili.yml",
            "bucket": None,
            "object_key": None,
            "download_url": None,
        }
    ]
    assert payload["events"][0]["message"] == "Search started"
    assert payload["events"][-1]["message"] == "Search completed"
    assert "secret-password" not in detail.text
    assert "state.json" not in detail.text
    assert "Traceback" not in detail.text


def test_attestazioni_generate_records_job_with_artifacts_and_shared_run_id(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        generation_adapter=_FakeGenerationAdapter(result=_generated_result()),
    )
    _login(client)

    response = client.post("/attestazioni/generate", json=_valid_generate_payload())

    assert response.status_code == 200
    run_id = response.json()["run_id"]
    detail = client.get(f"/jobs/{run_id}")
    payload = detail.json()
    assert payload["type"] == "attestazioni_generate"
    assert payload["status"] == "completed"
    assert payload["input"] == {
        "locatore_cf": OWNER_CF,
        "prepared_immobili_yaml_path": f"clients/web_prepare/{OWNER_CF}/immobili.yml",
    }
    assert payload["summary"] == {
        "requested_count": 1,
        "generated_count": 1,
        "failed_count": 0,
    }
    assert payload["artifacts"][0]["kind"] == "generation_immobili_yaml"
    assert payload["artifacts"][1]["kind"] == "attestazione_docx"
    assert payload["artifacts"][1]["download_url"] is None
    assert payload["events"][-1]["message"] == "Generation completed"


def test_clients_bulk_import_records_aborted_job_without_raw_csv_content(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        bulk_import_adapter=_FakeBulkImportAdapter(result=_bulk_result(status="aborted")),
    )
    _login(client)

    response = client.post(
        "/clients/bulk-import",
        json={
            "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\n",
            "force_update_visura": False,
            "fail_fast": True,
        },
    )

    assert response.status_code == 200
    run_id = response.json()["run_id"]
    detail = client.get(f"/jobs/{run_id}")
    payload = detail.json()
    assert payload["type"] == "clients_bulk_import"
    assert payload["status"] == "aborted"
    assert payload["input"] == {
        "force_update_visura": False,
        "fail_fast": True,
    }
    assert payload["artifacts"] == [
        {
            "kind": "clients_csv",
            "label": "Web-run clients.csv",
            "local_path": f"clients/web_bulk_import/{run_id}/clients.csv",
            "bucket": None,
            "object_key": None,
            "download_url": None,
        }
    ]
    assert "csv_content" not in detail.text
    assert "LOCATORE_CF" not in detail.text
    assert payload["events"][-1]["message"] == "Bulk import aborted"


def test_failed_adapter_call_records_failed_job_with_safe_message(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        prepare_adapter=_FakePrepareAdapter(error=PrepareImportFailedError("raw internal failure")),
    )
    _login(client)

    response = client.post("/attestazioni/search", json={"locatore_cf": OWNER_CF})

    assert response.status_code == 503
    assert response.json() == {"detail": "Prepare could not refresh client data."}
    jobs = client.get("/jobs").json()["jobs"]
    detail = client.get(f"/jobs/{jobs[0]['run_id']}")
    payload = detail.json()
    assert payload["status"] == "failed"
    assert payload["messages"] == ["Prepare could not refresh client data."]
    assert payload["events"][-1]["message"] == "Operation failed: Prepare could not refresh client data."
    assert "raw internal failure" not in detail.text


def test_bulk_import_runner_failure_records_failed_job_with_safe_message(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    client, _ = _make_client(
        tmp_path=tmp_path,
        bulk_import_adapter=_FakeBulkImportAdapter(
            error=ImportOnlyRunnerFailedError("backend detail must stay private")
        ),
    )
    _login(client)

    response = client.post(
        "/clients/bulk-import",
        json={
            "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\n",
            "force_update_visura": False,
            "fail_fast": False,
        },
    )

    assert response.status_code == 503
    jobs = client.get("/jobs").json()["jobs"]
    detail = client.get(f"/jobs/{jobs[0]['run_id']}")
    assert detail.json()["messages"] == ["Bulk import could not complete the import-only runner."]
    assert "backend detail must stay private" not in detail.text
