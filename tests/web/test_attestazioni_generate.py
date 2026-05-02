"""Tests for the Stage 4 protected `/attestazioni/generate` endpoint."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.domain.exceptions import GenerationPrepareRequiredError
from uppi.services.generation_runner import GenerationArtifactRef
from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig
from uppi.web.services.generation_adapter import GeneratedRunResult
from uppi.web.services.generation_yaml_builder import (
    NoSelectedImmobilesError,
    PreparedImmobileIdentityMismatchError,
    UnsafePreparedYamlPathError,
)
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
                    "IGNORE_SURCHARGES": False,
                    "CONTRACT_KIND": "ordinario",
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


class _FakePrepareAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    def prepare_search(self, locatore_cf: str, *, force_update_visura: bool = False) -> PreparedSearchResult:
        self.calls.append((locatore_cf, force_update_visura))
        return _prepared_search_result()


class _FakeGenerationAdapter:
    def __init__(self, *, result: GeneratedRunResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[object] = []

    def generate(self, payload):
        self.calls.append(payload)
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Fake generation adapter expected a result")
        return self.result


def _generated_result() -> GeneratedRunResult:
    return GeneratedRunResult(
        run_id="8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10",
        locatore_cf=OWNER_CF,
        prepared_output_path_relative=f"clients/web_prepare/{OWNER_CF}/immobili.yml",
        generation_output_path_relative=(
            f"clients/web_generation/{OWNER_CF}/8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10/immobili.yml"
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
        messages=(),
    )


def _make_client(fake_generation_adapter: _FakeGenerationAdapter) -> TestClient:
    return TestClient(
        create_app(
            _make_auth_config(),
            prepare_search_adapter=_FakePrepareAdapter(),
            generation_adapter=fake_generation_adapter,
        )
    )


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


def _valid_payload() -> dict:
    return {
        "locatore_cf": " rssmra80a01h501z ",
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
                "editable": {
                    "immobile_comune": "PESCARA",
                    "immobile_via": "VIA ROMA",
                    "immobile_civico": "10",
                    "immobile_piano": "1",
                    "immobile_interno": "2",
                    "energy_class": "G",
                    "arredato": "SI",
                    "istat": "",
                    "ignore_surcharges": False,
                    "contract_kind": "ordinario",
                },
                "run_only": {
                    "conduttore_nome": "Mario Rossi",
                    "conduttore_cf": OWNER_CF,
                    "conduttore_comune": "PESCARA",
                    "conduttore_via": "VIA VERDI 3",
                    "contratto_data": "2026-05-02",
                    "decorrenza_data": "2026-06-01",
                    "registrazione_data": "2026-05-10",
                    "registrazione_num": "12345",
                    "agenzia_entrate_sede": "PESCARA",
                    "canone_contrattuale_mensile": "500",
                    "durata_anni": "4",
                },
                "elements": {"a1": "X", "b1": "", "c1": "", "d1": ""},
            }
        ],
    }


def test_attestazioni_generate_requires_active_session():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakeGenerationAdapter(result=_generated_result()))

    response = client.post("/attestazioni/generate", json=_valid_payload())

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}


def test_attestazioni_generate_returns_synchronous_result_after_login_and_keeps_sensitive_values_out():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakeGenerationAdapter(result=_generated_result())
    client = _make_client(fake_adapter)
    _login(client)

    response = client.post("/attestazioni/generate", json=_valid_payload())

    assert response.status_code == 200
    assert fake_adapter.calls[0].locatore_cf == OWNER_CF
    assert response.json() == {
        "status": "generated",
        "run_id": "8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10",
        "locatore_cf": OWNER_CF,
        "input": {
            "prepared_immobili_yaml_path": f"clients/web_prepare/{OWNER_CF}/immobili.yml",
            "generation_immobili_yaml_path": (
                f"clients/web_generation/{OWNER_CF}/8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10/immobili.yml"
            ),
        },
        "summary": {
            "requested_count": 1,
            "generated_count": 1,
            "failed_count": 0,
        },
        "artifacts": [
            {
                "index": 1,
                "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                "kind": "attestazione_docx",
                "local_path": f"downloads/{OWNER_CF}/ATTESTAZIONE_{OWNER_CF}_81_F12_N345_S7.docx",
                "bucket": "attestazioni",
                "object_key": f"attestazioni/{OWNER_CF}/81.docx",
                "download_url": None,
            }
        ],
        "messages": [],
    }
    assert "password" not in response.text
    assert "pin" not in response.text
    assert "secret-password" not in response.text
    assert "test-session-secret" not in response.text
    assert "AE_PASSWORD" not in response.text
    assert "state.json" not in response.text


def test_attestazioni_generate_rejects_invalid_payload_and_safe_error_mappings():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakeGenerationAdapter(result=_generated_result()))
    _login(client)

    invalid = client.post("/attestazioni/generate", json={"locatore_cf": "SHORT"})
    assert invalid.status_code == 422

    cases = [
        (
            UnsafePreparedYamlPathError("outside"),
            400,
            "Prepared YAML path is outside the allowed web_prepare workspace.",
        ),
        (
            FileNotFoundError("missing"),
            404,
            "Prepared immobili YAML was not found.",
        ),
        (
            NoSelectedImmobilesError("At least one immobile must stay enabled for generation."),
            400,
            "At least one immobile must stay enabled for generation.",
        ),
        (
            PreparedImmobileIdentityMismatchError("mismatch"),
            409,
            "Prepared data no longer matches the current generation context. Run search/prepare again.",
        ),
        (
            GenerationPrepareRequiredError("prepare again"),
            409,
            "Prepared data no longer matches the current generation context. Run search/prepare again.",
        ),
    ]

    for error, expected_status, expected_detail in cases:
        fake_adapter = _FakeGenerationAdapter(error=error)
        scoped_client = _make_client(fake_adapter)
        _login(scoped_client)

        response = scoped_client.post("/attestazioni/generate", json=_valid_payload())

        assert response.status_code == expected_status
        assert response.json() == {"detail": expected_detail}


def test_attestazioni_generate_keeps_health_public_and_search_endpoint_working():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_generation_adapter = _FakeGenerationAdapter(result=_generated_result())
    client = _make_client(fake_generation_adapter)
    _login(client)

    live = client.get("/health/live")
    ready = client.get("/health/ready")
    search = client.post("/attestazioni/search", json={"locatore_cf": OWNER_CF})
    generate = client.post("/attestazioni/generate", json=_valid_payload())

    assert live.status_code == 200
    assert ready.status_code == 200
    assert search.status_code == 200
    assert generate.status_code == 200
