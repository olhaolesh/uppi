"""Tests for the Stage 3 protected `/attestazioni/search` endpoint."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.domain.exceptions import (
    PrepareGenerationFailedError,
    PrepareImportFailedError,
    PrepareInputError,
    PrepareNoDataError,
)
from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig
from uppi.web.services.prepare_adapter import PreparedSearchResult


def _make_auth_config() -> WebAppConfig:
    """Builds an explicit test config for protected search assertions."""
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


def _make_document(locatore_cf: str = "RSSMRA80A01H501Z") -> ImmobiliDocumentConfig:
    """Returns a minimal canonical document fixture for API tests."""
    return ImmobiliDocumentConfig.from_raw(
        {
            "LOCATORE_CF": locatore_cf,
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


class _FakePrepareAdapter:
    """Records `/attestazioni/search` calls without touching real prepare flow."""

    def __init__(self, *, result: PreparedSearchResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[str, bool]] = []

    def prepare_search(self, locatore_cf: str, *, force_update_visura: bool = False) -> PreparedSearchResult:
        self.calls.append((locatore_cf, force_update_visura))
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Fake adapter expected a result")
        return self.result


def _prepared_result(source: str = "db") -> PreparedSearchResult:
    """Builds a deterministic prepared search result fixture."""
    import_performed = source == "sister"
    return PreparedSearchResult(
        locatore_cf="RSSMRA80A01H501Z",
        output_path=Path("/tmp/immobili.yml"),
        output_path_relative="clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
        decision="db_miss_imported" if import_performed else "db_hit_no_force",
        db_hit_before_import=not import_performed,
        import_performed=import_performed,
        source=source,
        document=_make_document(),
    )


def _make_client(fake_adapter: _FakePrepareAdapter) -> TestClient:
    """Builds a TestClient with an injected fake prepare adapter."""
    return TestClient(create_app(_make_auth_config(), prepare_search_adapter=fake_adapter))


def _login(client: TestClient) -> None:
    """Authenticates the fixed Stage 2 operator for protected endpoint tests."""
    response = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )
    assert response.status_code == 200


def test_attestazioni_search_requires_active_session():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakePrepareAdapter(result=_prepared_result()))

    response = client.post("/attestazioni/search", json={"locatore_cf": "RSSMRA80A01H501Z"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}


def test_attestazioni_search_returns_prepared_document_after_login_and_normalizes_cf():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakePrepareAdapter(result=_prepared_result(source="db"))
    client = _make_client(fake_adapter)
    _login(client)

    response = client.post(
        "/attestazioni/search",
        json={"locatore_cf": " rssmra80a01h501z "},
    )

    assert response.status_code == 200
    assert fake_adapter.calls == [("RSSMRA80A01H501Z", False)]
    payload = response.json()
    assert payload["status"] == "prepared"
    assert payload["source"] == "db"
    assert payload["client"]["locatore_cf"] == "RSSMRA80A01H501Z"
    assert payload["document"]["immobili_yaml_path"] == "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml"
    assert "password" not in response.text
    assert "pin" not in response.text
    assert "secret-password" not in response.text
    assert "test-session-secret" not in response.text
    assert "AE_PASSWORD" not in response.text
    assert "state.json" not in response.text


def test_attestazioni_search_passes_explicit_force_update_flag():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakePrepareAdapter(result=_prepared_result(source="sister"))
    client = _make_client(fake_adapter)
    _login(client)

    response = client.post(
        "/attestazioni/search",
        json={
            "locatore_cf": "RSSMRA80A01H501Z",
            "force_update_visura": True,
        },
    )

    assert response.status_code == 200
    assert fake_adapter.calls == [("RSSMRA80A01H501Z", True)]
    assert response.json()["source"] == "sister"


def test_attestazioni_search_rejects_missing_or_invalid_locatore_cf():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakePrepareAdapter(result=_prepared_result())
    client = _make_client(fake_adapter)
    _login(client)

    missing = client.post("/attestazioni/search", json={})
    invalid = client.post("/attestazioni/search", json={"locatore_cf": "SHORT"})

    assert missing.status_code == 422
    assert invalid.status_code == 422
    assert fake_adapter.calls == []


def test_attestazioni_search_maps_prepare_errors_to_safe_http_responses():
    """Перевіряє сценарій, описаний у назві тесту."""
    cases = [
        (PrepareInputError("bad input"), 400, "Invalid prepare request."),
        (PrepareNoDataError("no data"), 404, "No prepared immobili data is available for the requested client."),
        (PrepareImportFailedError("import failed"), 503, "Prepare could not refresh client data."),
        (PrepareGenerationFailedError("generation failed"), 500, "Prepared client data could not be loaded safely."),
    ]

    for error, expected_status, expected_detail in cases:
        fake_adapter = _FakePrepareAdapter(error=error)
        client = _make_client(fake_adapter)
        _login(client)

        response = client.post("/attestazioni/search", json={"locatore_cf": "RSSMRA80A01H501Z"})

        assert response.status_code == expected_status
        assert response.json() == {"detail": expected_detail}


def test_attestazioni_search_does_not_break_existing_health_or_auth_endpoints():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakePrepareAdapter(result=_prepared_result()))

    live = client.get("/health/live")
    ready = client.get("/health/ready")
    login = client.post(
        "/auth/login",
        json={
            "username": "operator",
            "password": "secret-password",
            "pin": "1234",
        },
    )

    assert live.status_code == 200
    assert ready.status_code == 200
    assert login.status_code == 200
