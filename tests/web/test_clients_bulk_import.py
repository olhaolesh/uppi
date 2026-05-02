"""Tests for the Stage 5 protected `/clients/bulk-import` endpoint."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from uppi.domain.exceptions import BulkImportCsvLoadError
from uppi.web.app import create_app
from uppi.web.config import WebAppConfig, WebAuthConfig, WebSessionConfig
from uppi.web.services.bulk_import_adapter import (
    BulkImportCsvWriteError,
    BulkImportNoUsableRowsError,
)
from uppi.web.services.bulk_import_adapter import BulkImportWebInvalidRow, BulkImportWebResult, BulkImportWebRowResult


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


class _FakeBulkImportAdapter:
    """Records bulk-import calls without touching the real import-only runner."""

    def __init__(self, *, result: BulkImportWebResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[object] = []

    def import_clients(self, payload):
        self.calls.append(payload)
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Fake bulk adapter expected a result")
        return self.result


def _bulk_result(status: str = "completed") -> BulkImportWebResult:
    return BulkImportWebResult(
        status=status,
        run_id="8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10",
        clients_csv_path=Path("/tmp/clients.csv"),
        clients_csv_path_relative="clients/web_bulk_import/8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10/clients.csv",
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
                locatore_cf="RSSMRA80A01H501Z",
                status="imported",
                message="Imported successfully",
            ),
            BulkImportWebRowResult(
                row_number=3,
                locatore_cf="BNCLGU85C01G482K",
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
        messages=(),
    )


def _make_client(fake_bulk_import_adapter: _FakeBulkImportAdapter) -> TestClient:
    return TestClient(
        create_app(
            _make_auth_config(),
            bulk_import_adapter=fake_bulk_import_adapter,
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


def _valid_payload(**overrides) -> dict:
    payload = {
        "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
        "force_update_visura": False,
        "fail_fast": False,
    }
    payload.update(overrides)
    return payload


def test_clients_bulk_import_requires_active_session():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakeBulkImportAdapter(result=_bulk_result()))

    response = client.post("/clients/bulk-import", json=_valid_payload())

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}


def test_clients_bulk_import_returns_summary_after_login_and_hides_sensitive_values():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakeBulkImportAdapter(result=_bulk_result())
    client = _make_client(fake_adapter)
    _login(client)

    response = client.post("/clients/bulk-import", json=_valid_payload())

    assert response.status_code == 200
    assert fake_adapter.calls[0].csv_content == "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n"
    assert fake_adapter.calls[0].force_update_visura is False
    assert fake_adapter.calls[0].fail_fast is False
    assert response.json() == {
        "status": "completed",
        "run_id": "8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10",
        "input": {
            "clients_csv_path": "clients/web_bulk_import/8e9f4c8f1b5d4b8c9a7e6d5c4b3a2f10/clients.csv",
            "force_update_visura": False,
            "fail_fast": False,
        },
        "summary": {
            "total_rows": 3,
            "valid_rows": 2,
            "invalid_rows": 1,
            "unique_clients": 2,
            "imported_count": 2,
            "failed_count": 0,
            "skipped_count": 0,
        },
        "results": [
            {
                "row_number": 2,
                "locatore_cf": "RSSMRA80A01H501Z",
                "status": "imported",
                "message": "Imported successfully",
            },
            {
                "row_number": 3,
                "locatore_cf": "BNCLGU85C01G482K",
                "status": "imported",
                "message": "Imported successfully",
            },
        ],
        "invalid_rows": [
            {
                "row_number": 4,
                "code": "missing_locatore_cf",
                "message": "clients.csv row 4 is missing LOCATORE_CF",
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
    assert "immobili.yml" not in response.text


def test_clients_bulk_import_passes_explicit_flags_and_validates_missing_or_empty_csv_content():
    """Перевіряє сценарій, описаний у назві тесту."""
    fake_adapter = _FakeBulkImportAdapter(result=_bulk_result())
    client = _make_client(fake_adapter)
    _login(client)

    missing = client.post("/clients/bulk-import", json={})
    empty = client.post("/clients/bulk-import", json={"csv_content": "   "})
    explicit = client.post(
        "/clients/bulk-import",
        json=_valid_payload(force_update_visura=True, fail_fast=True),
    )

    assert missing.status_code == 422
    assert empty.status_code == 422
    assert explicit.status_code == 200
    assert fake_adapter.calls[-1].force_update_visura is True
    assert fake_adapter.calls[-1].fail_fast is True


def test_clients_bulk_import_maps_safe_bulk_errors_to_http_responses():
    """Перевіряє сценарій, описаний у назві тесту."""
    cases = [
        (
            BulkImportNoUsableRowsError("Bulk import CSV does not contain any valid unique LOCATORE_CF rows."),
            400,
            "Bulk import CSV does not contain any valid unique LOCATORE_CF rows.",
        ),
        (
            BulkImportCsvLoadError("bad csv"),
            400,
            "Bulk import CSV could not be parsed into usable rows.",
        ),
        (
            BulkImportCsvWriteError("cannot write"),
            500,
            "Bulk import could not persist the web-run CSV input.",
        ),
    ]

    for error, expected_status, expected_detail in cases:
        client = _make_client(_FakeBulkImportAdapter(error=error))
        _login(client)

        response = client.post("/clients/bulk-import", json=_valid_payload())

        assert response.status_code == expected_status
        assert response.json() == {"detail": expected_detail}


def test_clients_bulk_import_keeps_health_and_auth_endpoints_working():
    """Перевіряє сценарій, описаний у назві тесту."""
    client = _make_client(_FakeBulkImportAdapter(result=_bulk_result()))
    _login(client)

    live = client.get("/health/live")
    ready = client.get("/health/ready")
    me = client.get("/auth/me")
    bulk = client.post("/clients/bulk-import", json=_valid_payload())

    assert live.status_code == 200
    assert ready.status_code == 200
    assert me.status_code == 200
    assert bulk.status_code == 200
