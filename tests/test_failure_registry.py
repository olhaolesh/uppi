"""Тести для failure registry model і append-only storage contract."""

from __future__ import annotations

from pathlib import Path

from uppi.domain.failure_registry import (
    FailureArtifactRef,
    FailureRecord,
    FailureStage,
)
from uppi.services.failure_registry import JsonlFailureRegistryStorage


def test_failure_record_from_error_preserves_contract_shape_and_redacts_message():
    """Перевіряє сценарій, описаний у назві тесту."""
    error = RuntimeError(
        "token=abc123 password=secret codice_fiscale=RSSMRA80A01H501Z",
    )

    record = FailureRecord.from_error(
        run_id="run-001",
        client_cf="RSSMRA80A01H501Z",
        stage=FailureStage.DOCUMENT_STAGE,
        error=error,
        retryable=False,
        artifact_refs=[
            ("storage_object", "attestazioni/RSSMRA80A01H501Z/42.docx?token=abc123"),
            FailureArtifactRef.create("local_path", Path("/tmp/RSSMRA80A01H501Z.docx")),
        ],
    )

    assert record.run_id == "run-001"
    assert record.client_cf == "RSSMRA80A01H501Z"
    assert record.stage is FailureStage.DOCUMENT_STAGE
    assert record.error_type == "RuntimeError"
    assert record.retryable is False
    assert "<token:redacted>" in record.message_redacted
    assert "<secret:redacted>" in record.message_redacted
    assert "<cf:redacted>" in record.message_redacted
    assert len(record.artifact_refs) == 2
    assert record.artifact_refs[0].kind == "storage_object"
    assert "<token:redacted>" in record.artifact_refs[0].ref
    assert "<cf:redacted>" in record.artifact_refs[1].ref


def test_failure_record_roundtrip_preserves_stage_retryable_and_artifact_refs():
    """Перевіряє сценарій, описаний у назві тесту."""
    original = FailureRecord.from_error(
        run_id="run-002",
        client_cf="BNCMRA80A01H501Z",
        stage=FailureStage.CANONE_STAGE,
        error=ValueError("bad input"),
        retryable=True,
        artifact_refs=[("calc_snapshot", "contract/81/input.json")],
    )

    restored = FailureRecord.from_dict(original.to_dict())

    assert restored.run_id == "run-002"
    assert restored.stage is FailureStage.CANONE_STAGE
    assert restored.retryable is True
    assert restored.message_redacted == "bad input"
    assert restored.artifact_refs[0].kind == "calc_snapshot"
    assert restored.artifact_refs[0].ref == "contract/81/input.json"


def test_jsonl_failure_registry_storage_appends_without_overwriting_existing_records(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = JsonlFailureRegistryStorage(tmp_path / "failure_registry.jsonl")

    first = FailureRecord.from_error(
        run_id="run-a",
        client_cf="AAAABB00C11D222E",
        stage=FailureStage.VISURA_INGEST,
        error=RuntimeError("upload failed"),
        retryable=True,
    )
    second = FailureRecord.from_error(
        run_id="run-b",
        client_cf="FFFFGG00H11I222L",
        stage=FailureStage.AUDIT_STAGE,
        error="audit failed",
        retryable=False,
        artifact_refs=[("audit_stage", "generation_or_upload")],
    )

    storage.append(first)
    storage.append(second)

    records = storage.list_records()
    assert len(records) == 2
    assert records[0].run_id == "run-a"
    assert records[1].run_id == "run-b"


def test_jsonl_failure_registry_storage_filters_by_run_id_and_client_cf(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = JsonlFailureRegistryStorage(tmp_path / "failure_registry.jsonl")

    storage.append(
        FailureRecord.from_error(
            run_id="run-keep",
            client_cf="RSSMRA80A01H501Z",
            stage=FailureStage.PERSON_SYNC,
            error="skip",
            retryable=True,
        )
    )
    storage.append(
        FailureRecord.from_error(
            run_id="run-skip",
            client_cf="BNCMRA80A01H501Z",
            stage=FailureStage.CONTRACT_SYNC,
            error="skip",
            retryable=False,
        )
    )

    by_run = storage.list_records(run_id="run-keep")
    by_cf = storage.list_records(client_cf="RSSMRA80A01H501Z")

    assert len(by_run) == 1
    assert by_run[0].stage is FailureStage.PERSON_SYNC
    assert len(by_cf) == 1
    assert by_cf[0].stage is FailureStage.PERSON_SYNC
