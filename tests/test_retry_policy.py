"""Тести для explicit retry policy matrix без retry-engine redesign."""

from __future__ import annotations

from uppi.domain.exceptions import CanoneInputValidationError
from uppi.domain.failure_registry import FailureStage
from uppi.services.failure_registry import FailureRegistryRecorder
from uppi.services.retry_policy import FailureKind, RetryPolicyMatrix
from uppi.services.validation import validate_canone_input


class RecordingFailureStorage:
    """In-memory storage для перевірки integration між recorder і retry matrix."""

    def __init__(self) -> None:
        """Ініціалізує порожній список records."""
        self.records = []

    def append(self, record) -> None:
        """Запам'ятовує record без side effects."""
        self.records.append(record)

    def list_records(self, *, run_id=None, client_cf=None):
        """Повертає всі накопичені records."""
        return list(self.records)


class FakePlaywrightError(Exception):
    """Імітує browser-derived failure без торкання реального Playwright."""

    __module__ = "playwright.async_api"


def test_retry_policy_marks_infra_like_failure_retryable_for_visura_ingest():
    """Перевіряє сценарій, описаний у назві тесту."""
    decision = RetryPolicyMatrix().decide(
        stage=FailureStage.VISURA_INGEST,
        error=ConnectionError("connection reset by peer"),
    )

    assert decision.retryable is True
    assert decision.failure_kind is FailureKind.INFRA_TRANSIENT


def test_retry_policy_marks_browser_derived_failure_non_retryable():
    """Перевіряє сценарій, описаний у назві тесту."""
    decision = RetryPolicyMatrix().decide(
        stage=FailureStage.DOCUMENT_STAGE,
        error=FakePlaywrightError("captcha submit failed"),
    )

    assert decision.retryable is False
    assert decision.failure_kind is FailureKind.BROWSER_DERIVED


def test_retry_policy_marks_validation_failure_non_retryable_even_if_recoverable():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_canone_input({"superficie_catastale": 10})
    error = CanoneInputValidationError.from_validation_result(
        result,
        fallback_message="Структурно некоректний canone input.",
    )

    decision = RetryPolicyMatrix().decide(
        stage=FailureStage.CANONE_STAGE,
        error=error,
    )

    assert error.recoverable is True
    assert decision.retryable is False
    assert decision.failure_kind is FailureKind.VALIDATION_CONTRACT


def test_failure_registry_recorder_uses_retry_matrix_for_retryable_field():
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage=storage)

    record = recorder.record_failure(
        run_id="run-101",
        client_cf="RSSMRA80A01H501Z",
        stage=FailureStage.DOCUMENT_STAGE,
        error=ConnectionError("temporary network timeout"),
    )

    assert record is not None
    assert record.retryable is True
    assert storage.records[0].retryable is True


def test_failure_registry_recorder_explicit_retryable_override_wins_over_matrix():
    """Перевіряє сценарій, описаний у назві тесту."""
    storage = RecordingFailureStorage()
    recorder = FailureRegistryRecorder(storage=storage)

    record = recorder.record_failure(
        run_id="run-102",
        client_cf="RSSMRA80A01H501Z",
        stage=FailureStage.VISURA_INGEST,
        error=ConnectionError("temporary network timeout"),
        retryable=False,
    )

    assert record is not None
    assert record.retryable is False
    assert storage.records[0].retryable is False
