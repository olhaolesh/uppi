"""Явна retry policy matrix для non-browser failure reporting."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from minio.error import S3Error
from psycopg2 import InterfaceError, OperationalError

from uppi.domain.exceptions import DomainError, ValidationError
from uppi.domain.failure_registry import FailureStage


class FailureKind(StrEnum):
    """Класифікує типи failure surface для matrix-based retry decisions."""

    BROWSER_DERIVED = "browser_derived"
    INFRA_TRANSIENT = "infra_transient"
    STORAGE_TRANSIENT = "storage_transient"
    VALIDATION_CONTRACT = "validation_contract"
    DATA_CONTRACT = "data_contract"
    LOCAL_ARTIFACT = "local_artifact"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RetryDecision:
    """Результат retry-класифікації для одного stage/error combination."""

    retryable: bool
    failure_kind: FailureKind
    reason: str


@dataclass(frozen=True)
class StageRetryPolicy:
    """Описує, які failure kinds допустимо retry-ити для конкретної stage."""

    retryable_kinds: frozenset[FailureKind]
    no_retry_kinds: frozenset[FailureKind]
    default_retryable: bool = False


BROWSER_NO_RETRY_TEXT_MARKERS = (
    "playwright",
    "selector",
    "locator",
    "captcha",
    "storage_state",
    "state.json",
    "logout",
    "login",
    "sister transition",
)
"""Явний no-retry surface для browser-derived failures."""

BROWSER_NO_RETRY_MODULE_MARKERS = (
    "playwright",
    "scrapy_playwright",
)
"""Модулі, які трактуються як browser-derived surface."""

TRANSIENT_INFRA_TEXT_MARKERS = (
    "connection refused",
    "connection reset",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "network is unreachable",
)
"""Консервативні текстові маркери для infra-like transient failures."""

DEFAULT_NO_RETRY_KINDS = frozenset(
    {
        FailureKind.BROWSER_DERIVED,
        FailureKind.VALIDATION_CONTRACT,
        FailureKind.DATA_CONTRACT,
        FailureKind.LOCAL_ARTIFACT,
    }
)


def _make_stage_policy(*, retryable_kinds: frozenset[FailureKind]) -> StageRetryPolicy:
    """Створює explicit stage policy з shared no-retry rules."""
    return StageRetryPolicy(
        retryable_kinds=retryable_kinds,
        no_retry_kinds=DEFAULT_NO_RETRY_KINDS,
        default_retryable=False,
    )


RETRY_POLICY_MATRIX: dict[FailureStage, StageRetryPolicy] = {
    FailureStage.PERSON_SYNC: _make_stage_policy(
        retryable_kinds=frozenset({FailureKind.INFRA_TRANSIENT}),
    ),
    FailureStage.VISURA_INGEST: _make_stage_policy(
        retryable_kinds=frozenset(
            {
                FailureKind.INFRA_TRANSIENT,
                FailureKind.STORAGE_TRANSIENT,
            }
        ),
    ),
    FailureStage.IMMOBILE_SYNC: _make_stage_policy(
        retryable_kinds=frozenset({FailureKind.INFRA_TRANSIENT}),
    ),
    FailureStage.CONTRACT_SYNC: _make_stage_policy(
        retryable_kinds=frozenset({FailureKind.INFRA_TRANSIENT}),
    ),
    FailureStage.CANONE_STAGE: _make_stage_policy(
        retryable_kinds=frozenset({FailureKind.INFRA_TRANSIENT}),
    ),
    FailureStage.DOCUMENT_STAGE: _make_stage_policy(
        retryable_kinds=frozenset(
            {
                FailureKind.INFRA_TRANSIENT,
                FailureKind.STORAGE_TRANSIENT,
            }
        ),
    ),
    FailureStage.AUDIT_STAGE: _make_stage_policy(
        retryable_kinds=frozenset({FailureKind.INFRA_TRANSIENT}),
    ),
    FailureStage.PIPELINE_FATAL: _make_stage_policy(
        retryable_kinds=frozenset(
            {
                FailureKind.INFRA_TRANSIENT,
                FailureKind.STORAGE_TRANSIENT,
            }
        ),
    ),
}
"""Явна stage-by-stage retry matrix без blind browser retry."""


def _error_text(error: Exception | str) -> str:
    """Нормалізує текст помилки для lightweight policy heuristics."""
    return str(error).strip().lower()


def _error_module(error: Exception | str) -> str:
    """Повертає module path класу помилки або порожній рядок для plain text."""
    if isinstance(error, BaseException):
        return type(error).__module__.lower()
    return ""


def _error_class_name(error: Exception | str) -> str:
    """Повертає нормалізовану назву класу помилки."""
    if isinstance(error, BaseException):
        return type(error).__name__.lower()
    return "error"


def is_browser_derived_error(error: Exception | str) -> bool:
    """Визначає surface, який не можна blindly retry-ити через browser-state risk."""
    module_name = _error_module(error)
    if any(marker in module_name for marker in BROWSER_NO_RETRY_MODULE_MARKERS):
        return True

    haystack = " ".join((_error_class_name(error), module_name, _error_text(error)))
    return any(marker in haystack for marker in BROWSER_NO_RETRY_TEXT_MARKERS)


def classify_failure_kind(error: Exception | str) -> tuple[FailureKind, str]:
    """Класифікує failure surface для подальшого matrix-based retry decision."""
    if is_browser_derived_error(error):
        return FailureKind.BROWSER_DERIVED, "browser-derived surface"

    if isinstance(error, ValidationError):
        return FailureKind.VALIDATION_CONTRACT, "typed validation error"

    if isinstance(error, S3Error):
        return FailureKind.STORAGE_TRANSIENT, "minio s3 error"

    if isinstance(error, (OperationalError, InterfaceError, ConnectionError, TimeoutError)):
        return FailureKind.INFRA_TRANSIENT, "transient infra error"

    if isinstance(error, (FileNotFoundError, PermissionError)):
        return FailureKind.LOCAL_ARTIFACT, "local artifact io error"

    if isinstance(error, DomainError):
        return FailureKind.DATA_CONTRACT, "typed domain error"

    if isinstance(error, (ValueError, TypeError, KeyError, AttributeError, AssertionError)):
        return FailureKind.DATA_CONTRACT, "data contract error"

    if any(marker in _error_text(error) for marker in TRANSIENT_INFRA_TEXT_MARKERS):
        return FailureKind.INFRA_TRANSIENT, "transient infra text marker"

    return FailureKind.UNKNOWN, "conservative default"


class RetryPolicyMatrix:
    """Тонкий policy object для stage/error -> retry decision без retry engine redesign."""

    def __init__(
        self,
        matrix: dict[FailureStage, StageRetryPolicy] | None = None,
    ) -> None:
        """Зберігає explicit policy matrix з current conservative defaults."""
        self.matrix = dict(matrix or RETRY_POLICY_MATRIX)

    def stage_policy(self, stage: FailureStage) -> StageRetryPolicy:
        """Повертає policy для конкретної stage з explicit fallback на fatal policy."""
        return self.matrix.get(stage, self.matrix[FailureStage.PIPELINE_FATAL])

    def decide(self, *, stage: FailureStage, error: Exception | str) -> RetryDecision:
        """Повертає matrix-based retry decision без запуску реального retry engine."""
        failure_kind, reason = classify_failure_kind(error)
        stage_policy = self.stage_policy(stage)

        if failure_kind in stage_policy.no_retry_kinds:
            return RetryDecision(
                retryable=False,
                failure_kind=failure_kind,
                reason=f"{reason}; explicit no-retry for {stage.value}",
            )

        if failure_kind in stage_policy.retryable_kinds:
            return RetryDecision(
                retryable=True,
                failure_kind=failure_kind,
                reason=f"{reason}; explicit retryable for {stage.value}",
            )

        return RetryDecision(
            retryable=stage_policy.default_retryable,
            failure_kind=failure_kind,
            reason=f"{reason}; stage default for {stage.value}",
        )
