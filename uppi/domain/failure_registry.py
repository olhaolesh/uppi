"""Модель failure registry для non-browser stage-level помилок."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable

from uppi.logging_config import sanitize_log_text


class FailureStage(StrEnum):
    """Стабільні stage identifiers, узгоджені з current Sprint 2 boundaries."""

    PERSON_SYNC = "PersonSync"
    VISURA_INGEST = "VisuraIngest"
    IMMOBILE_SYNC = "ImmobileSync"
    CONTRACT_SYNC = "ContractSync"
    CANONE_STAGE = "CanoneStage"
    DOCUMENT_STAGE = "DocumentStage"
    AUDIT_STAGE = "AuditStage"
    PIPELINE_FATAL = "PipelineFatal"


@dataclass(frozen=True)
class FailureArtifactRef:
    """Безпечне посилання на пов'язаний артефакт без сирого sensitive payload."""

    kind: str
    ref: str

    @classmethod
    def create(cls, kind: str, ref: str | Path) -> "FailureArtifactRef":
        """Створює redacted artifact reference для збереження в registry."""
        return cls(
            kind=sanitize_log_text(str(kind).strip()),
            ref=sanitize_log_text(str(ref).strip()),
        )

    def to_dict(self) -> dict[str, str]:
        """Повертає serializable shape для storage adapter."""
        return {
            "kind": self.kind,
            "ref": self.ref,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FailureArtifactRef":
        """Відновлює artifact reference зі storage payload."""
        return cls.create(
            kind=str(payload.get("kind") or ""),
            ref=str(payload.get("ref") or ""),
        )


@dataclass(frozen=True)
class FailureRecord:
    """Append-only failure record для майбутнього stage-level reporting."""

    run_id: str
    client_cf: str
    stage: FailureStage
    error_type: str
    retryable: bool
    message_redacted: str
    artifact_refs: tuple[FailureArtifactRef, ...] = field(default_factory=tuple)
    recorded_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
    )

    @classmethod
    def from_error(
        cls,
        *,
        run_id: str,
        client_cf: str,
        stage: FailureStage,
        error: Exception | str,
        retryable: bool,
        artifact_refs: Iterable[FailureArtifactRef | tuple[str, str] | dict[str, Any]] = (),
    ) -> "FailureRecord":
        """Створює standardized failure record з already-redacted message surface."""
        if isinstance(error, BaseException):
            error_type = type(error).__name__
            message = str(error)
        else:
            error_type = "Error"
            message = str(error)

        normalized_refs: list[FailureArtifactRef] = []
        for artifact_ref in artifact_refs:
            if isinstance(artifact_ref, FailureArtifactRef):
                normalized_refs.append(artifact_ref)
            elif isinstance(artifact_ref, dict):
                normalized_refs.append(FailureArtifactRef.from_dict(artifact_ref))
            else:
                kind, ref = artifact_ref
                normalized_refs.append(FailureArtifactRef.create(kind, ref))

        return cls(
            run_id=str(run_id),
            client_cf=str(client_cf).strip(),
            stage=stage,
            error_type=sanitize_log_text(error_type),
            retryable=bool(retryable),
            message_redacted=sanitize_log_text(message),
            artifact_refs=tuple(normalized_refs),
        )

    def to_dict(self) -> dict[str, Any]:
        """Повертає serializable payload для storage contract."""
        return {
            "run_id": self.run_id,
            "client_cf": self.client_cf,
            "stage": self.stage.value,
            "error_type": self.error_type,
            "retryable": self.retryable,
            "message_redacted": self.message_redacted,
            "artifact_refs": [artifact.to_dict() for artifact in self.artifact_refs],
            "recorded_at_utc": self.recorded_at_utc,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FailureRecord":
        """Відновлює failure record зі storage payload."""
        return cls(
            run_id=str(payload["run_id"]),
            client_cf=str(payload["client_cf"]),
            stage=FailureStage(str(payload["stage"])),
            error_type=sanitize_log_text(str(payload["error_type"])),
            retryable=bool(payload["retryable"]),
            message_redacted=sanitize_log_text(str(payload["message_redacted"])),
            artifact_refs=tuple(
                FailureArtifactRef.from_dict(item)
                for item in payload.get("artifact_refs", [])
            ),
            recorded_at_utc=str(payload["recorded_at_utc"]),
        )
