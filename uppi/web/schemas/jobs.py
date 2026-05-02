"""Schemas for the Stage 9 lightweight web job registry and jobs API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

JobType = Literal[
    "attestazioni_search",
    "attestazioni_generate",
    "clients_bulk_import",
]
JobStatus = Literal["running", "completed", "failed", "aborted", "partial"]
JobEventLevel = Literal["info", "warning", "error"]


class _StrictWebModel(BaseModel):
    """Base model for additive web DTOs that reject unknown fields."""

    model_config = ConfigDict(extra="forbid")


class JobActorResponse(_StrictWebModel):
    """Minimal actor identity recorded for one web-run job."""

    username: str


class JobArtifactResponse(_StrictWebModel):
    """Safe artifact reference stored in the web job registry."""

    kind: str
    label: str
    local_path: str | None = None
    bucket: str | None = None
    object_key: str | None = None
    download_url: str | None = None


class JobEventResponse(_StrictWebModel):
    """Safe event entry recorded for one job without exposing raw logs."""

    timestamp: str
    level: JobEventLevel
    message: str


class JobListItemResponse(_StrictWebModel):
    """Summary row returned by `GET /jobs`."""

    run_id: str
    type: JobType
    status: JobStatus
    created_at: str
    updated_at: str
    summary: dict[str, Any] = Field(default_factory=dict)
    artifact_count: int = 0
    message_count: int = 0


class JobDetailResponse(_StrictWebModel):
    """Detailed job record returned by `GET /jobs/{run_id}` and persisted to storage."""

    run_id: str
    type: JobType
    status: JobStatus
    created_at: str
    updated_at: str
    started_at: str | None = None
    finished_at: str | None = None
    actor: JobActorResponse
    input: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[JobArtifactResponse] = Field(default_factory=list)
    events: list[JobEventResponse] = Field(default_factory=list)
    messages: list[str] = Field(default_factory=list)

    def to_list_item(self) -> JobListItemResponse:
        """Converts one detailed record into the public list-row shape."""
        return JobListItemResponse(
            run_id=self.run_id,
            type=self.type,
            status=self.status,
            created_at=self.created_at,
            updated_at=self.updated_at,
            summary=self.summary,
            artifact_count=len(self.artifacts),
            message_count=len(self.messages),
        )


class JobsListResponse(_StrictWebModel):
    """Protected response shape for `GET /jobs`."""

    jobs: list[JobListItemResponse]


__all__ = [
    "JobActorResponse",
    "JobArtifactResponse",
    "JobDetailResponse",
    "JobEventResponse",
    "JobListItemResponse",
    "JobStatus",
    "JobType",
    "JobsListResponse",
]
