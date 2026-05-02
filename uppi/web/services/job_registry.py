"""Lightweight Stage 9 web job registry stored in a workspace-local JSON file."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable
from uuid import uuid4

from uppi.config.app_config import project_root
from uppi.web.schemas.jobs import (
    JobActorResponse,
    JobArtifactResponse,
    JobDetailResponse,
    JobEventResponse,
    JobsListResponse,
)


class JobRegistryNotFoundError(KeyError):
    """Raised when a requested job record does not exist."""


class JobRegistry:
    """Persists and queries lightweight web job records without touching DB schemas."""

    def __init__(
        self,
        *,
        storage_path: Path | None = None,
        repo_root: Path | None = None,
        now_factory: Callable[[], datetime] | None = None,
        run_id_factory: Callable[[], str] | None = None,
    ) -> None:
        resolved_repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.storage_path = (
            Path(storage_path)
            if storage_path is not None
            else resolved_repo_root / "clients" / "web_jobs" / "jobs.json"
        )
        self.now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self.run_id_factory = run_id_factory or (lambda: uuid4().hex)
        self._lock = Lock()

    def start_job(
        self,
        *,
        job_type: str,
        actor_username: str,
        input_metadata: dict[str, Any] | None = None,
        started_message: str,
        run_id: str | None = None,
    ) -> JobDetailResponse:
        """Creates one running job record before delegating to an adapter."""
        resolved_run_id = str(run_id or self.run_id_factory())
        timestamp = self._timestamp()
        job = JobDetailResponse(
            run_id=resolved_run_id,
            type=job_type,
            status="running",
            created_at=timestamp,
            updated_at=timestamp,
            started_at=timestamp,
            finished_at=None,
            actor=JobActorResponse(username=actor_username),
            input=dict(input_metadata or {}),
            summary={},
            artifacts=[],
            events=[
                JobEventResponse(
                    timestamp=timestamp,
                    level="info",
                    message=started_message,
                )
            ],
            messages=[],
        )
        with self._lock:
            jobs = [existing for existing in self._read_jobs_unlocked() if existing.run_id != resolved_run_id]
            jobs.append(job)
            self._write_jobs_unlocked(jobs)
        return job

    def complete_job(
        self,
        run_id: str,
        *,
        status: str = "completed",
        summary: dict[str, Any] | None = None,
        artifacts: list[JobArtifactResponse] | None = None,
        messages: list[str] | None = None,
        completion_message: str,
        event_level: str = "info",
    ) -> JobDetailResponse:
        """Marks one job as completed/aborted/partial and stores safe outputs."""
        with self._lock:
            jobs = self._read_jobs_unlocked()
            job, index = self._find_job_unlocked(jobs, run_id)
            timestamp = self._timestamp()
            updated = job.model_copy(
                update={
                    "status": status,
                    "updated_at": timestamp,
                    "finished_at": timestamp,
                    "summary": dict(summary or {}),
                    "artifacts": list(artifacts or []),
                    "messages": list(messages or []),
                    "events": [
                        *job.events,
                        JobEventResponse(
                            timestamp=timestamp,
                            level=event_level,
                            message=completion_message,
                        ),
                    ],
                }
            )
            jobs[index] = updated
            self._write_jobs_unlocked(jobs)
        return updated

    def fail_job(
        self,
        run_id: str,
        *,
        safe_message: str,
        summary: dict[str, Any] | None = None,
        messages: list[str] | None = None,
    ) -> JobDetailResponse:
        """Marks one job as failed with a safe message and no traceback exposure."""
        with self._lock:
            jobs = self._read_jobs_unlocked()
            job, index = self._find_job_unlocked(jobs, run_id)
            timestamp = self._timestamp()
            combined_messages = list(messages or [])
            if safe_message not in combined_messages:
                combined_messages.append(safe_message)
            updated = job.model_copy(
                update={
                    "status": "failed",
                    "updated_at": timestamp,
                    "finished_at": timestamp,
                    "summary": dict(summary or job.summary),
                    "messages": combined_messages,
                    "events": [
                        *job.events,
                        JobEventResponse(
                            timestamp=timestamp,
                            level="error",
                            message=f"Operation failed: {safe_message}",
                        ),
                    ],
                }
            )
            jobs[index] = updated
            self._write_jobs_unlocked(jobs)
        return updated

    def get_job(self, run_id: str) -> JobDetailResponse:
        """Returns one detailed job record or raises if it does not exist."""
        with self._lock:
            jobs = self._read_jobs_unlocked()
            job, _ = self._find_job_unlocked(jobs, run_id)
            return job

    def list_jobs(
        self,
        *,
        job_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> JobsListResponse:
        """Returns newest-first list items with optional type/status filters."""
        effective_limit = max(1, min(int(limit), 200))
        with self._lock:
            jobs = self._read_jobs_unlocked()
        filtered = [
            job
            for job in jobs
            if (job_type is None or job.type == job_type)
            and (status is None or job.status == status)
        ]
        filtered.sort(key=lambda item: (item.created_at, item.updated_at, item.run_id), reverse=True)
        return JobsListResponse(jobs=[job.to_list_item() for job in filtered[:effective_limit]])

    def _read_jobs_unlocked(self) -> list[JobDetailResponse]:
        if not self.storage_path.exists():
            return []
        raw = self.storage_path.read_text(encoding="utf-8").strip()
        if not raw:
            return []
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise ValueError("Web jobs storage must contain a JSON array.")
        return [JobDetailResponse.model_validate(item) for item in payload]

    def _write_jobs_unlocked(self, jobs: list[JobDetailResponse]) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.storage_path.with_suffix(f"{self.storage_path.suffix}.tmp")
        temp_path.write_text(
            json.dumps(
                [job.model_dump(mode="json") for job in jobs],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        temp_path.replace(self.storage_path)

    @staticmethod
    def _find_job_unlocked(
        jobs: list[JobDetailResponse],
        run_id: str,
    ) -> tuple[JobDetailResponse, int]:
        for index, job in enumerate(jobs):
            if job.run_id == run_id:
                return job, index
        raise JobRegistryNotFoundError(run_id)

    def _timestamp(self) -> str:
        return self.now_factory().astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00",
            "Z",
        )


__all__ = [
    "JobRegistry",
    "JobRegistryNotFoundError",
]
