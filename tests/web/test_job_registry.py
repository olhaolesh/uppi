"""Tests for the Stage 9 lightweight web job registry."""

from __future__ import annotations

from datetime import datetime, timezone

from uppi.web.schemas.jobs import JobArtifactResponse
from uppi.web.services.job_registry import JobRegistry


def _utc(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def test_job_registry_stores_jobs_newest_first_with_limit_and_filters(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    timestamps = iter(
        [
            _utc(2026, 5, 2, 12, 0, 0),
            _utc(2026, 5, 2, 12, 0, 5),
            _utc(2026, 5, 2, 12, 1, 0),
            _utc(2026, 5, 2, 12, 1, 5),
            _utc(2026, 5, 2, 12, 2, 0),
            _utc(2026, 5, 2, 12, 2, 5),
        ]
    )
    registry = JobRegistry(
        storage_path=tmp_path / "clients" / "web_jobs" / "jobs.json",
        now_factory=lambda: next(timestamps),
        run_id_factory=iter(["run-1", "run-2", "run-3"]).__next__,
    )

    run_1 = registry.start_job(
        job_type="attestazioni_search",
        actor_username="operator",
        input_metadata={"locatore_cf": "RSSMRA80A01H501Z"},
        started_message="Search started",
    )
    registry.complete_job(
        run_1.run_id,
        summary={"immobili_count": 2, "active_count": 2},
        completion_message="Search completed",
    )

    run_2 = registry.start_job(
        job_type="clients_bulk_import",
        actor_username="operator",
        input_metadata={"fail_fast": True},
        started_message="Bulk import started",
    )
    registry.complete_job(
        run_2.run_id,
        status="aborted",
        summary={"total_rows": 3, "failed_count": 1},
        artifacts=[
            JobArtifactResponse(
                kind="clients_csv",
                label="Web-run clients.csv",
                local_path="clients/web_bulk_import/run-2/clients.csv",
            )
        ],
        messages=["Bulk import aborted after the first failure."],
        completion_message="Bulk import aborted",
        event_level="warning",
    )

    run_3 = registry.start_job(
        job_type="attestazioni_generate",
        actor_username="operator",
        input_metadata={"locatore_cf": "RSSMRA80A01H501Z"},
        started_message="Generation started",
    )
    registry.complete_job(
        run_3.run_id,
        status="partial",
        summary={"requested_count": 2, "generated_count": 1, "failed_count": 1},
        completion_message="Generation completed with failures",
        event_level="warning",
    )

    assert registry.storage_path.exists()

    listed = registry.list_jobs()
    assert [job.run_id for job in listed.jobs] == ["run-3", "run-2", "run-1"]
    assert registry.list_jobs(limit=1).jobs[0].run_id == "run-3"
    assert [job.run_id for job in registry.list_jobs(job_type="clients_bulk_import").jobs] == ["run-2"]
    assert [job.run_id for job in registry.list_jobs(status="partial").jobs] == ["run-3"]

    detail = registry.get_job("run-2")
    assert detail.status == "aborted"
    assert detail.artifacts[0].local_path == "clients/web_bulk_import/run-2/clients.csv"
    assert detail.messages == ["Bulk import aborted after the first failure."]
    assert detail.events[-1].message == "Bulk import aborted"
