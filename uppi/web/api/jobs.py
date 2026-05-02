"""Stage 9 protected jobs endpoints for lightweight web-run history."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.schemas.jobs import JobDetailResponse, JobStatus, JobType, JobsListResponse
from uppi.web.security import require_authenticated_user
from uppi.web.services.job_registry import JobRegistry, JobRegistryNotFoundError

router = APIRouter(prefix="/jobs", tags=["jobs"])


def get_job_registry(request: Request) -> JobRegistry:
    """Returns the app-scoped job registry or creates the default one lazily."""
    registry = getattr(request.app.state, "job_registry", None)
    if registry is None:
        registry = JobRegistry()
        request.app.state.job_registry = registry
    return registry


@router.get("", response_model=JobsListResponse)
def list_jobs(
    _: AuthenticatedUser = Depends(require_authenticated_user),
    job_type: JobType | None = Query(default=None, alias="type"),
    job_status: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=200),
    job_registry: JobRegistry = Depends(get_job_registry),
) -> JobsListResponse:
    """Returns newest-first job list items for the authenticated web operator."""
    return job_registry.list_jobs(job_type=job_type, status=job_status, limit=limit)


@router.get("/{run_id}", response_model=JobDetailResponse)
def get_job(
    run_id: str,
    _: AuthenticatedUser = Depends(require_authenticated_user),
    job_registry: JobRegistry = Depends(get_job_registry),
) -> JobDetailResponse:
    """Returns one detailed job record including safe events and artifact refs."""
    try:
        return job_registry.get_job(run_id)
    except JobRegistryNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested job run was not found.",
        ) from exc
