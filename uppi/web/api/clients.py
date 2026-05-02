"""Stage 5 protected bulk-import endpoint for web-friendly CSV submission."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from uppi.domain.exceptions import BulkImportCsvLoadError, ImportOnlyRunnerFailedError
from uppi.web.api.jobs import get_job_registry
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.schemas.clients import ClientsBulkImportRequest, ClientsBulkImportResponse
from uppi.web.schemas.jobs import JobArtifactResponse
from uppi.web.security import require_authenticated_user
from uppi.web.services.bulk_import_adapter import (
    BulkImportAdapter,
    BulkImportCsvWriteError,
    BulkImportNoUsableRowsError,
)
from uppi.web.services.job_registry import JobRegistry

router = APIRouter(prefix="/clients", tags=["clients"])


def get_bulk_import_adapter(request: Request) -> BulkImportAdapter:
    """Returns the app-scoped bulk-import adapter or creates the default one lazily."""
    adapter = getattr(request.app.state, "bulk_import_adapter", None)
    if adapter is None:
        adapter = BulkImportAdapter()
        request.app.state.bulk_import_adapter = adapter
    return adapter


@router.post("/bulk-import", response_model=ClientsBulkImportResponse)
def bulk_import_clients(
    payload: ClientsBulkImportRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    bulk_import_adapter: BulkImportAdapter = Depends(get_bulk_import_adapter),
    job_registry: JobRegistry = Depends(get_job_registry),
) -> ClientsBulkImportResponse:
    """Stores one web-run CSV file and reuses the current bulk import-only boundary."""
    job = job_registry.start_job(
        job_type="clients_bulk_import",
        actor_username=user.username,
        input_metadata={
            "force_update_visura": payload.force_update_visura,
            "fail_fast": payload.fail_fast,
        },
        started_message="Bulk import started",
    )
    try:
        result = bulk_import_adapter.import_clients(payload, run_id=job.run_id)
    except BulkImportNoUsableRowsError as exc:
        detail = str(exc)
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except BulkImportCsvLoadError as exc:
        detail = "Bulk import CSV could not be parsed into usable rows."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except ImportOnlyRunnerFailedError as exc:
        detail = "Bulk import could not complete the import-only runner."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail,
        ) from exc
    except BulkImportCsvWriteError as exc:
        detail = "Bulk import could not persist the web-run CSV input."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc
    except Exception as exc:
        detail = "Unexpected error while processing the bulk import request."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc

    response = ClientsBulkImportResponse.from_web_result(result)
    job_registry.complete_job(
        job.run_id,
        status=response.status,
        summary={
            "total_rows": response.summary.total_rows,
            "valid_rows": response.summary.valid_rows,
            "invalid_rows": response.summary.invalid_rows,
            "unique_clients": response.summary.unique_clients,
            "imported_count": response.summary.imported_count,
            "failed_count": response.summary.failed_count,
            "skipped_count": response.summary.skipped_count,
        },
        artifacts=[
            JobArtifactResponse(
                kind="clients_csv",
                label="Web-run clients.csv",
                local_path=response.input.clients_csv_path,
            )
        ],
        messages=list(response.messages),
        completion_message=(
            "Bulk import aborted" if response.status == "aborted" else "Bulk import completed"
        ),
        event_level="warning" if response.status == "aborted" else "info",
    )
    return response
