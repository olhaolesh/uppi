"""Stage 5 protected bulk-import endpoint for web-friendly CSV submission."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from uppi.domain.exceptions import BulkImportCsvLoadError, ImportOnlyRunnerFailedError
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.schemas.clients import ClientsBulkImportRequest, ClientsBulkImportResponse
from uppi.web.security import require_authenticated_user
from uppi.web.services.bulk_import_adapter import (
    BulkImportAdapter,
    BulkImportCsvWriteError,
    BulkImportNoUsableRowsError,
)

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
    _: AuthenticatedUser = Depends(require_authenticated_user),
    bulk_import_adapter: BulkImportAdapter = Depends(get_bulk_import_adapter),
) -> ClientsBulkImportResponse:
    """Stores one web-run CSV file and reuses the current bulk import-only boundary."""
    try:
        result = bulk_import_adapter.import_clients(payload)
    except BulkImportNoUsableRowsError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except BulkImportCsvLoadError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Bulk import CSV could not be parsed into usable rows.",
        ) from exc
    except ImportOnlyRunnerFailedError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Bulk import could not complete the import-only runner.",
        ) from exc
    except BulkImportCsvWriteError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Bulk import could not persist the web-run CSV input.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error while processing the bulk import request.",
        ) from exc

    return ClientsBulkImportResponse.from_web_result(result)
