"""Stage 3 protected search/prepare endpoint for attestazioni workflows."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from uppi.domain.exceptions import (
    PrepareGenerationFailedError,
    PrepareImportFailedError,
    PrepareInputError,
    PrepareNoDataError,
    PrepareOutputWriteError,
    YamlInputValidationError,
)
from uppi.web.schemas.attestazioni import (
    AttestazioniSearchRequest,
    AttestazioniSearchResponse,
)
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.security import require_authenticated_user
from uppi.web.services.prepare_adapter import PrepareSearchAdapter

router = APIRouter(prefix="/attestazioni", tags=["attestazioni"])


def get_prepare_search_adapter(request: Request) -> PrepareSearchAdapter:
    """Returns the app-scoped adapter or creates the default one lazily."""
    adapter = getattr(request.app.state, "prepare_search_adapter", None)
    if adapter is None:
        adapter = PrepareSearchAdapter()
        request.app.state.prepare_search_adapter = adapter
    return adapter


@router.post("/search", response_model=AttestazioniSearchResponse)
def search_attestazioni(
    payload: AttestazioniSearchRequest,
    _: AuthenticatedUser = Depends(require_authenticated_user),
    prepare_adapter: PrepareSearchAdapter = Depends(get_prepare_search_adapter),
) -> AttestazioniSearchResponse:
    """Delegates to current prepare-by-CF and returns a frontend-friendly document DTO."""
    try:
        result = prepare_adapter.prepare_search(
            payload.locatore_cf,
            force_update_visura=payload.force_update_visura,
        )
    except PrepareInputError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid prepare request.",
        ) from exc
    except PrepareNoDataError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No prepared immobili data is available for the requested client.",
        ) from exc
    except PrepareImportFailedError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Prepare could not refresh client data.",
        ) from exc
    except (PrepareGenerationFailedError, PrepareOutputWriteError, FileNotFoundError, YamlInputValidationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Prepared client data could not be loaded safely.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error while preparing client data.",
        ) from exc

    return AttestazioniSearchResponse.from_prepared_result(result)
