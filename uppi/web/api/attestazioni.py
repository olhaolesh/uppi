"""Stage 3 protected search/prepare endpoint for attestazioni workflows."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from uppi.domain.exceptions import (
    GenerationPrepareRequiredError,
    PrepareGenerationFailedError,
    PrepareImportFailedError,
    PrepareInputError,
    PrepareNoDataError,
    PrepareOutputWriteError,
    YamlInputValidationError,
)
from uppi.web.schemas.attestazioni import (
    AttestazioniGenerateRequest,
    AttestazioniGenerateResponse,
    AttestazioniSearchRequest,
    AttestazioniSearchResponse,
)
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.security import require_authenticated_user
from uppi.web.services.generation_adapter import GenerationAdapter, GenerationRunFailedError
from uppi.web.services.generation_yaml_builder import (
    NoSelectedImmobilesError,
    PreparedDocumentClientMismatchError,
    PreparedImmobileIdentityMismatchError,
    PreparedImmobileIndexNotFoundError,
    UnsafePreparedYamlPathError,
)
from uppi.web.services.prepare_adapter import PrepareSearchAdapter

router = APIRouter(prefix="/attestazioni", tags=["attestazioni"])


def get_prepare_search_adapter(request: Request) -> PrepareSearchAdapter:
    """Returns the app-scoped adapter or creates the default one lazily."""
    adapter = getattr(request.app.state, "prepare_search_adapter", None)
    if adapter is None:
        adapter = PrepareSearchAdapter()
        request.app.state.prepare_search_adapter = adapter
    return adapter


def get_generation_adapter(request: Request) -> GenerationAdapter:
    """Returns the app-scoped generation adapter or creates the default one lazily."""
    adapter = getattr(request.app.state, "generation_adapter", None)
    if adapter is None:
        adapter = GenerationAdapter()
        request.app.state.generation_adapter = adapter
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


@router.post("/generate", response_model=AttestazioniGenerateResponse)
def generate_attestazioni(
    payload: AttestazioniGenerateRequest,
    _: AuthenticatedUser = Depends(require_authenticated_user),
    generation_adapter: GenerationAdapter = Depends(get_generation_adapter),
) -> AttestazioniGenerateResponse:
    """Builds a web-run generation YAML and delegates to the current generation-only path."""
    try:
        result = generation_adapter.generate(payload)
    except UnsafePreparedYamlPathError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Prepared YAML path is outside the allowed web_prepare workspace.",
        ) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Prepared immobili YAML was not found.",
        ) from exc
    except (NoSelectedImmobilesError, PreparedImmobileIndexNotFoundError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except (PreparedDocumentClientMismatchError, PreparedImmobileIdentityMismatchError, GenerationPrepareRequiredError) as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Prepared data no longer matches the current generation context. "
                "Run search/prepare again."
            ),
        ) from exc
    except YamlInputValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Generation YAML validation failed for the requested edits.",
        ) from exc
    except GenerationRunFailedError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Generation failed before any artifact could be produced.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error while generating attestazioni.",
        ) from exc

    return AttestazioniGenerateResponse.from_generated_result(result)
