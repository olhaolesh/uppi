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
from uppi.web.api.jobs import get_job_registry
from uppi.web.schemas.attestazioni import (
    AttestazioniGenerateRequest,
    AttestazioniGenerateResponse,
    AttestazioniSearchRequest,
    AttestazioniSearchResponse,
)
from uppi.web.schemas.auth import AuthenticatedUser
from uppi.web.schemas.jobs import JobArtifactResponse
from uppi.web.security import require_authenticated_user
from uppi.web.services.generation_adapter import GenerationAdapter, GenerationRunFailedError
from uppi.web.services.job_registry import JobRegistry
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
    user: AuthenticatedUser = Depends(require_authenticated_user),
    prepare_adapter: PrepareSearchAdapter = Depends(get_prepare_search_adapter),
    job_registry: JobRegistry = Depends(get_job_registry),
) -> AttestazioniSearchResponse:
    """Delegates to current prepare-by-CF and returns a frontend-friendly document DTO."""
    job = job_registry.start_job(
        job_type="attestazioni_search",
        actor_username=user.username,
        input_metadata={
            "locatore_cf": payload.locatore_cf,
            "force_update_visura": payload.force_update_visura,
        },
        started_message="Search started",
    )
    try:
        result = prepare_adapter.prepare_search(
            payload.locatore_cf,
            force_update_visura=payload.force_update_visura,
        )
    except PrepareInputError as exc:
        detail = "Invalid prepare request."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except PrepareNoDataError as exc:
        detail = "No prepared immobili data is available for the requested client."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        ) from exc
    except PrepareImportFailedError as exc:
        detail = "Prepare could not refresh client data."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail,
        ) from exc
    except (PrepareGenerationFailedError, PrepareOutputWriteError, FileNotFoundError, YamlInputValidationError) as exc:
        detail = "Prepared client data could not be loaded safely."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc
    except Exception as exc:
        detail = "Unexpected error while preparing client data."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc

    response = AttestazioniSearchResponse.from_prepared_result(result)
    job_registry.complete_job(
        job.run_id,
        summary={
            "immobili_count": response.document.immobili_count,
            "active_count": response.document.active_count,
            "source": response.source,
        },
        artifacts=[
            JobArtifactResponse(
                kind="prepared_immobili_yaml",
                label="Prepared immobili.yml",
                local_path=response.document.immobili_yaml_path,
            )
        ],
        messages=list(response.messages),
        completion_message="Search completed",
    )
    return response


@router.post("/generate", response_model=AttestazioniGenerateResponse)
def generate_attestazioni(
    payload: AttestazioniGenerateRequest,
    user: AuthenticatedUser = Depends(require_authenticated_user),
    generation_adapter: GenerationAdapter = Depends(get_generation_adapter),
    job_registry: JobRegistry = Depends(get_job_registry),
) -> AttestazioniGenerateResponse:
    """Builds a web-run generation YAML and delegates to the current generation-only path."""
    job = job_registry.start_job(
        job_type="attestazioni_generate",
        actor_username=user.username,
        input_metadata={
            "locatore_cf": payload.locatore_cf,
            "prepared_immobili_yaml_path": payload.prepared_immobili_yaml_path
            or f"clients/web_prepare/{payload.locatore_cf}/immobili.yml",
        },
        started_message="Generation started",
    )
    try:
        result = generation_adapter.generate(payload, run_id=job.run_id)
    except UnsafePreparedYamlPathError as exc:
        detail = "Prepared YAML path is outside the allowed web_prepare workspace."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except FileNotFoundError as exc:
        detail = "Prepared immobili YAML was not found."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        ) from exc
    except (NoSelectedImmobilesError, PreparedImmobileIndexNotFoundError) as exc:
        detail = str(exc)
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except (PreparedDocumentClientMismatchError, PreparedImmobileIdentityMismatchError, GenerationPrepareRequiredError) as exc:
        detail = (
            "Prepared data no longer matches the current generation context. "
            "Run search/prepare again."
        )
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail,
        ) from exc
    except YamlInputValidationError as exc:
        detail = "Generation YAML validation failed for the requested edits."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=detail,
        ) from exc
    except GenerationRunFailedError as exc:
        detail = "Generation failed before any artifact could be produced."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc
    except Exception as exc:
        detail = "Unexpected error while generating attestazioni."
        job_registry.fail_job(job.run_id, safe_message=detail)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        ) from exc

    response = AttestazioniGenerateResponse.from_generated_result(result)
    job_status = "partial" if result.generated_count > 0 and result.failed_count > 0 else "completed"
    completion_message = (
        "Generation completed with failures"
        if job_status == "partial"
        else "Generation completed"
    )
    job_registry.complete_job(
        job.run_id,
        status=job_status,
        summary={
            "requested_count": response.summary.requested_count,
            "generated_count": response.summary.generated_count,
            "failed_count": response.summary.failed_count,
        },
        artifacts=[
            JobArtifactResponse(
                kind="generation_immobili_yaml",
                label="Generation immobili.yml",
                local_path=response.input.generation_immobili_yaml_path,
            ),
            *[
                JobArtifactResponse(
                    kind=artifact.kind,
                    label=(
                        f"Attestazione F{artifact.identity.foglio} "
                        f"N{artifact.identity.numero} S{artifact.identity.sub}"
                    ),
                    local_path=artifact.local_path,
                    bucket=artifact.bucket,
                    object_key=artifact.object_key,
                    download_url=artifact.download_url,
                )
                for artifact in response.artifacts
            ],
        ],
        messages=list(response.messages),
        completion_message=completion_message,
        event_level="warning" if job_status == "partial" else "info",
    )
    return response
