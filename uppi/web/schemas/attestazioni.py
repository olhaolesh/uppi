"""Schemas for the Stage 3 and Stage 4 attestazioni web API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from uppi.config.immobili import ELEMENT_KEYS

if TYPE_CHECKING:
    from uppi.web.services.generation_adapter import GeneratedRunResult
    from uppi.web.services.prepare_adapter import PreparedSearchResult


ALLOWED_ELEMENT_KEYS = {element_key.lower() for element_key in ELEMENT_KEYS}


def _blankable(value: Any) -> Any:
    """Serializes `None` as blank string while preserving booleans and numbers."""
    if value is None:
        return ""
    return value


class _StrictWebModel(BaseModel):
    """Base model for additive web DTOs that reject unknown fields."""

    model_config = ConfigDict(extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def _strip_strings(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip()
        return value


class AttestazioniSearchRequest(_StrictWebModel):
    """Protected request payload for `POST /attestazioni/search`."""

    locatore_cf: str = Field(..., description="Codice fiscale del locatore.")
    force_update_visura: bool = False

    @field_validator("locatore_cf", mode="before")
    @classmethod
    def _normalize_locatore_cf(cls, value: Any) -> str:
        return str(value or "").strip().upper()

    @field_validator("locatore_cf")
    @classmethod
    def _validate_locatore_cf(cls, value: str) -> str:
        if not value:
            raise ValueError("locatore_cf is required")
        if len(value) != 16 or not value.isalnum():
            raise ValueError("locatore_cf must be a 16-character alphanumeric codice fiscale")
        return value


class AttestazioniSearchClientResponse(BaseModel):
    """Client-level document metadata and persistable root fields."""

    locatore_cf: str
    comune: str
    tipo_catasto: str
    ufficio_label: str
    locatore_comune_res: str
    locatore_via: str
    locatore_civico: str


class AttestazioniSearchDocumentResponse(BaseModel):
    """Document-level metadata returned after prepare succeeded."""

    immobili_yaml_path: str
    immobili_count: int
    active_count: int


class AttestazioniImmobileIdentityResponse(BaseModel):
    """Identity fields for one immobile entry."""

    foglio: Any
    numero: Any
    sub: Any


class AttestazioniImmobileVisuraResponse(BaseModel):
    """Visura/display fields for one immobile entry."""

    rendita: Any
    superficie_totale: Any
    categoria: Any
    visura_comune: Any
    visura_via: Any
    visura_civico: Any


class AttestazioniImmobileEditableResponse(BaseModel):
    """Editable persistable fields for one immobile entry."""

    immobile_comune: Any
    immobile_via: Any
    immobile_civico: Any
    immobile_piano: Any
    immobile_interno: Any
    energy_class: Any
    arredato: Any
    istat: Any
    ignore_surcharges: Any
    contract_kind: Any


class AttestazioniImmobileRunOnlyResponse(BaseModel):
    """Run-only fields for one immobile entry."""

    conduttore_nome: Any
    conduttore_cf: Any
    conduttore_comune: Any
    conduttore_via: Any
    contratto_data: Any
    decorrenza_data: Any
    registrazione_data: Any
    registrazione_num: Any
    agenzia_entrate_sede: Any
    canone_contrattuale_mensile: Any
    durata_anni: Any


class AttestazioniImmobileResponse(BaseModel):
    """Frontend-friendly grouped view of one prepared immobile."""

    index: int
    enabled: bool
    identity: AttestazioniImmobileIdentityResponse
    visura: AttestazioniImmobileVisuraResponse
    editable: AttestazioniImmobileEditableResponse
    run_only: AttestazioniImmobileRunOnlyResponse
    elements: dict[str, Any]


class AttestazioniSearchResponse(BaseModel):
    """Protected response shape for the Stage 3 search/prepare adapter endpoint."""

    status: Literal["prepared"]
    source: Literal["db", "sister", "unknown"]
    client: AttestazioniSearchClientResponse
    document: AttestazioniSearchDocumentResponse
    immobili: list[AttestazioniImmobileResponse]
    messages: list[str]

    @classmethod
    def from_prepared_result(cls, result: "PreparedSearchResult") -> "AttestazioniSearchResponse":
        document = result.document
        immobili = list(document.immobili)
        return cls(
            status="prepared",
            source=result.source if result.source in {"db", "sister", "unknown"} else "unknown",
            client=AttestazioniSearchClientResponse(
                locatore_cf=document.locatore_cf,
                comune=str(_blankable(document.comune)),
                tipo_catasto=str(_blankable(document.tipo_catasto)),
                ufficio_label=str(_blankable(document.ufficio_label)),
                locatore_comune_res=str(_blankable(document.locatore_comune_res)),
                locatore_via=str(_blankable(document.locatore_via)),
                locatore_civico=str(_blankable(document.locatore_civico)),
            ),
            document=AttestazioniSearchDocumentResponse(
                immobili_yaml_path=result.output_path_relative,
                immobili_count=len(immobili),
                active_count=sum(1 for immobile in immobili if immobile.enabled),
            ),
            immobili=[
                AttestazioniImmobileResponse(
                    index=index,
                    enabled=immobile.enabled,
                    identity=AttestazioniImmobileIdentityResponse(
                        foglio=_blankable(immobile.foglio),
                        numero=_blankable(immobile.numero),
                        sub=_blankable(immobile.sub),
                    ),
                    visura=AttestazioniImmobileVisuraResponse(
                        rendita=_blankable(immobile.rendita),
                        superficie_totale=_blankable(immobile.superficie_totale),
                        categoria=_blankable(immobile.categoria),
                        visura_comune=_blankable(immobile.visura_comune),
                        visura_via=_blankable(immobile.visura_via),
                        visura_civico=_blankable(immobile.visura_civico),
                    ),
                    editable=AttestazioniImmobileEditableResponse(
                        immobile_comune=_blankable(immobile.immobile_comune),
                        immobile_via=_blankable(immobile.immobile_via),
                        immobile_civico=_blankable(immobile.immobile_civico),
                        immobile_piano=_blankable(immobile.immobile_piano),
                        immobile_interno=_blankable(immobile.immobile_interno),
                        energy_class=_blankable(immobile.energy_class),
                        arredato=_blankable(immobile.arredato),
                        istat=_blankable(immobile.istat),
                        ignore_surcharges=_blankable(immobile.ignore_surcharges),
                        contract_kind=_blankable(immobile.contract_kind),
                    ),
                    run_only=AttestazioniImmobileRunOnlyResponse(
                        conduttore_nome=_blankable(immobile.conduttore_nome),
                        conduttore_cf=_blankable(immobile.conduttore_cf),
                        conduttore_comune=_blankable(immobile.conduttore_comune),
                        conduttore_via=_blankable(immobile.conduttore_via),
                        contratto_data=_blankable(immobile.contratto_data),
                        decorrenza_data=_blankable(immobile.decorrenza_data),
                        registrazione_data=_blankable(immobile.registrazione_data),
                        registrazione_num=_blankable(immobile.registrazione_num),
                        agenzia_entrate_sede=_blankable(immobile.agenzia_entrate_sede),
                        canone_contrattuale_mensile=_blankable(immobile.canone_contrattuale_mensile),
                        durata_anni=_blankable(immobile.durata_anni),
                    ),
                    elements={
                        key: _blankable(value)
                        for key, value in sorted(immobile.elements.items())
                    },
                )
                for index, immobile in enumerate(immobili, start=1)
            ],
            messages=list(result.messages),
        )


class AttestazioniGenerateClientUpdates(_StrictWebModel):
    """Allowed root-level editable fields for one generation web run."""

    locatore_comune_res: str | None = None
    locatore_via: str | None = None
    locatore_civico: str | None = None


class AttestazioniGenerateIdentity(_StrictWebModel):
    """Immutable identity echo used to bind operator edits to prepared YAML rows."""

    foglio: Any
    numero: Any
    sub: Any


class AttestazioniGenerateEditable(_StrictWebModel):
    """Allowed immobile-level persistable edits."""

    immobile_comune: Any | None = None
    immobile_via: Any | None = None
    immobile_civico: Any | None = None
    immobile_piano: Any | None = None
    immobile_interno: Any | None = None
    energy_class: Any | None = None
    arredato: Any | None = None
    istat: Any | None = None
    ignore_surcharges: Any | None = None
    contract_kind: Any | None = None


class AttestazioniGenerateRunOnly(_StrictWebModel):
    """Allowed run-only fields for one generation web run."""

    conduttore_nome: Any | None = None
    conduttore_cf: Any | None = None
    conduttore_comune: Any | None = None
    conduttore_via: Any | None = None
    contratto_data: Any | None = None
    decorrenza_data: Any | None = None
    registrazione_data: Any | None = None
    registrazione_num: Any | None = None
    agenzia_entrate_sede: Any | None = None
    canone_contrattuale_mensile: Any | None = None
    durata_anni: Any | None = None


class AttestazioniGenerateImmobileRequest(_StrictWebModel):
    """One operator-edited immobile row sent to `POST /attestazioni/generate`."""

    index: int = Field(..., ge=1)
    enabled: bool = True
    identity: AttestazioniGenerateIdentity
    editable: AttestazioniGenerateEditable = Field(default_factory=AttestazioniGenerateEditable)
    run_only: AttestazioniGenerateRunOnly = Field(default_factory=AttestazioniGenerateRunOnly)
    elements: dict[str, Any] = Field(default_factory=dict)

    @field_validator("elements", mode="before")
    @classmethod
    def _normalize_elements(cls, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("elements must be an object")
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key or "").strip().lower()
            if key not in ALLOWED_ELEMENT_KEYS:
                raise ValueError(f"Unsupported element key: {raw_key}")
            normalized[key] = raw_value.strip() if isinstance(raw_value, str) else raw_value
        return normalized


class AttestazioniGenerateRequest(_StrictWebModel):
    """Protected request payload for `POST /attestazioni/generate`."""

    locatore_cf: str
    prepared_immobili_yaml_path: str | None = None
    client_updates: AttestazioniGenerateClientUpdates = Field(default_factory=AttestazioniGenerateClientUpdates)
    immobili: list[AttestazioniGenerateImmobileRequest]

    @field_validator("locatore_cf", mode="before")
    @classmethod
    def _normalize_locatore_cf(cls, value: Any) -> str:
        return str(value or "").strip().upper()

    @field_validator("locatore_cf")
    @classmethod
    def _validate_locatore_cf(cls, value: str) -> str:
        if not value:
            raise ValueError("locatore_cf is required")
        if len(value) != 16 or not value.isalnum():
            raise ValueError("locatore_cf must be a 16-character alphanumeric codice fiscale")
        return value

    @field_validator("prepared_immobili_yaml_path", mode="before")
    @classmethod
    def _normalize_prepared_yaml_path(cls, value: Any) -> str | None:
        if value is None:
            return None
        raw = str(value).strip()
        return raw or None

    @model_validator(mode="after")
    def _validate_selected_immobili(self) -> "AttestazioniGenerateRequest":
        if not self.immobili:
            raise ValueError("immobili must contain at least one requested row")
        indexes = [entry.index for entry in self.immobili]
        if len(set(indexes)) != len(indexes):
            raise ValueError("immobili indexes must be unique")
        if not any(entry.enabled for entry in self.immobili):
            raise ValueError("at least one immobile must be enabled for generation")
        return self


class AttestazioniGenerationInputResponse(BaseModel):
    """Safe references to prepared and generated YAML inputs."""

    prepared_immobili_yaml_path: str
    generation_immobili_yaml_path: str


class AttestazioniGenerationSummaryResponse(BaseModel):
    """Synchronous generation counters for the MVP response."""

    requested_count: int
    generated_count: int
    failed_count: int


class AttestazioniGenerationArtifactResponse(BaseModel):
    """Safe synchronous artifact reference for one generated document."""

    index: int
    identity: AttestazioniImmobileIdentityResponse
    kind: Literal["attestazione_docx"]
    local_path: str | None
    bucket: str | None
    object_key: str | None
    download_url: None = None


class AttestazioniGenerateResponse(BaseModel):
    """Protected synchronous response shape for `POST /attestazioni/generate`."""

    status: Literal["generated"]
    run_id: str
    locatore_cf: str
    input: AttestazioniGenerationInputResponse
    summary: AttestazioniGenerationSummaryResponse
    artifacts: list[AttestazioniGenerationArtifactResponse]
    messages: list[str]

    @classmethod
    def from_generated_result(cls, result: "GeneratedRunResult") -> "AttestazioniGenerateResponse":
        return cls(
            status="generated",
            run_id=result.run_id,
            locatore_cf=result.locatore_cf,
            input=AttestazioniGenerationInputResponse(
                prepared_immobili_yaml_path=result.prepared_output_path_relative,
                generation_immobili_yaml_path=result.generation_output_path_relative,
            ),
            summary=AttestazioniGenerationSummaryResponse(
                requested_count=result.requested_count,
                generated_count=result.generated_count,
                failed_count=result.failed_count,
            ),
            artifacts=[
                AttestazioniGenerationArtifactResponse(
                    index=artifact.index,
                    identity=AttestazioniImmobileIdentityResponse(
                        foglio=_blankable(artifact.foglio),
                        numero=_blankable(artifact.numero),
                        sub=_blankable(artifact.sub),
                    ),
                    kind="attestazione_docx",
                    local_path=artifact.local_path,
                    bucket=artifact.bucket,
                    object_key=artifact.object_key,
                    download_url=None,
                )
                for artifact in result.artifacts
            ],
            messages=list(result.messages),
        )


__all__ = [
    "AttestazioniGenerateRequest",
    "AttestazioniGenerateResponse",
    "AttestazioniSearchRequest",
    "AttestazioniSearchResponse",
]
