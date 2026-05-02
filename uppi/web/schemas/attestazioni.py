"""Schemas for the Stage 3 attestazioni search/prepare web API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from uppi.web.services.prepare_adapter import PreparedSearchResult


def _blankable(value: Any) -> Any:
    """Serializes `None` as blank string while preserving booleans and numbers."""
    if value is None:
        return ""
    return value


class AttestazioniSearchRequest(BaseModel):
    """Protected request payload for `POST /attestazioni/search`."""

    locatore_cf: str = Field(..., description="Codice fiscale del locatore.")
    force_update_visura: bool = False

    @field_validator("locatore_cf", mode="before")
    @classmethod
    def _normalize_locatore_cf(cls, value: Any) -> str:
        """Trims and uppercases the incoming codice fiscale."""
        return str(value or "").strip().upper()

    @field_validator("locatore_cf")
    @classmethod
    def _validate_locatore_cf(cls, value: str) -> str:
        """Applies the same lightweight CF format expectations as current prepare input."""
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
        """Builds the response DTO from the prepared search adapter result."""
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
