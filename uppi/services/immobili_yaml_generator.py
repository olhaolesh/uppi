"""DB-driven generator and serializer for the canonical single-client `immobili.yml`."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import yaml

from uppi.config.immobili import ImmobileConfig, ImmobiliDocumentConfig
from uppi.domain.db import get_pg_connection
from uppi.domain.exceptions import ImmobiliDocumentNotFoundError
from uppi.services.repositories.prepare_document_repo import (
    PrepareDocumentImmobileRow,
    PrepareDocumentRootRow,
    db_load_prepare_document_elements,
    db_load_prepare_document_immobili,
    db_load_prepare_document_root,
)


ELEMENT_KEY_SEQUENCE = (
    [f"A{i}" for i in range(1, 3)]
    + [f"B{i}" for i in range(1, 6)]
    + [f"C{i}" for i in range(1, 8)]
    + [f"D{i}" for i in range(1, 14)]
)

RUN_ONLY_FIELD_SEQUENCE = (
    "CONDUTTORE_NOME",
    "CONDUTTORE_CF",
    "CONDUTTORE_COMUNE",
    "CONDUTTORE_VIA",
    "CONTRATTO_DATA",
    "DECORRENZA_DATA",
    "REGISTRAZIONE_DATA",
    "REGISTRAZIONE_NUM",
    "AGENZIA_ENTRATE_SEDE",
    "CANONE_CONTRATTUALE_MENSILE",
    "DURATA_ANNI",
)


@dataclass(frozen=True)
class ImmobiliDocumentMetadataDefaults:
    """Explicit defaults for root metadata that do not exist as canonical DB columns."""

    comune: str = "PESCARA"
    tipo_catasto: str = "F"
    ufficio_provinciale_label: str = "PESCARA Territorio"


class ImmobiliYamlGeneratorService:
    """Future prepare-oriented entry point for building/writing one document per CF."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], Any] = get_pg_connection,
        metadata_defaults: ImmobiliDocumentMetadataDefaults | None = None,
    ) -> None:
        self.connection_factory = connection_factory
        self.metadata_defaults = metadata_defaults or ImmobiliDocumentMetadataDefaults()

    def build_document(self, locatore_cf: str) -> ImmobiliDocumentConfig:
        """Builds the in-memory document from DB using an injected connection factory."""
        conn = self.connection_factory()
        try:
            return build_immobili_document_from_db(
                conn,
                locatore_cf,
                metadata_defaults=self.metadata_defaults,
            )
        finally:
            conn.close()

    def dump_yaml(self, locatore_cf: str) -> str:
        """Convenience wrapper that builds and serializes one document."""
        return dump_immobili_document_yaml(self.build_document(locatore_cf))

    def write_yaml(self, locatore_cf: str, path: str | Path) -> Path:
        """Builds the document and writes it to a YAML file."""
        document = self.build_document(locatore_cf)
        return write_immobili_document_yaml(document, path)


def build_immobili_document_from_db(
    conn,
    locatore_cf: str,
    *,
    metadata_defaults: ImmobiliDocumentMetadataDefaults | None = None,
) -> ImmobiliDocumentConfig:
    """Builds the canonical single-client document from DB-only data plus explicit metadata defaults."""
    defaults = metadata_defaults or ImmobiliDocumentMetadataDefaults()
    normalized_cf = str(locatore_cf or "").strip().upper()
    if not normalized_cf:
        raise ImmobiliDocumentNotFoundError(
            "LOCATORE_CF is required to build immobili.yml from DB.",
            details={"locatore_cf": normalized_cf},
        )

    root_row = db_load_prepare_document_root(conn, normalized_cf)
    immobile_rows = db_load_prepare_document_immobili(conn, normalized_cf)

    if not root_row or not immobile_rows:
        raise ImmobiliDocumentNotFoundError(
            f"No DB data found for LOCATORE_CF={normalized_cf}",
            details={
                "locatore_cf": normalized_cf,
                "root_found": bool(root_row),
                "immobili_count": len(immobile_rows),
            },
        )

    elements_by_immobile = db_load_prepare_document_elements(
        conn,
        [row.immobile_id for row in immobile_rows],
    )

    return ImmobiliDocumentConfig(
        locatore_cf=root_row.locatore_cf,
        comune=defaults.comune,
        tipo_catasto=defaults.tipo_catasto,
        ufficio_label=defaults.ufficio_provinciale_label,
        locatore_comune_res=root_row.locatore_comune_res,
        locatore_via=root_row.locatore_via,
        locatore_civico=root_row.locatore_civico,
        immobili=tuple(
            _build_immobile_config(row, elements_by_immobile.get(row.immobile_id, {}))
            for row in immobile_rows
        ),
    )


def serialize_immobili_document(document: ImmobiliDocumentConfig) -> dict[str, Any]:
    """Converts the typed document into a YAML-ready root mapping."""
    serialized = {
        "LOCATORE_CF": _serialize_scalar(document.locatore_cf),
        "COMUNE": _serialize_scalar(document.comune),
        "TIPO_CATASTO": _serialize_scalar(document.tipo_catasto),
        "UFFICIO_PROVINCIALE_LABEL": _serialize_scalar(document.ufficio_label),
        "LOCATORE_COMUNE_RES": _serialize_scalar(document.locatore_comune_res),
        "LOCATORE_VIA": _serialize_scalar(document.locatore_via),
        "LOCATORE_CIVICO": _serialize_scalar(document.locatore_civico),
        "immobili": [_serialize_immobile_entry(immobile) for immobile in document.immobili],
    }

    if document.extra:
        serialized.update(document.extra)

    return serialized


def dump_immobili_document_yaml(document: ImmobiliDocumentConfig) -> str:
    """Serializes the document into deterministic YAML text."""
    return yaml.safe_dump(
        serialize_immobili_document(document),
        sort_keys=False,
        allow_unicode=True,
    )


def write_immobili_document_yaml(document: ImmobiliDocumentConfig, path: str | Path) -> Path:
    """Writes the generated document to disk and returns the resolved path."""
    resolved_path = Path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(dump_immobili_document_yaml(document), encoding="utf-8")
    return resolved_path


def _build_immobile_config(row: PrepareDocumentImmobileRow, elements: dict[str, str]) -> ImmobileConfig:
    """Maps one DB read-model row into the typed immobile config."""
    return ImmobileConfig(
        enabled=True,
        foglio=row.foglio,
        numero=row.numero,
        sub=row.sub,
        rendita=row.rendita,
        superficie_totale=row.superficie_totale,
        categoria=row.categoria,
        visura_comune=row.visura_comune,
        visura_via=row.visura_via,
        visura_civico=row.visura_civico,
        immobile_comune=row.immobile_comune,
        immobile_via=row.immobile_via,
        immobile_civico=row.immobile_civico,
        immobile_piano=row.immobile_piano,
        immobile_interno=row.immobile_interno,
        energy_class=row.energy_class,
        arredato=row.arredato,
        istat=row.istat,
        ignore_surcharges=row.ignore_surcharges,
        contract_kind=row.contract_kind,
        conduttore_nome=None,
        conduttore_cf=None,
        conduttore_comune=None,
        conduttore_via=None,
        contratto_data=None,
        decorrenza_data=None,
        registrazione_data=None,
        registrazione_num=None,
        agenzia_entrate_sede=None,
        canone_contrattuale_mensile=None,
        durata_anni=None,
        elements=elements,
    )


def _serialize_immobile_entry(immobile: ImmobileConfig) -> dict[str, Any]:
    """Converts one typed immobile config into ordered YAML-ready fields."""
    serialized = {
        "enabled": bool(immobile.enabled),
        "FOGLIO": _serialize_scalar(immobile.foglio),
        "NUMERO": _serialize_scalar(immobile.numero),
        "SUB": _serialize_scalar(immobile.sub),
        "RENDITA": _serialize_scalar(immobile.rendita),
        "SUPERFICIE_TOTALE": _serialize_scalar(immobile.superficie_totale),
        "CATEGORIA": _serialize_scalar(immobile.categoria),
        "VISURA_COMUNE": _serialize_scalar(immobile.visura_comune),
        "VISURA_VIA": _serialize_scalar(immobile.visura_via),
        "VISURA_CIVICO": _serialize_scalar(immobile.visura_civico),
        "IMMOBILE_COMUNE": _serialize_scalar(immobile.immobile_comune),
        "IMMOBILE_VIA": _serialize_scalar(immobile.immobile_via),
        "IMMOBILE_CIVICO": _serialize_scalar(immobile.immobile_civico),
        "IMMOBILE_PIANO": _serialize_scalar(immobile.immobile_piano),
        "IMMOBILE_INTERNO": _serialize_scalar(immobile.immobile_interno),
        "ENERGY_CLASS": _serialize_scalar(immobile.energy_class),
        "ARREDATO": _serialize_scalar(immobile.arredato),
        "ISTAT": _serialize_scalar(immobile.istat),
        "IGNORE_SURCHARGES": _serialize_scalar(immobile.ignore_surcharges),
        "CONTRACT_KIND": _serialize_scalar(immobile.contract_kind),
    }

    for element_key in ELEMENT_KEY_SEQUENCE:
        serialized[element_key] = _serialize_scalar(immobile.elements.get(element_key.lower()))

    for run_only_field in RUN_ONLY_FIELD_SEQUENCE:
        serialized[run_only_field] = ""

    if immobile.extra:
        serialized.update(immobile.extra)

    return serialized


def _serialize_scalar(value: Any) -> Any:
    """Normalizes DB/model values into stable YAML-friendly scalars."""
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return float(value)
    return value
