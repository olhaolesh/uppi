"""Map the canonical single-client `immobili.yml` document into generation items."""

from __future__ import annotations

from typing import Any

from uppi.config.immobili import ImmobileConfig, ImmobiliDocumentConfig
from uppi.services.policies.immobili_generation_policy import normalize_run_only_item_value


ELEMENT_KEYS = (
    [f"a{i}" for i in range(1, 3)]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)


def map_immobili_document_to_item(
    document: ImmobiliDocumentConfig,
    immobile: ImmobileConfig,
) -> dict[str, Any]:
    """Build one generation item from root-level YAML data plus one immobile entry."""
    mapped = {
        "locatore_cf": document.locatore_cf,
        "codice_fiscale": document.locatore_cf,
        "comune": document.comune,
        "tipo_catasto": document.tipo_catasto,
        "ufficio_label": document.ufficio_label,
        "locatore_comune_res": document.locatore_comune_res,
        "locatore_via": document.locatore_via,
        "locatore_civico": document.locatore_civico,
        "foglio": immobile.foglio,
        "numero": immobile.numero,
        "sub": immobile.sub,
        "rendita": immobile.rendita,
        "superficie_totale": immobile.superficie_totale,
        "categoria": immobile.categoria,
        "immobile_comune": immobile.immobile_comune,
        "immobile_via": immobile.immobile_via,
        "immobile_civico": immobile.immobile_civico,
        "immobile_piano": immobile.immobile_piano,
        "immobile_interno": immobile.immobile_interno,
        "energy_class": immobile.energy_class,
        "arredato": immobile.arredato,
        "istat": immobile.istat,
        "ignore_surcharges": immobile.ignore_surcharges,
        "contract_kind": immobile.contract_kind,
        "contratto_data": immobile.contratto_data,
        "conduttore_nome": immobile.conduttore_nome,
        "conduttore_cf": immobile.conduttore_cf,
        "conduttore_comune": immobile.conduttore_comune,
        "conduttore_via": immobile.conduttore_via,
        "decorrenza_data": immobile.decorrenza_data,
        "registrazione_data": immobile.registrazione_data,
        "registrazione_num": immobile.registrazione_num,
        "agenzia_entrate_sede": immobile.agenzia_entrate_sede,
        "canone_contrattuale_mensile": immobile.canone_contrattuale_mensile,
        "durata_anni": immobile.durata_anni,
    }

    for element_key in ELEMENT_KEYS:
        mapped[element_key] = immobile.elements.get(element_key)

    extra: dict[str, Any] = {
        "enabled": immobile.enabled,
        "VISURA_COMUNE": immobile.visura_comune,
        "VISURA_VIA": immobile.visura_via,
        "VISURA_CIVICO": immobile.visura_civico,
    }
    if document.extra:
        extra.update(document.extra)
    if immobile.extra:
        extra.update(immobile.extra)
    if extra:
        mapped["extra"] = extra

    return {
        field_name: normalize_run_only_item_value(field_name, value)
        for field_name, value in mapped.items()
    }
