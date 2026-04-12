"""Typed models for the canonical single-client `immobili.yml` input contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


ROOT_METADATA_KEYS = {
    "LOCATORE_CF",
    "COMUNE",
    "TIPO_CATASTO",
    "UFFICIO_PROVINCIALE_LABEL",
}

ROOT_PERSISTABLE_KEYS = {
    "LOCATORE_COMUNE_RES",
    "LOCATORE_VIA",
    "LOCATORE_CIVICO",
}

ROOT_ONLY_KEYS = ROOT_METADATA_KEYS | ROOT_PERSISTABLE_KEYS

IMMOBILE_BASE_KEYS = {
    "ENABLED",
    "FOGLIO",
    "NUMERO",
    "SUB",
    "RENDITA",
    "SUPERFICIE_TOTALE",
    "CATEGORIA",
    "VISURA_COMUNE",
    "VISURA_VIA",
    "VISURA_CIVICO",
    "IMMOBILE_COMUNE",
    "IMMOBILE_VIA",
    "IMMOBILE_CIVICO",
    "IMMOBILE_PIANO",
    "IMMOBILE_INTERNO",
    "ENERGY_CLASS",
    "ARREDATO",
    "ISTAT",
    "IGNORE_SURCHARGES",
    "CONTRACT_KIND",
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
}

ELEMENT_KEYS = {f"{prefix}{num}" for prefix in ("A", "B", "C", "D") for num in range(1, 14)}

IMMOBILE_ONLY_KEYS = IMMOBILE_BASE_KEYS | ELEMENT_KEYS


def _get_value(raw: Mapping[str, Any], key: str) -> Any:
    """Reads canonical uppercase keys but tolerates lowercase aliases in YAML."""
    if key in raw:
        return raw[key]
    return raw.get(key.lower())


def _normalize_optional_value(value: Any) -> Any | None:
    """Trims strings and maps empty string values to `None`."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _parse_enabled(value: Any) -> bool:
    """Applies rollout semantics: missing `enabled` means active record."""
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return True
    return raw not in {"0", "false", "no", "n", "off"}


def _extract_extra(raw: Mapping[str, Any], *, known_keys: set[str]) -> dict[str, Any]:
    """Preserves unknown keys without promoting them into the canonical contract."""
    return {
        key: value
        for key, value in raw.items()
        if str(key).strip().upper() not in known_keys
    }


@dataclass(frozen=True)
class ImmobileConfig:
    """Structured representation of one immobile entry inside `immobili.yml`."""

    enabled: bool = True

    foglio: Any | None = None
    numero: Any | None = None
    sub: Any | None = None

    rendita: Any | None = None
    superficie_totale: Any | None = None
    categoria: Any | None = None
    visura_comune: Any | None = None
    visura_via: Any | None = None
    visura_civico: Any | None = None

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

    elements: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any]) -> "ImmobileConfig":
        """Builds a typed immobile config from one YAML mapping."""
        if not isinstance(raw, Mapping):
            raise ValueError("Immobile entry must be a mapping")

        elements = {
            str(key).strip().lower(): _normalize_optional_value(value)
            for key, value in raw.items()
            if str(key).strip().upper() in ELEMENT_KEYS
        }

        return cls(
            enabled=_parse_enabled(_get_value(raw, "ENABLED")),
            foglio=_normalize_optional_value(_get_value(raw, "FOGLIO")),
            numero=_normalize_optional_value(_get_value(raw, "NUMERO")),
            sub=_normalize_optional_value(_get_value(raw, "SUB")),
            rendita=_normalize_optional_value(_get_value(raw, "RENDITA")),
            superficie_totale=_normalize_optional_value(_get_value(raw, "SUPERFICIE_TOTALE")),
            categoria=_normalize_optional_value(_get_value(raw, "CATEGORIA")),
            visura_comune=_normalize_optional_value(_get_value(raw, "VISURA_COMUNE")),
            visura_via=_normalize_optional_value(_get_value(raw, "VISURA_VIA")),
            visura_civico=_normalize_optional_value(_get_value(raw, "VISURA_CIVICO")),
            immobile_comune=_normalize_optional_value(_get_value(raw, "IMMOBILE_COMUNE")),
            immobile_via=_normalize_optional_value(_get_value(raw, "IMMOBILE_VIA")),
            immobile_civico=_normalize_optional_value(_get_value(raw, "IMMOBILE_CIVICO")),
            immobile_piano=_normalize_optional_value(_get_value(raw, "IMMOBILE_PIANO")),
            immobile_interno=_normalize_optional_value(_get_value(raw, "IMMOBILE_INTERNO")),
            energy_class=_normalize_optional_value(_get_value(raw, "ENERGY_CLASS")),
            arredato=_normalize_optional_value(_get_value(raw, "ARREDATO")),
            istat=_normalize_optional_value(_get_value(raw, "ISTAT")),
            ignore_surcharges=_normalize_optional_value(_get_value(raw, "IGNORE_SURCHARGES")),
            contract_kind=_normalize_optional_value(_get_value(raw, "CONTRACT_KIND")),
            conduttore_nome=_normalize_optional_value(_get_value(raw, "CONDUTTORE_NOME")),
            conduttore_cf=_normalize_optional_value(_get_value(raw, "CONDUTTORE_CF")),
            conduttore_comune=_normalize_optional_value(_get_value(raw, "CONDUTTORE_COMUNE")),
            conduttore_via=_normalize_optional_value(_get_value(raw, "CONDUTTORE_VIA")),
            contratto_data=_normalize_optional_value(_get_value(raw, "CONTRATTO_DATA")),
            decorrenza_data=_normalize_optional_value(_get_value(raw, "DECORRENZA_DATA")),
            registrazione_data=_normalize_optional_value(_get_value(raw, "REGISTRAZIONE_DATA")),
            registrazione_num=_normalize_optional_value(_get_value(raw, "REGISTRAZIONE_NUM")),
            agenzia_entrate_sede=_normalize_optional_value(_get_value(raw, "AGENZIA_ENTRATE_SEDE")),
            canone_contrattuale_mensile=_normalize_optional_value(
                _get_value(raw, "CANONE_CONTRATTUALE_MENSILE")
            ),
            durata_anni=_normalize_optional_value(_get_value(raw, "DURATA_ANNI")),
            elements=elements,
            extra=_extract_extra(raw, known_keys=IMMOBILE_ONLY_KEYS),
        )


@dataclass(frozen=True)
class ImmobiliDocumentConfig:
    """Structured representation of the canonical single-client generation document."""

    locatore_cf: str
    comune: Any | None = None
    tipo_catasto: Any | None = None
    ufficio_label: Any | None = None

    locatore_comune_res: Any | None = None
    locatore_via: Any | None = None
    locatore_civico: Any | None = None

    immobili: tuple[ImmobileConfig, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any]) -> "ImmobiliDocumentConfig":
        """Builds the root document model from a validated YAML mapping."""
        if not isinstance(raw, Mapping):
            raise ValueError("Immobili document must be a mapping")

        immobili_raw = raw.get("immobili")
        if not isinstance(immobili_raw, list):
            raise ValueError("Immobili document must contain an 'immobili' list")

        locatore_cf = str(_get_value(raw, "LOCATORE_CF") or "").strip()
        if not locatore_cf:
            raise ValueError("LOCATORE_CF is required")

        return cls(
            locatore_cf=locatore_cf,
            comune=_normalize_optional_value(_get_value(raw, "COMUNE")),
            tipo_catasto=_normalize_optional_value(_get_value(raw, "TIPO_CATASTO")),
            ufficio_label=_normalize_optional_value(_get_value(raw, "UFFICIO_PROVINCIALE_LABEL")),
            locatore_comune_res=_normalize_optional_value(_get_value(raw, "LOCATORE_COMUNE_RES")),
            locatore_via=_normalize_optional_value(_get_value(raw, "LOCATORE_VIA")),
            locatore_civico=_normalize_optional_value(_get_value(raw, "LOCATORE_CIVICO")),
            immobili=tuple(ImmobileConfig.from_raw(entry) for entry in immobili_raw),
            extra=_extract_extra(raw, known_keys=ROOT_ONLY_KEYS | {"IMMOBILI"}),
        )
