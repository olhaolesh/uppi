"""Central field policy for the canonical single-client `immobili.yml` contract."""

from __future__ import annotations

from typing import Any

from uppi.config.immobili import ELEMENT_KEYS, ROOT_METADATA_KEYS, ROOT_PERSISTABLE_KEYS


CLEAR_MARKER = "-"

IMMOBILE_IDENTITY_FIELDS = frozenset({"FOGLIO", "NUMERO", "SUB"})
VISURA_DISPLAY_FIELDS = frozenset(
    {
        "RENDITA",
        "SUPERFICIE_TOTALE",
        "CATEGORIA",
        "VISURA_COMUNE",
        "VISURA_VIA",
        "VISURA_CIVICO",
    }
)
IMMOBILE_OVERRIDE_FIELDS = frozenset(
    {
        "IMMOBILE_COMUNE",
        "IMMOBILE_VIA",
        "IMMOBILE_CIVICO",
        "IMMOBILE_PIANO",
        "IMMOBILE_INTERNO",
    }
)
PERSISTABLE_TECHNICAL_CLEARABLE_FIELDS = frozenset(
    {
        "ENERGY_CLASS",
        "ARREDATO",
        "ISTAT",
        "IGNORE_SURCHARGES",
    }
)
NON_CLEARABLE_PERSISTABLE_FIELDS = frozenset({"CONTRACT_KIND"})
RUN_ONLY_FIELDS = frozenset(
    {
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
)

# Persistable clearable fields write their cleared state back to DB. Run-only
# fields clear only the current generation context. Metadata, identity, visura
# display fields, and CONTRACT_KIND reject "-" entirely.
DB_CLEARABLE_FIELDS = frozenset(ROOT_PERSISTABLE_KEYS) | IMMOBILE_OVERRIDE_FIELDS | PERSISTABLE_TECHNICAL_CLEARABLE_FIELDS | frozenset(ELEMENT_KEYS)
FORBIDDEN_CLEAR_FIELDS = (
    frozenset(ROOT_METADATA_KEYS)
    | IMMOBILE_IDENTITY_FIELDS
    | VISURA_DISPLAY_FIELDS
    | NON_CLEARABLE_PERSISTABLE_FIELDS
)
CANONICAL_POLICY_FIELDS = DB_CLEARABLE_FIELDS | FORBIDDEN_CLEAR_FIELDS | RUN_ONLY_FIELDS

ROOT_METADATA_ITEM_FIELDS = frozenset(
    {
        "locatore_cf",
        "codice_fiscale",
        "comune",
        "tipo_catasto",
        "ufficio_label",
    }
)
RUN_ONLY_ITEM_FIELDS = frozenset(
    {
        "conduttore_nome",
        "conduttore_cf",
        "conduttore_comune",
        "conduttore_via",
        "contratto_data",
        "decorrenza_data",
        "registrazione_data",
        "registrazione_num",
        "agenzia_entrate_sede",
        "canone_contrattuale_mensile",
        "durata_anni",
    }
)
DB_CLEARABLE_ITEM_FIELDS = frozenset(
    {
        "locatore_comune_res",
        "locatore_via",
        "locatore_civico",
        "immobile_comune",
        "immobile_via",
        "immobile_civico",
        "immobile_piano",
        "immobile_interno",
        "energy_class",
        "arredato",
        "istat",
        "ignore_surcharges",
    }
) | frozenset({element_key.lower() for element_key in ELEMENT_KEYS})


def is_clear_marker(value: Any) -> bool:
    """Returns `True` when the YAML value requests explicit clear semantics."""
    return value is not None and str(value).strip() == CLEAR_MARKER


def is_generation_record_enabled(raw_entry: dict[str, Any]) -> bool:
    """Applies rollout semantics: missing `enabled` means active generation record."""
    raw_value = raw_entry.get("ENABLED", raw_entry.get("enabled"))
    if raw_value is None:
        return True
    if isinstance(raw_value, bool):
        return raw_value

    normalized = str(raw_value).strip().lower()
    if not normalized:
        return True
    return normalized not in {"0", "false", "no", "n", "off"}


def normalized_contract_field_name(field_name: Any) -> str:
    """Returns the canonical uppercase YAML field name used by policy checks."""
    return str(field_name or "").strip().upper()


def normalize_run_only_item_value(field_name: str, value: Any) -> Any:
    """Converts run-only clear markers into blank current-run state."""
    if field_name in RUN_ONLY_ITEM_FIELDS and is_clear_marker(value):
        return None
    return value


def classify_clear_semantics(field_name: Any) -> str:
    """Returns one of `db_clear`, `run_clear`, or `forbidden` for `-` usage."""
    normalized = normalized_contract_field_name(field_name)
    if normalized not in CANONICAL_POLICY_FIELDS:
        return "unknown"
    if normalized in DB_CLEARABLE_FIELDS:
        return "db_clear"
    if normalized in RUN_ONLY_FIELDS:
        return "run_clear"
    return "forbidden"
