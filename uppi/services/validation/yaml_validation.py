"""Validation rules for YAML/client input surface."""

from __future__ import annotations

from typing import Any

from uppi.config.clients import ClientConfig
from uppi.config.immobili import IMMOBILE_ONLY_KEYS, ROOT_METADATA_KEYS, ROOT_ONLY_KEYS
from uppi.services.policies.immobili_generation_policy import (
    IMMOBILE_IDENTITY_FIELDS,
    NON_CLEARABLE_PERSISTABLE_FIELDS,
    VISURA_DISPLAY_FIELDS,
    classify_clear_semantics,
    is_clear_marker,
    is_generation_record_enabled,
    normalized_contract_field_name,
)
from uppi.services.validation.models import ValidationResult
from uppi.utils.parse_utils import safe_float


def _lookup_value(raw: dict[str, Any], key: str) -> Any:
    """Reads canonical uppercase keys but tolerates lowercase aliases in raw YAML."""
    if key in raw:
        return raw[key]
    return raw.get(key.lower())


def _has_key(raw: dict[str, Any], key: str) -> bool:
    """Checks for one canonical key while tolerating lowercase YAML aliases."""
    return key in raw or key.lower() in raw


def _is_blankish(value: Any) -> bool:
    """Treats `None` and empty strings as missing structural values."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _add_forbidden_clear_error(result: ValidationResult, *, field: str, yaml_field: str) -> None:
    """Adds a concrete operator-facing error for unsupported `-` clear targets."""
    if yaml_field in ROOT_METADATA_KEYS:
        message = f"{field} cannot use '-' clear semantics because root metadata must stay explicit."
        code = "immobili_document_forbidden_clear_root_metadata_field"
    elif yaml_field in IMMOBILE_IDENTITY_FIELDS:
        message = f"{field} cannot use '-' clear semantics because identity fields drive strict DB matching."
        code = "immobili_document_forbidden_clear_identity_field"
    elif yaml_field in VISURA_DISPLAY_FIELDS:
        message = f"{field} cannot use '-' clear semantics because visura/display fields come from DB state."
        code = "immobili_document_forbidden_clear_display_field"
    elif yaml_field in NON_CLEARABLE_PERSISTABLE_FIELDS:
        message = f"{field} cannot use '-' clear semantics because {yaml_field} does not support a safe cleared DB state."
        code = "immobili_document_forbidden_clear_non_clearable_persistable_field"
    else:
        message = f"{field} cannot use '-' clear semantics in the canonical immobili.yml contract."
        code = "immobili_document_forbidden_clear_field"

    result.add_error(code, message, field=field)


def _validate_field_clear_semantics(
    result: ValidationResult,
    *,
    raw_mapping: dict[str, Any],
    field_prefix: str,
) -> None:
    """Validates `-` usage against centralized field-class policy tables."""
    for raw_key, raw_value in raw_mapping.items():
        yaml_field = normalized_contract_field_name(raw_key)
        if not yaml_field or not is_clear_marker(raw_value):
            continue

        if classify_clear_semantics(yaml_field) == "forbidden":
            _add_forbidden_clear_error(
                result,
                field=f"{field_prefix}.{yaml_field}" if field_prefix else yaml_field,
                yaml_field=yaml_field,
            )


def _validate_active_identity_fields(
    result: ValidationResult,
    *,
    raw_entry: dict[str, Any],
    field_prefix: str,
) -> None:
    """Requires explicit strict-match identity for each active generation record."""
    for field_name in ("FOGLIO", "NUMERO"):
        if not _has_key(raw_entry, field_name) or _is_blankish(_lookup_value(raw_entry, field_name)):
            result.add_error(
                "immobili_document_active_item_missing_identity",
                f"{field_prefix} must include non-empty {field_name} for active generation.",
                field=f"{field_prefix}.{field_name}",
            )

    if not _has_key(raw_entry, "SUB"):
        result.add_error(
            "immobili_document_active_item_missing_identity",
            f"{field_prefix} must include SUB for active generation, even when the cadastral sub is blank.",
            field=f"{field_prefix}.SUB",
        )


def validate_client_yaml_record(raw: Any) -> ValidationResult:
    """Перевіряє базовий structural contract сирого YAML-запису."""
    result = ValidationResult()
    if not isinstance(raw, dict):
        result.add_error("yaml_record_not_mapping", "Запис клієнта має бути словником.")
        return result

    locatore_cf = str(raw.get("LOCATORE_CF") or raw.get("locatore_cf") or "").strip()
    if not locatore_cf:
        result.add_error("yaml_missing_locatore_cf", "LOCATORE_CF є обов'язковим для запису клієнта.")

    return result


def validate_client_config(client_cfg: ClientConfig) -> ValidationResult:
    """Перевіряє warning-first contract вже нормалізованого клієнтського запису."""
    result = ValidationResult()

    if client_cfg.contract_kind and client_cfg.contract_kind.upper() not in {"CONCORDATO", "TRANSITORIO", "STUDENTI"}:
        result.add_warning(
            "yaml_unknown_contract_kind",
            f"Невідомий CONTRACT_KIND '{client_cfg.contract_kind}' буде оброблено current fallback-логікою.",
            field="contract_kind",
        )

    cadastral_fields = [client_cfg.foglio, client_cfg.numero, client_cfg.sub]
    present_cadastral = sum(1 for value in cadastral_fields if value is not None)
    if 0 < present_cadastral < 3:
        result.add_warning(
            "yaml_partial_cadastral_identity",
            "Частково заповнений кадастровий ідентифікатор може дати неоднозначний match.",
        )

    if client_cfg.istat is not None and client_cfg.istat < 0:
        result.add_warning(
            "yaml_negative_istat",
            "Негативний ISTAT збережеться як current behavior, але виглядає підозріло.",
            field="istat",
        )

    if client_cfg.durata_anni is not None:
        durata = str(client_cfg.durata_anni).strip()
        if durata and durata != "-" and not durata.isdigit():
            result.add_warning(
                "yaml_non_integer_durata",
                f"DURATA_ANNI='{client_cfg.durata_anni}' не виглядає як ціле число.",
                field="durata_anni",
            )

    if client_cfg.energy_class is not None:
        energy = str(client_cfg.energy_class).strip().upper()
        if energy and energy not in {"-", "A", "B", "C", "D", "E", "F", "G"}:
            result.add_warning(
                "yaml_unexpected_energy_class",
                f"ENERGY_CLASS='{client_cfg.energy_class}' не входить до типового діапазону A-G.",
                field="energy_class",
            )

    if client_cfg.canone_contrattuale_mensile is not None and safe_float(client_cfg.canone_contrattuale_mensile) is None:
        result.add_warning(
            "yaml_non_numeric_canone",
            "CANONE_CONTRATTUALE_MENSILE не виглядає як число й буде оброблено current fallback-логікою.",
            field="canone_contrattuale_mensile",
        )

    return result


def validate_immobili_document_yaml(raw: Any) -> ValidationResult:
    """Validates the canonical single-client document shape and field policy usage."""
    result = ValidationResult()

    if not isinstance(raw, dict):
        result.add_error(
            "immobili_document_not_mapping",
            "Single-client immobili.yml must be a root mapping.",
        )
        return result

    _validate_field_clear_semantics(result, raw_mapping=raw, field_prefix="")

    locatore_cf_raw = _lookup_value(raw, "LOCATORE_CF")
    if not is_clear_marker(locatore_cf_raw) and not str(locatore_cf_raw or "").strip():
        result.add_error(
            "immobili_document_missing_locatore_cf",
            "LOCATORE_CF is required at the root level of immobili.yml.",
            field="LOCATORE_CF",
        )

    root_keys = {str(key).strip().upper() for key in raw.keys()}
    misplaced_immobile_fields = sorted((root_keys - {"IMMOBILI"}) & IMMOBILE_ONLY_KEYS)
    if misplaced_immobile_fields:
        result.add_error(
            "immobili_document_root_contains_immobile_fields",
            "Root-level document cannot contain immobile-only fields: "
            + ", ".join(misplaced_immobile_fields),
            field="immobili",
        )

    if "immobili" not in raw:
        result.add_error(
            "immobili_document_missing_immobili",
            "Single-client immobili.yml must contain an 'immobili' list.",
            field="immobili",
        )
        return result

    immobili = raw.get("immobili")
    if not isinstance(immobili, list):
        result.add_error(
            "immobili_document_immobili_not_list",
            "The 'immobili' field must be a list.",
            field="immobili",
        )
        return result

    for index, entry in enumerate(immobili):
        field_name = f"immobili[{index}]"
        if not isinstance(entry, dict):
            result.add_error(
                "immobili_document_item_not_mapping",
                f"{field_name} must be a mapping.",
                field=field_name,
            )
            continue

        entry_keys = {str(key).strip().upper() for key in entry.keys()}
        misplaced_root_fields = sorted(entry_keys & ROOT_ONLY_KEYS)
        if misplaced_root_fields:
            result.add_error(
                "immobili_document_item_contains_root_fields",
                f"{field_name} cannot contain root-only fields: " + ", ".join(misplaced_root_fields),
                field=field_name,
            )

        _validate_field_clear_semantics(result, raw_mapping=entry, field_prefix=field_name)

        if is_generation_record_enabled(entry):
            _validate_active_identity_fields(
                result,
                raw_entry=entry,
                field_prefix=field_name,
            )

    return result
