"""Validation rules для YAML/client input surface."""

from __future__ import annotations

from typing import Any

from uppi.config.clients import ClientConfig
from uppi.config.immobili import IMMOBILE_ONLY_KEYS, ROOT_ONLY_KEYS
from uppi.services.validation.models import ValidationResult
from uppi.utils.parse_utils import safe_float


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
    """Performs structural validation for the single-client `immobili.yml` contract."""
    result = ValidationResult()

    if not isinstance(raw, dict):
        result.add_error(
            "immobili_document_not_mapping",
            "Single-client immobili.yml must be a root mapping.",
        )
        return result

    locatore_cf = str(raw.get("LOCATORE_CF") or raw.get("locatore_cf") or "").strip()
    if not locatore_cf:
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

    return result
