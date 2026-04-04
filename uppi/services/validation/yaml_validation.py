"""Validation rules для YAML/client input surface."""

from __future__ import annotations

from typing import Any

from uppi.config.clients import ClientConfig
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
