"""Validation rules для parser output surface після завантаження visura."""

from __future__ import annotations

from typing import Any

from uppi.services.validation.models import ValidationResult


def validate_parsed_visura_output(parsed: Any) -> ValidationResult:
    """Перевіряє базовий shape parsed output без нормалізації parser behavior."""
    result = ValidationResult()

    if not isinstance(parsed, list):
        result.add_error("parser_output_not_list", "Parser output має бути списком записів.")
        return result

    for idx, item in enumerate(parsed):
        if not isinstance(item, dict):
            result.add_error("parser_item_not_mapping", f"Parser item #{idx} має бути словником.")
            continue

        foglio = item.get("foglio")
        numero = item.get("numero")
        if not foglio or not numero:
            result.add_warning(
                "parser_missing_cadastral_identity",
                f"Parser item #{idx} не має повного foglio/numero й може не пройти далі current pipeline.",
            )

        if not item.get("immobile_comune"):
            result.add_warning(
                "parser_missing_immobile_comune",
                f"Parser item #{idx} не має immobile_comune.",
            )

        if not (item.get("via_name") or item.get("indirizzo_raw")):
            result.add_warning(
                "parser_missing_address_text",
                f"Parser item #{idx} не має ані via_name, ані indirizzo_raw.",
            )

    return result
