"""Validation rules для surface підготовки `CanoneInput`."""

from __future__ import annotations

from typing import Any

from uppi.domain.canone_models import CanoneInput
from uppi.services.validation.models import ValidationResult


def validate_canone_input(candidate: Any) -> ValidationResult:
    """Перевіряє базовий contract `CanoneInput` без зміни calculation logic."""
    result = ValidationResult()

    if not isinstance(candidate, CanoneInput):
        result.add_error("canone_input_wrong_type", "Очікувався об'єкт CanoneInput.")
        return result

    if candidate.superficie_catastale <= 0:
        result.add_warning(
            "canone_non_positive_surface",
            "superficie_catastale <= 0; current calculation path може дати підозрілий результат.",
            field="superficie_catastale",
        )

    if not candidate.micro_zona and not candidate.foglio:
        result.add_warning(
            "canone_missing_location_markers",
            "Відсутні і micro_zona, і foglio; зона може визначатися неповно.",
        )

    if not candidate.categoria_catasto:
        result.add_warning(
            "canone_missing_categoria",
            "Відсутня categoria_catasto.",
            field="categoria_catasto",
        )

    if not candidate.classe_catasto:
        result.add_warning(
            "canone_missing_classe",
            "Відсутня classe_catasto.",
            field="classe_catasto",
        )

    if candidate.durata_anni <= 0:
        result.add_warning(
            "canone_non_positive_durata",
            "durata_anni <= 0 виглядає підозріло для current canone preparation.",
            field="durata_anni",
        )

    if candidate.istat is not None and candidate.istat < 0:
        result.add_warning(
            "canone_negative_istat",
            "Негативний ISTAT виглядає підозріло, але лишається warning-first.",
            field="istat",
        )

    return result
