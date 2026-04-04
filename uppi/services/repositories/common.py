"""Спільні helper-и й константи для thin repository-модулів.

Тут залишаються лише нейтральні утиліти підготовки даних і константи,
які використовуються кількома repo-модулями. Patch/business semantics
винесені в `uppi.services.policies.*`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from uppi.domain.immobile import Immobile
from uppi.utils.parse_utils import clean_str, clean_sub, safe_float

# Зберігаємо історичне ім'я логера, щоб split не міняв logging surface.
logger = logging.getLogger("uppi.services.db_repo")

# Константи для ключів елементів (A1..D13)
ELEMENT_KEYS = (
    ["a1", "a2"]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)

# Список колонок, які ми очікуємо отримати з парсера/Immobile об'єкта.
IMMOBILI_PARSED_COLUMNS = [
    "table_num_immobile",
    "sez_urbana",
    "foglio",
    "numero",
    "sub",
    "zona_cens",
    "micro_zona",
    "categoria",
    "classe",
    "consistenza",
    "rendita",
    "superficie_totale",
    "superficie_escluse",
    "superficie_raw",
    "immobile_comune",
    "via_type",
    "via_name",
    "via_num",
    "scala",
    "interno",
    "piano",
    "indirizzo_raw",
    "dati_ulteriori",
]


def immobile_from_parsed_dict(data: Dict[str, Any]) -> Immobile:
    """Створює `Immobile` зі словника парсера з базовою конверсією площ."""
    d = data.copy()
    if "superficie_totale" in d:
        d["superficie_totale"] = safe_float(d.get("superficie_totale"))
    if "superficie_escluse" in d:
        d["superficie_escluse"] = safe_float(d.get("superficie_escluse"))
    return Immobile(**d)


def immobile_db_row(imm: Immobile) -> Dict[str, Any]:
    """Нормалізує `Immobile` до словника, сумісного з поточним SQL-вставленням."""
    row: Dict[str, Any] = {}

    for col in IMMOBILI_PARSED_COLUMNS:
        raw = getattr(imm, col, None)

        if col == "sub":
            row[col] = clean_sub(raw)
            continue

        if col in ("superficie_totale", "superficie_escluse"):
            row[col] = safe_float(raw)
            continue

        row[col] = clean_str(raw)

    if row.get("foglio") is not None:
        row["foglio"] = str(row["foglio"]).strip()
    if row.get("numero") is not None:
        row["numero"] = str(row["numero"]).strip()
    if row.get("sub") is None:
        row["sub"] = ""

    return row
