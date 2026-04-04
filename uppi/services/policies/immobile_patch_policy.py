"""Pure-function policy rules для `immobile` patch/update semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, List, Optional, Sequence, Tuple

from itemadapter import ItemAdapter

from uppi.utils.parse_utils import clean_str


@dataclass(frozen=True)
class ElementMutation:
    """Описує одну мутацію елемента нерухомості без прив'язки до SQL."""

    action: str
    grp: str
    code: str
    value: Optional[str] = None


def iter_upsert_element_mutations(adapter: ItemAdapter) -> Iterator[ElementMutation]:
    """Повертає current patch-операції для `db_upsert_immobile_elements()`."""
    all_keys = (
        ["a1", "a2"]
        + [f"b{i}" for i in range(1, 6)]
        + [f"c{i}" for i in range(1, 8)]
        + [f"d{i}" for i in range(1, 14)]
    )

    for key in all_keys:
        raw_val = adapter.get(key)
        if raw_val is None:
            continue

        val = str(raw_val).strip()
        grp = key[0].upper()
        code = key[1:]

        if val == "-":
            yield ElementMutation(action="delete", grp=grp, code=code)
        elif val:
            yield ElementMutation(action="upsert", grp=grp, code=code, value=val)


def iter_apply_element_mutations(
    adapter: ItemAdapter,
    element_keys: Sequence[str],
) -> Iterator[ElementMutation]:
    """Повертає current apply-операції для `db_apply_immobile_elements()`."""
    for key in element_keys:
        raw = adapter.get(key)
        if raw is None:
            continue

        val = str(raw).strip()
        if val == "":
            continue

        grp = key[0].upper()
        code = key.upper()

        if val == "-":
            yield ElementMutation(action="delete", grp=grp, code=code)
        else:
            yield ElementMutation(action="upsert", grp=grp, code=code, value=val)


def build_real_address_update_plan(
    real_address_id: Optional[int] = None,
    energy_class: Optional[str] = None,
) -> Tuple[List[str], List[Any]]:
    """Будує current SQL update-plan для real address і `energy_class`.

    Функція навмисно зберігає поточний quirky behavior для whitespace
    `energy_class`: `clean_str()` повертає `None`, після чого гілка
    `val != ""` призводить до `AttributeError` на `val.upper()`.
    """
    updates: List[str] = []
    params: List[Any] = []

    if real_address_id is not None:
        updates.append("real_address_id = %s")
        params.append(real_address_id)

    if energy_class is not None:
        val = clean_str(energy_class)
        if val == "-":
            updates.append("energy_class = NULL")
        elif val != "":
            updates.append("energy_class = %s")
            params.append(val.upper())

    return updates, params
