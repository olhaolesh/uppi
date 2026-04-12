"""Pure-function policy rules для current contract patch semantics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Mapping

from itemadapter import ItemAdapter

from uppi.utils.parse_utils import clean_str, parse_date, safe_float


@dataclass(frozen=True)
class ContractPatchDecision:
    """Містить current patch-рішення для upsert контракту."""

    params: Dict[str, Any]
    kind_was_unknown: bool


def _resolve_contract_kind(raw_kind: Any) -> tuple[str, bool]:
    """Повертає current normalized contract kind і прапор fallback-а."""
    kind_raw = clean_str(raw_kind)
    new_kind = (kind_raw or "CONCORDATO").upper()
    if new_kind not in ["CONCORDATO", "TRANSITORIO", "STUDENTI"]:
        return "CONCORDATO", True
    return new_kind, False


def _resolve_arredato(raw_arredato: Any, old_contract: Mapping[str, Any]) -> float:
    """Застосовує current patch-правила до `arredato`."""
    old_arredato = float(old_contract.get("arredato_pct") or 0.0)
    if str(raw_arredato).strip() == "-":
        return 0.0
    if raw_arredato is not None and str(raw_arredato).strip() != "":
        return safe_float(raw_arredato) or 0.0
    return old_arredato


def _resolve_durata(raw_durata: Any, old_contract: Mapping[str, Any]) -> int:
    """Застосовує current patch-правила до `durata_anni`."""
    old_durata = old_contract.get("durata_anni")
    if str(raw_durata).strip() == "-":
        new_durata = None
    elif raw_durata is not None and str(raw_durata).strip() != "":
        new_durata = int(raw_durata)
    else:
        new_durata = old_durata

    if new_durata is None:
        return 3
    return int(new_durata)


def _resolve_istat(raw_istat: Any, old_contract: Mapping[str, Any]) -> float:
    """Застосовує current patch-правила до `istat`."""
    old_istat = float(old_contract.get("istat_rate") or 0.0)
    if str(raw_istat).strip() == "-":
        return 0.0
    if raw_istat is not None and str(raw_istat).strip() != "":
        return safe_float(raw_istat) or 0.0
    return old_istat


def _resolve_ignore_surcharges(raw_ignore: Any, old_contract: Mapping[str, Any]) -> bool:
    """Застосовує current patch-правила до `ignore_surcharges`."""
    old_ignore = bool(old_contract.get("ignore_surcharges")) if old_contract else False
    if str(raw_ignore).strip() == "-":
        return False
    if raw_ignore is not None and str(raw_ignore).strip() != "":
        return str(raw_ignore).lower() in ("true", "1", "yes", "y")
    return old_ignore


def build_contract_patch_decision(
    immobile_id: int,
    adapter: ItemAdapter,
    old_contract: Mapping[str, Any],
) -> ContractPatchDecision:
    """Будує current patch-рішення для insert/update контракту без SQL."""
    new_kind, kind_was_unknown = _resolve_contract_kind(adapter.get("contract_kind"))

    cond_cf = clean_str(adapter.get("conduttore_cf")) or old_contract.get("conduttore_cf")
    start_date = parse_date(adapter.get("contratto_data")) or old_contract.get("start_date")
    decorrenza = parse_date(adapter.get("decorrenza_data")) or old_contract.get("decorrenza_data")
    reg_data = parse_date(adapter.get("registrazione_data")) or old_contract.get("registrazione_data")
    reg_num = clean_str(adapter.get("registrazione_num")) or old_contract.get("registrazione_num")
    ae_sede = clean_str(adapter.get("agenzia_entrate_sede")) or old_contract.get("agenzia_entrate_sede")
    canone_val = safe_float(adapter.get("canone_contrattuale_mensile")) or old_contract.get(
        "canone_contrattuale_mensile"
    )

    if not start_date:
        start_date = date.today()

    params = {
        "immobile_id": immobile_id,
        "cond_cf": cond_cf,
        "kind": new_kind,
        "start_date": start_date,
        "durata": _resolve_durata(adapter.get("durata_anni"), old_contract),
        "decorrenza": decorrenza,
        "reg_data": reg_data,
        "reg_num": reg_num,
        "ae_sede": ae_sede,
        "canone": canone_val,
        "istat": _resolve_istat(adapter.get("istat"), old_contract),
        "arredato": _resolve_arredato(adapter.get("arredato"), old_contract),
        "ignore_surcharges": _resolve_ignore_surcharges(adapter.get("ignore_surcharges"), old_contract),
    }
    return ContractPatchDecision(params=params, kind_was_unknown=kind_was_unknown)


def build_generation_contract_patch_decision(
    immobile_id: int,
    adapter: ItemAdapter,
    old_contract: Mapping[str, Any],
) -> ContractPatchDecision:
    """Build the generation-only DB write-back for persistable contract fields."""
    raw_kind = clean_str(adapter.get("contract_kind"))
    old_kind = clean_str(old_contract.get("contract_kind"))

    kind_was_unknown = False
    if raw_kind:
        new_kind, kind_was_unknown = _resolve_contract_kind(raw_kind)
    elif old_kind:
        new_kind, _ = _resolve_contract_kind(old_kind)
    else:
        new_kind = "CONCORDATO"

    params = {
        "immobile_id": immobile_id,
        "kind": new_kind,
        "istat": _resolve_istat(adapter.get("istat"), old_contract),
        "arredato": _resolve_arredato(adapter.get("arredato"), old_contract),
        "ignore_surcharges": _resolve_ignore_surcharges(adapter.get("ignore_surcharges"), old_contract),
    }
    return ContractPatchDecision(params=params, kind_was_unknown=kind_was_unknown)
