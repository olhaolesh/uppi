"""Pure-function policy units для patch/update семантики проєкту.

Цей пакет ізолює business rules, які раніше були змішані з repository
операціями. Політики не виконують SQL і не мають побічних ефектів, окрім
звичайних перетворень вхідних даних.
"""

from uppi.services.policies.contract_patch_policy import (
    ContractPatchDecision,
    build_contract_patch_decision,
)
from uppi.services.policies.immobile_patch_policy import (
    ElementMutation,
    build_real_address_update_plan,
    iter_apply_element_mutations,
    iter_upsert_element_mutations,
)
from uppi.services.policies.patch_policy import resolve_patch_value

__all__ = [
    "ContractPatchDecision",
    "ElementMutation",
    "build_contract_patch_decision",
    "build_real_address_update_plan",
    "iter_apply_element_mutations",
    "iter_upsert_element_mutations",
    "resolve_patch_value",
]
