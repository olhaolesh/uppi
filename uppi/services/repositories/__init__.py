"""Тонкі repository-модулі для роботи з поточним SQL-контрактом проєкту.

Пакет групує функції з колишнього монолітного `db_repo.py` за зонами
відповідальності, але не змінює їхню фактичну поведінку. Публічна
сумісність назовні поки що зберігається через facade-модуль
`uppi.services.db_repo`.
"""

from uppi.services.repositories.address_repo import db_upsert_address
from uppi.services.repositories.audit_repo import (
    db_insert_attestazione_log,
    db_insert_canone_calc,
)
from uppi.services.repositories.common import (
    ELEMENT_KEYS,
    IMMOBILI_PARSED_COLUMNS,
    immobile_db_row,
    immobile_from_parsed_dict,
)
from uppi.services.repositories.contract_repo import (
    db_load_contract_context,
    db_upsert_contract,
)
from uppi.services.repositories.immobile_repo import (
    db_apply_immobile_elements,
    db_load_immobili,
    db_prune_old_immobili_without_contracts,
    db_update_immobile_real_address,
    db_upsert_immobile,
    db_upsert_immobile_elements,
)
from uppi.services.repositories.person_repo import db_upsert_person
from uppi.services.repositories.visura_repo import (
    VisuraState,
    db_upsert_visura,
    fetch_visura_state,
)
from uppi.services.policies.patch_policy import resolve_patch_value

__all__ = [
    "ELEMENT_KEYS",
    "IMMOBILI_PARSED_COLUMNS",
    "VisuraState",
    "db_apply_immobile_elements",
    "db_insert_attestazione_log",
    "db_insert_canone_calc",
    "db_load_contract_context",
    "db_load_immobili",
    "db_prune_old_immobili_without_contracts",
    "db_update_immobile_real_address",
    "db_upsert_address",
    "db_upsert_contract",
    "db_upsert_immobile",
    "db_upsert_immobile_elements",
    "db_upsert_person",
    "db_upsert_visura",
    "fetch_visura_state",
    "immobile_db_row",
    "immobile_from_parsed_dict",
    "resolve_patch_value",
]
