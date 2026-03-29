"""Characterization-тести для smart patch logic у db_repo."""

from __future__ import annotations

from datetime import date

import pytest
from itemadapter import ItemAdapter

from uppi.services.db_repo import (
    db_apply_immobile_elements,
    db_update_immobile_real_address,
    db_upsert_contract,
    db_upsert_immobile_elements,
    resolve_patch_value,
)


def _normalize_sql(sql: str) -> str:
    """Нормалізує тестові дані у форму, зручну для assert-перевірок."""
    return " ".join(sql.split())


class RecordingCursor:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, conn: "RecordingConnection"):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.conn = conn

    def __enter__(self):
        """Повертає тестовий об’єкт як контекстний менеджер."""
        return self

    def __exit__(self, exc_type, exc, tb):
        """Завершує використання тестового контекстного менеджера."""
        return False

    def execute(self, sql, params=None):
        """Запам’ятовує виклик execute для подальших assert-перевірок."""
        self.conn.executed.append((_normalize_sql(sql), params))

    def fetchone(self):
        """Повертає заздалегідь підготовлений один тестовий рядок."""
        if self.conn.fetchone_results:
            return self.conn.fetchone_results.pop(0)
        return None


class RecordingConnection:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, fetchone_results=None):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.fetchone_results = list(fetchone_results or [])
        self.executed = []
        self.rollback_called = False

    def cursor(self, cursor_factory=None):
        """Повертає тестовий курсор для імітації DB-доступу."""
        return RecordingCursor(self)

    def rollback(self):
        """Імітує rollback без зміни зовнішнього стану."""
        self.rollback_called = True


def _find_statement_params(conn: RecordingConnection, contains: str):
    """Допомагає дістати потрібний SQL-фрагмент або параметри для assert-перевірки."""
    for sql, params in conn.executed:
        if contains in sql:
            return params
    raise AssertionError(f"Statement containing {contains!r} not found. Executed: {conn.executed!r}")


def test_resolve_patch_value_dash_returns_default_or_none():
    """Перевіряє сценарій, описаний у назві тесту."""
    assert resolve_patch_value("-", "OLD") is None
    assert resolve_patch_value("-", "OLD", default=0.0) == 0.0


def test_resolve_patch_value_missing_or_empty_preserves_db_value():
    """Перевіряє сценарій, описаний у назві тесту."""
    assert resolve_patch_value(None, "OLD") == "OLD"
    assert resolve_patch_value("", "OLD") == "OLD"
    assert resolve_patch_value("   ", "OLD") == "OLD"


def test_resolve_patch_value_known_current_behavior_zero_string_is_treated_as_new_value():
    """Перевіряє сценарій, описаний у назві тесту."""
    assert resolve_patch_value("0", "OLD") == "0"


def test_db_upsert_immobile_elements_missing_keys_do_not_touch_db():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_upsert_immobile_elements(conn, immobile_id=101, adapter=ItemAdapter({}))

    assert conn.executed == []


def test_db_upsert_immobile_elements_known_current_behavior_dash_marker_deletes_using_numeric_code_only():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_upsert_immobile_elements(conn, immobile_id=101, adapter=ItemAdapter({"d12": "-"}))

    sql, params = conn.executed[0]
    assert "DELETE FROM public.immobile_elements" in sql
    assert params == (101, "D", "12")


def test_db_upsert_immobile_elements_empty_string_is_ignored():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_upsert_immobile_elements(conn, immobile_id=101, adapter=ItemAdapter({"a1": "   "}))

    assert conn.executed == []


def test_db_upsert_immobile_elements_non_empty_value_upserts_using_numeric_code_only():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_upsert_immobile_elements(conn, immobile_id=101, adapter=ItemAdapter({"b3": " X "}))

    sql, params = conn.executed[0]
    assert "INSERT INTO public.immobile_elements" in sql
    assert params == (101, "B", "3", "X")


def test_db_apply_immobile_elements_missing_keys_do_not_touch_db():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_apply_immobile_elements(conn, immobile_id=202, adapter=ItemAdapter({}))

    assert conn.executed == []


def test_db_apply_immobile_elements_known_current_behavior_dash_marker_deletes_using_full_upper_code():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_apply_immobile_elements(conn, immobile_id=202, adapter=ItemAdapter({"d12": "-"}))

    sql, params = conn.executed[0]
    assert "DELETE FROM public.immobile_elements" in sql
    assert params == (202, "D", "D12")


def test_db_apply_immobile_elements_non_empty_value_upserts_using_full_upper_code():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_apply_immobile_elements(conn, immobile_id=202, adapter=ItemAdapter({"b3": " X "}))

    sql, params = conn.executed[0]
    assert "INSERT INTO public.immobile_elements" in sql
    assert params == (202, "B", "B3", "X")


def test_db_update_immobile_real_address_returns_without_sql_when_no_effective_updates():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_update_immobile_real_address(conn, immobile_id=303, real_address_id=None, energy_class=None)

    assert conn.executed == []


def test_db_update_immobile_real_address_known_current_behavior_dash_marker_sets_energy_class_to_null():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_update_immobile_real_address(conn, immobile_id=303, real_address_id=None, energy_class="-")

    sql, params = conn.executed[0]
    assert "UPDATE public.immobili SET energy_class = NULL, updated_at = now() WHERE id = %s" == sql
    assert params == [303]


def test_db_update_immobile_real_address_known_current_behavior_whitespace_energy_class_raises_attribute_error():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    with pytest.raises(AttributeError):
        db_update_immobile_real_address(conn, immobile_id=303, real_address_id=77, energy_class="   ")


def test_db_update_immobile_real_address_non_empty_value_uppercases_energy_class():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection()

    db_update_immobile_real_address(conn, immobile_id=303, real_address_id=None, energy_class=" b ")

    sql, params = conn.executed[0]
    assert "energy_class = %s" in sql
    assert params == ["B", 303]


def test_contract_insert_defaults_to_concordato_today_and_three_years_when_missing():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection(fetchone_results=[None, ("contract-1",)])

    contract_id = db_upsert_contract(conn, immobile_id=1, adapter=ItemAdapter({}))

    assert contract_id == "contract-1"
    params = _find_statement_params(conn, "INSERT INTO public.contracts")
    assert params["kind"] == "CONCORDATO"
    assert params["durata"] == 3
    assert params["start_date"] == date.today()
    assert params["istat"] == 0.0
    assert params["arredato"] == 0.0
    assert params["ignore_surcharges"] is False


def test_contract_update_known_current_behavior_missing_contract_kind_resets_to_concordato_instead_of_preserving_db_value():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-2",
        "contract_kind": "STUDENTI",
        "durata_anni": 6,
        "arredato_pct": 10.0,
        "istat_rate": 75.0,
        "ignore_surcharges": True,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": date(2020, 2, 3),
        "registrazione_data": date(2020, 3, 4),
        "registrazione_num": "REG-OLD",
        "agenzia_entrate_sede": "PESCARA",
        "canone_contrattuale_mensile": 750.0,
        "conduttore_cf": "CONDOLD",
    }
    conn = RecordingConnection(fetchone_results=[old_contract])

    contract_id = db_upsert_contract(conn, immobile_id=1, adapter=ItemAdapter({}))

    assert contract_id == "contract-2"
    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["kind"] == "CONCORDATO"
    assert params["durata"] == 6
    assert params["istat"] == 75.0
    assert params["arredato"] == 10.0
    assert params["ignore_surcharges"] is True
    assert params["start_date"] == date(2020, 1, 2)


def test_contract_update_dash_markers_clear_patch_fields_and_known_current_behavior_durata_falls_back_to_three_years():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-3",
        "contract_kind": "TRANSITORIO",
        "durata_anni": 8,
        "arredato_pct": 25.0,
        "istat_rate": 50.0,
        "ignore_surcharges": True,
        "start_date": date(2022, 1, 1),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": None,
        "agenzia_entrate_sede": None,
        "canone_contrattuale_mensile": 900.0,
        "conduttore_cf": None,
    }
    adapter = ItemAdapter(
        {
            "arredato": "-",
            "durata_anni": "-",
            "istat": "-",
            "ignore_surcharges": "-",
        }
    )
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["arredato"] == 0.0
    assert params["istat"] == 0.0
    assert params["ignore_surcharges"] is False
    assert params["durata"] == 3


def test_contract_update_explicit_values_override_old_patch_fields():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-4",
        "contract_kind": "CONCORDATO",
        "durata_anni": 4,
        "arredato_pct": 5.0,
        "istat_rate": 30.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": None,
        "agenzia_entrate_sede": None,
        "canone_contrattuale_mensile": 600.0,
        "conduttore_cf": "CONDOLD",
    }
    adapter = ItemAdapter(
        {
            "contract_kind": "studenti",
            "arredato": "12.5",
            "durata_anni": "5",
            "istat": "75",
            "ignore_surcharges": "yes",
        }
    )
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["kind"] == "STUDENTI"
    assert params["arredato"] == 12.5
    assert params["durata"] == 5
    assert params["istat"] == 75.0
    assert params["ignore_surcharges"] is True


def test_contract_update_known_current_behavior_invalid_numeric_patch_values_zero_out_arredato_and_istat():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-5",
        "contract_kind": "CONCORDATO",
        "durata_anni": 4,
        "arredato_pct": 22.0,
        "istat_rate": 80.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": None,
        "agenzia_entrate_sede": None,
        "canone_contrattuale_mensile": 600.0,
        "conduttore_cf": None,
    }
    adapter = ItemAdapter({"arredato": "abc", "istat": "abc"})
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["arredato"] == 0.0
    assert params["istat"] == 0.0


def test_contract_update_known_current_behavior_explicit_zero_canone_does_not_override_existing_non_zero_value():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-6",
        "contract_kind": "CONCORDATO",
        "durata_anni": 4,
        "arredato_pct": 0.0,
        "istat_rate": 0.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": None,
        "agenzia_entrate_sede": None,
        "canone_contrattuale_mensile": 750.0,
        "conduttore_cf": None,
    }
    adapter = ItemAdapter({"canone_contrattuale_mensile": "0"})
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["canone"] == 750.0


def test_contract_insert_known_current_behavior_explicit_zero_canone_becomes_none_when_no_old_value():
    """Перевіряє сценарій, описаний у назві тесту."""
    conn = RecordingConnection(fetchone_results=[None, ("contract-7",)])

    db_upsert_contract(
        conn,
        immobile_id=1,
        adapter=ItemAdapter({"canone_contrattuale_mensile": "0"}),
    )

    params = _find_statement_params(conn, "INSERT INTO public.contracts")
    assert params["canone"] is None


def test_contract_update_invalid_date_preserves_existing_date_values():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-8",
        "contract_kind": "CONCORDATO",
        "durata_anni": 4,
        "arredato_pct": 0.0,
        "istat_rate": 0.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": date(2020, 2, 3),
        "registrazione_data": date(2020, 3, 4),
        "registrazione_num": "REG-OLD",
        "agenzia_entrate_sede": "PESCARA",
        "canone_contrattuale_mensile": 600.0,
        "conduttore_cf": None,
    }
    adapter = ItemAdapter(
        {
            "contratto_data": "15.01.2024",
            "decorrenza_data": "15.01.2024",
            "registrazione_data": "15.01.2024",
        }
    )
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["start_date"] == date(2020, 1, 2)
    assert params["decorrenza"] == date(2020, 2, 3)
    assert params["reg_data"] == date(2020, 3, 4)


def test_contract_update_known_current_behavior_dash_is_not_delete_marker_for_plain_text_fields():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-9",
        "contract_kind": "CONCORDATO",
        "durata_anni": 4,
        "arredato_pct": 0.0,
        "istat_rate": 0.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": "REG-OLD",
        "agenzia_entrate_sede": "PESCARA",
        "canone_contrattuale_mensile": 600.0,
        "conduttore_cf": "CONDOLD",
    }
    adapter = ItemAdapter(
        {
            "conduttore_cf": "-",
            "registrazione_num": "-",
            "agenzia_entrate_sede": "-",
        }
    )
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["cond_cf"] == "-"
    assert params["reg_num"] == "-"
    assert params["ae_sede"] == "-"


def test_contract_update_known_current_behavior_unknown_contract_kind_defaults_to_concordato():
    """Перевіряє сценарій, описаний у назві тесту."""
    old_contract = {
        "id": "contract-10",
        "contract_kind": "STUDENTI",
        "durata_anni": 4,
        "arredato_pct": 0.0,
        "istat_rate": 0.0,
        "ignore_surcharges": False,
        "start_date": date(2020, 1, 2),
        "decorrenza_data": None,
        "registrazione_data": None,
        "registrazione_num": None,
        "agenzia_entrate_sede": None,
        "canone_contrattuale_mensile": 600.0,
        "conduttore_cf": None,
    }
    adapter = ItemAdapter({"contract_kind": "SOMETHING_ELSE"})
    conn = RecordingConnection(fetchone_results=[old_contract])

    db_upsert_contract(conn, immobile_id=1, adapter=adapter)

    params = _find_statement_params(conn, "UPDATE public.contracts SET")
    assert params["kind"] == "CONCORDATO"
