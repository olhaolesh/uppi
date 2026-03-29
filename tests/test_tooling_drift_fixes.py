"""Тести для low-risk runtime/tooling drift fixes у support-утилітах."""

from __future__ import annotations

import importlib
from pathlib import Path

import uppi.domain.db as domain_db
import uppi.utils.db_utils.init_db as init_db
from uppi.cli import inspect_clients


class RecordingCursor:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, fetchone_value=None, fetchall_value=None):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.fetchone_value = fetchone_value
        self.fetchall_value = fetchall_value if fetchall_value is not None else []
        self.executed = []
        self.closed = False

    def execute(self, sql, params=None):
        """Запам’ятовує виклик execute для подальших assert-перевірок."""
        self.executed.append((sql, params))

    def fetchone(self):
        """Повертає заздалегідь підготовлений один тестовий рядок."""
        return self.fetchone_value

    def fetchall(self):
        """Повертає заздалегідь підготовлений список тестових рядків."""
        return self.fetchall_value

    def close(self):
        """Імітує закриття тестового ресурсу."""
        self.closed = True

    def __enter__(self):
        """Повертає тестовий об’єкт як контекстний менеджер."""
        return self

    def __exit__(self, exc_type, exc, tb):
        """Завершує використання тестового контекстного менеджера."""
        self.close()


class RecordingConnection:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, cursor):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.cursor_obj = cursor
        self.cursor_calls = []
        self.committed = False
        self.closed = False

    def cursor(self, *args, **kwargs):
        """Повертає тестовий курсор для імітації DB-доступу."""
        self.cursor_calls.append((args, kwargs))
        return self.cursor_obj

    def commit(self):
        """Імітує commit у recording connection."""
        self.committed = True

    def rollback(self):
        """Імітує rollback без зміни зовнішнього стану."""
        pass

    def close(self):
        """Імітує закриття тестового ресурсу."""
        self.closed = True


def test_db_has_visura_uses_current_locatore_cf_column(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    cursor = RecordingCursor(fetchone_value=(1,))
    conn = RecordingConnection(cursor)

    monkeypatch.setattr(domain_db, "get_pg_connection", lambda: conn)

    exists = domain_db.db_has_visura("RSSMRA80A01H501X")

    assert exists is True
    assert conn.committed is True
    sql, params = cursor.executed[0]
    assert "locatore_cf" in sql
    assert " where cf = " not in sql.lower()
    assert params == ("RSSMRA80A01H501X",)


def test_init_db_schema_file_is_resolved_relative_to_module():
    """Перевіряє сценарій, описаний у назві тесту."""
    expected = Path(init_db.__file__).resolve().with_name("uppi_schema.sql")

    assert init_db.SCHEMA_FILE == expected
    assert init_db.SCHEMA_FILE.exists()


def test_init_db_known_current_behavior_uses_db_ssl_mode_env(monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setenv("DB_SSL_MODE", "require")
    reloaded = importlib.reload(init_db)
    try:
        assert reloaded.config_db["sslmode"] == "require"
    finally:
        monkeypatch.delenv("DB_SSL_MODE", raising=False)
        importlib.reload(reloaded)


def test_execute_sql_file_reads_given_path_and_closes_resources(tmp_path, monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    sql_path = tmp_path / "schema.sql"
    sql_path.write_text("SELECT 1;", encoding="utf-8")

    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)

    monkeypatch.setattr(init_db.psycopg2, "connect", lambda **_: conn)

    init_db.execute_sql_file(sql_path, {"host": "localhost"})

    assert cursor.executed == [("SELECT 1;", None)]
    assert conn.committed is True
    assert cursor.closed is True
    assert conn.closed is True


def test_fetch_visura_uses_current_locatore_cf_column():
    """Перевіряє сценарій, описаний у назві тесту."""
    cursor = RecordingCursor(fetchone_value={"id": 1})
    conn = RecordingConnection(cursor)

    row = inspect_clients.fetch_visura(conn, "RSSMRA80A01H501X")

    assert row == {"id": 1}
    sql, params = cursor.executed[0]
    assert "locatore_cf" in sql
    assert " where cf = " not in sql.lower()
    assert params == ("RSSMRA80A01H501X",)


def test_fetch_immobili_uses_owner_cf_and_current_address_projection():
    """Перевіряє сценарій, описаний у назві тесту."""
    cursor = RecordingCursor(fetchall_value=[{"id": 7}])
    conn = RecordingConnection(cursor)

    rows = inspect_clients.fetch_immobili(conn, "RSSMRA80A01H501X")

    assert rows == [{"id": 7}]
    sql, params = cursor.executed[0]
    assert "i.owner_cf = %s" in sql
    assert "visura_cf" not in sql
    assert "LEFT JOIN addresses va ON i.visura_address_id = va.id" in sql
    assert "LEFT JOIN addresses ra ON i.real_address_id = ra.id" in sql
    assert "immobile_comune" in sql
    assert params == ("RSSMRA80A01H501X",)


def test_fetch_contract_participants_reads_current_contract_and_person_links():
    """Перевіряє сценарій, описаний у назві тесту."""
    cursor = RecordingCursor(fetchall_value=[{"role": "LOCATORE"}])
    conn = RecordingConnection(cursor)

    rows = inspect_clients.fetch_contract_participants(conn, "contract-1")

    assert rows == [{"role": "LOCATORE"}]
    sql, params = cursor.executed[0]
    assert "contract_parties" not in sql
    assert "FROM contracts c" in sql
    assert "JOIN immobili i ON i.id = c.immobile_id" in sql
    assert "LEFT JOIN persons p_own ON p_own.cf = i.owner_cf" in sql
    assert "LEFT JOIN persons p_cond ON p_cond.cf = c.conduttore_cf" in sql
    assert params == ("contract-1", "contract-1")


def test_fetch_address_sources_uses_current_real_and_visura_address_links():
    """Перевіряє сценарій, описаний у назві тесту."""
    cursor = RecordingCursor(fetchone_value={"immobile_id": 11})
    conn = RecordingConnection(cursor)

    row = inspect_clients.fetch_address_sources(conn, 11)

    assert row == {"immobile_id": 11}
    sql, params = cursor.executed[0]
    assert "contract_overrides" not in sql
    assert "i.visura_address_id" in sql
    assert "i.real_address_id" in sql
    assert "LEFT JOIN addresses va ON va.id = i.visura_address_id" in sql
    assert "LEFT JOIN addresses ra ON ra.id = i.real_address_id" in sql
    assert params == (11,)


def test_print_block_1_yaml_hint_uses_current_clients_yml_field_names(capsys):
    """Перевіряє сценарій, описаний у назві тесту."""
    inspect_clients.print_block_1_yaml_hint(
        "RSSMRA80A01H501X",
        {"immobile_comune": "Pescara", "foglio": "12", "numero": "345", "sub": "7"},
    )

    out = capsys.readouterr().out

    assert "IMMOBILE_COMUNE" in out
    assert "FOGLIO" in out
    assert "NUMERO" in out
    assert "SUB" in out
    assert "IMMOBILE_FOGLIO" not in out
    assert "IMMOBILE_NUMERO" not in out
    assert "IMMOBILE_SUB" not in out


def test_print_block_2_full_dump_uses_current_contract_id_and_address_sources(capsys, monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    monkeypatch.setattr(
        inspect_clients,
        "fetch_contract_participants",
        lambda conn, contract_id: [{"role": "LOCATORE", "name": "Mario", "surname": "Rossi", "cf": "RSSMRA80A01H501X"}],
    )
    monkeypatch.setattr(inspect_clients, "fetch_canone", lambda conn, contract_id: [])
    monkeypatch.setattr(
        inspect_clients,
        "fetch_address_sources",
        lambda conn, immobile_id: {"real_address_id": 12, "visura_address_id": 9},
    )

    inspect_clients.print_block_2_full_dump(
        imm={"id": 77, "foglio": "12"},
        contracts=[{"id": "contract-uuid", "immobile_id": 77}],
        conn=object(),
    )

    out = capsys.readouterr().out

    assert "CONTRACT [1] contract-uuid" in out
    assert "ADDRESS_SOURCES" in out
    assert "CONTRACT_OVERRIDES" not in out
