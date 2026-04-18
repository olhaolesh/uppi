"""Integration-тести для current DB/repository contract на temp Postgres."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytest
from itemadapter import ItemAdapter
import yaml

import uppi.domain.db as domain_db
from uppi.domain.exceptions import ImmobiliDocumentNotFoundError
from uppi.domain.immobile import Immobile
from uppi.services.db_repo import (
    db_apply_immobile_elements,
    db_insert_canone_calc,
    db_load_immobile_by_identity,
    db_load_contract_context,
    db_load_immobili,
    db_update_person_residence_address,
    db_update_immobile_real_address,
    db_upsert_address,
    db_upsert_generation_contract,
    db_upsert_contract,
    db_upsert_immobile,
    db_upsert_immobile_elements,
    db_upsert_person,
    db_upsert_visura,
    fetch_visura_state,
)
from uppi.services.immobili_yaml_generator import (
    ImmobiliDocumentMetadataDefaults,
    ImmobiliYamlGeneratorService,
    build_immobili_document_from_db,
    dump_immobili_document_yaml,
    write_immobili_document_yaml,
)
from uppi.services.repositories.prepare_document_repo import db_load_prepare_document_presence


SCHEMA_FILE = Path(__file__).resolve().parents[1] / "uppi" / "utils" / "db_utils" / "uppi_schema.sql"
INITDB_BIN = shutil.which("initdb")
PG_CTL_BIN = shutil.which("pg_ctl")


def _pick_free_port() -> int:
    """Повертає вільний TCP-порт для запуску тимчасового Postgres."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Запускає зовнішню команду й піднімає зрозумілу помилку при збої."""
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


@dataclass
class TempPostgresHarness:
    """Керує життєвим циклом локального тимчасового Postgres-кластера для тестів."""

    base_dir: Path
    data_dir: Path
    log_file: Path
    port: int

    @property
    def connect_kwargs(self) -> dict[str, object]:
        """Повертає canonical kwargs для psycopg2.connect до temp Postgres."""
        return {
            "host": "127.0.0.1",
            "port": self.port,
            "dbname": "postgres",
            "user": "postgres",
            "sslmode": "disable",
        }

    def connect(self, *, autocommit: bool = False):
        """Створює новий psycopg2 connection до тимчасової БД."""
        conn = psycopg2.connect(**self.connect_kwargs)
        conn.autocommit = autocommit
        return conn

    def apply_schema(self) -> None:
        """Ініціалізує тимчасову БД production-схемою з SQL-файлу проєкту."""
        sql = SCHEMA_FILE.read_text(encoding="utf-8")
        with self.connect(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

    def reset_data(self) -> None:
        """Очищає всі прикладні таблиці між тестами без повторного initdb."""
        truncate_sql = """
        TRUNCATE TABLE
            public.attestazioni,
            public.canone_calcoli,
            public.contracts,
            public.immobile_elements,
            public.immobili,
            public.visure,
            public.persons,
            public.addresses
        RESTART IDENTITY CASCADE;
        """
        with self.connect(autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(truncate_sql)


@pytest.fixture(scope="session")
def temp_postgres_db(tmp_path_factory) -> TempPostgresHarness:
    """Піднімає один temp Postgres cluster на сесію тестів."""
    if not INITDB_BIN or not PG_CTL_BIN:
        pytest.skip("Local PostgreSQL binaries are required for temp Postgres integration tests")

    base_dir = tmp_path_factory.mktemp("temp-postgres")
    data_dir = base_dir / "data"
    log_file = base_dir / "postgres.log"

    port = _pick_free_port()

    _run([INITDB_BIN, "-D", str(data_dir), "-A", "trust", "-U", "postgres"])
    _run(
        [
            PG_CTL_BIN,
            "-D",
            str(data_dir),
                "-l",
                str(log_file),
                "-o",
                f"-F -p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories=/tmp",
                "start",
            ]
        )

    harness = TempPostgresHarness(
        base_dir=base_dir,
        data_dir=data_dir,
        log_file=log_file,
        port=port,
    )

    deadline = time.time() + 10
    last_error = None
    while time.time() < deadline:
        try:
            with harness.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            last_error = None
            break
        except psycopg2.Error as exc:
            last_error = exc
            time.sleep(0.1)
    if last_error is not None:
        raise RuntimeError(f"Temp Postgres did not become ready: {last_error}")

    harness.apply_schema()

    try:
        yield harness
    finally:
        _run([PG_CTL_BIN, "-D", str(data_dir), "stop", "-m", "fast"])


@pytest.fixture()
def pg_conn(temp_postgres_db: TempPostgresHarness):
    """Повертає чистий connection до temp Postgres для кожного тесту."""
    temp_postgres_db.reset_data()
    conn = temp_postgres_db.connect()
    try:
        yield conn
    finally:
        conn.close()


def _fetchone_dict(conn, sql: str, params=()):
    """Виконує SELECT і повертає один рядок як словник."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def _fetchall_dicts(conn, sql: str, params=()):
    """Виконує SELECT і повертає всі рядки як список словників."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _make_immobile(**overrides) -> Immobile:
    """Створює типовий об'єкт нерухомості для integration-сценаріїв."""
    base = {
        "foglio": "12",
        "numero": "345",
        "sub": "7",
        "zona_cens": "2",
        "micro_zona": "5",
        "categoria": "A/2",
        "classe": "3",
        "consistenza": "5 vani",
        "rendita": "€ 123.45",
        "superficie_totale": 98.7,
        "superficie_escluse": 90.1,
        "superficie_raw": "Totale: 98,7 Totale escluse aree scoperte**: 90,1",
    }
    base.update(overrides)
    return Immobile(**base)


def _seed_owner(conn, *, cf: str = "RSSMRA80A01H501Z", comune: str = "Pescara") -> tuple[str, int]:
    """Створює власника й повертає його CF разом з residence address id."""
    addr_id = db_upsert_address(
        conn,
        {
            "comune": comune,
            "via_full": "Via Roma",
            "civico": "10",
        },
    )
    db_upsert_person(conn, cf, surname="Rossi", name="Mario", address_id=addr_id)
    return cf, addr_id


def test_visura_repo_contracts_persist_and_domain_db_check_work_on_temp_postgres(pg_conn, temp_postgres_db, monkeypatch):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)

    visura_id = db_upsert_visura(
        pg_conn,
        owner_cf,
        "visure-bucket",
        "visure/RSSMRA80A01H501Z.pdf",
        "checksum-123",
        fetched_now=True,
    )
    pg_conn.commit()

    state = fetch_visura_state(pg_conn, owner_cf)
    visura_row = _fetchone_dict(
        pg_conn,
        """
        SELECT id, locatore_cf, pdf_bucket, pdf_object, checksum_sha256, fetched_at
        FROM public.visure
        WHERE locatore_cf = %s;
        """,
        (owner_cf,),
    )

    assert state is not None
    assert state.id == visura_id
    assert state.pdf_bucket == "visure-bucket"
    assert state.pdf_object == "visure/RSSMRA80A01H501Z.pdf"
    assert state.fetched_at is not None
    assert visura_row["id"] == visura_id
    assert visura_row["checksum_sha256"] == "checksum-123"

    monkeypatch.setattr(domain_db, "get_pg_connection", lambda: temp_postgres_db.connect())
    assert domain_db.db_has_visura(owner_cf) is True


def test_known_current_behavior_fetch_visura_state_returns_trimmed_shape_without_checksum_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    db_upsert_visura(
        pg_conn,
        owner_cf,
        "visure-bucket",
        "visure/RSSMRA80A01H501Z.pdf",
        "checksum-123",
        fetched_now=True,
    )
    pg_conn.commit()

    state = fetch_visura_state(pg_conn, owner_cf)

    assert state is not None
    assert not hasattr(state, "checksum_sha256")


def test_address_and_person_repo_contracts_deduplicate_and_preserve_existing_fields_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    addr_id_1 = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via  Roma", "civico": None},
    )
    addr_id_2 = db_upsert_address(
        pg_conn,
        {"comune": " PESCARA ", "via_full": "  Via Roma  ", "civico": None},
    )

    db_upsert_person(pg_conn, "RSSMRA80A01H501Z", surname="Rossi", name="Mario", address_id=addr_id_1)
    db_upsert_person(pg_conn, "RSSMRA80A01H501Z", surname=None, name="Marco", address_id=None)
    pg_conn.commit()

    address_row = _fetchone_dict(
        pg_conn,
        "SELECT id, civico FROM public.addresses WHERE id = %s;",
        (addr_id_1,),
    )
    person_row = _fetchone_dict(
        pg_conn,
        "SELECT cf, surname, name, residence_address_id FROM public.persons WHERE cf = %s;",
        ("RSSMRA80A01H501Z",),
    )

    assert addr_id_1 == addr_id_2
    assert address_row["civico"] == "SNC"
    assert person_row["surname"] == "Rossi"
    assert person_row["name"] == "Marco"
    assert person_row["residence_address_id"] == addr_id_1


def test_immobile_repo_contracts_persist_and_load_current_joined_shape_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    visura_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Corso Vittorio", "civico": "12", "piano": "1", "interno": "2", "scala": "A"},
    )
    real_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Montesilvano", "via_full": "Via Milano", "civico": "8", "piano": "3", "interno": "4"},
    )
    visura_id = db_upsert_visura(pg_conn, owner_cf, "visure-bucket", "visure/doc.pdf", "sum", fetched_now=True)

    immobile_id = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(),
        visura_addr_id=visura_addr_id,
        source_visura_id=visura_id,
    )
    db_update_immobile_real_address(pg_conn, immobile_id, real_address_id=real_addr_id, energy_class=" b ")

    db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(consistenza="6 vani", categoria=None, micro_zona=None),
        visura_addr_id=None,
        source_visura_id=None,
    )
    pg_conn.commit()

    loaded = db_load_immobili(pg_conn, owner_cf)

    assert len(loaded) == 1
    loaded_id, imm = loaded[0]
    assert loaded_id == immobile_id
    assert imm.consistenza == "6 vani"
    assert imm.categoria == "A/2"
    assert imm.micro_zona == "5"
    assert imm.energy_class == "B"
    assert imm.immobile_comune == "Pescara"
    assert imm.via_name == "Corso Vittorio"
    assert imm.via_num == "12"
    assert imm.piano == "1"
    assert imm.interno == "2"
    assert imm.scala == "A"
    assert imm.immobile_comune_override == "Montesilvano"
    assert imm.immobile_via_override == "Via Milano"
    assert imm.immobile_civico_override == "8"
    assert imm.immobile_piano_override == "3"
    assert imm.immobile_interno_override == "4"


def test_db_load_immobile_by_identity_uses_strict_generation_key_on_temp_postgres(pg_conn):
    """Generation matching should be deterministic on owner CF plus cadastral identity."""
    owner_cf, _ = _seed_owner(pg_conn)
    visura_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Corso Vittorio", "civico": "12"},
    )
    immobile_id = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(sub=""),
        visura_addr_id=visura_addr_id,
        source_visura_id=None,
    )
    pg_conn.commit()

    matched = db_load_immobile_by_identity(pg_conn, owner_cf, "12", "345", "")
    missing = db_load_immobile_by_identity(pg_conn, owner_cf, "12", "345", "7")

    assert matched is not None
    matched_id, matched_imm = matched
    assert matched_id == immobile_id
    assert matched_imm.foglio == "12"
    assert matched_imm.numero == "345"
    assert matched_imm.sub == ""
    assert missing is None


def test_known_current_behavior_element_persistence_uses_two_code_shapes_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    immobile_id = db_upsert_immobile(pg_conn, owner_cf, _make_immobile(sub=""))

    db_upsert_immobile_elements(pg_conn, immobile_id, ItemAdapter({"d12": "X"}))
    db_apply_immobile_elements(pg_conn, immobile_id, ItemAdapter({"d12": "Y"}))
    pg_conn.commit()

    rows = _fetchall_dicts(
        pg_conn,
        """
        SELECT grp, code, value
        FROM public.immobile_elements
        WHERE immobile_id = %s
        ORDER BY code;
        """,
        (immobile_id,),
    )

    assert rows == [
        {"grp": "D", "code": "12", "value": "X"},
        {"grp": "D", "code": "D12", "value": "Y"},
    ]


def test_db_upsert_contract_persists_current_fields_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    cond_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Test", "civico": "20"},
    )
    db_upsert_person(pg_conn, "BNCMRA80A01H501Z", surname="Bianchi", name="Mario", address_id=cond_addr_id)
    immobile_id = db_upsert_immobile(pg_conn, owner_cf, _make_immobile(sub=""))

    contract_id = db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "conduttore_cf": "BNCMRA80A01H501Z",
                "contract_kind": "TRANSITORIO",
                "contratto_data": "01/01/2025",
                "decorrenza_data": "01/02/2025",
                "registrazione_data": "05/02/2025",
                "registrazione_num": "REG-123",
                "agenzia_entrate_sede": "PESCARA",
                "canone_contrattuale_mensile": "650",
                "istat": 5,
                "arredato": "0.10",
                "durata_anni": "4",
                "ignore_surcharges": "yes",
            }
        ),
    )
    pg_conn.commit()

    row = _fetchone_dict(
        pg_conn,
        """
        SELECT
            id,
            conduttore_cf,
            contract_kind,
            start_date,
            durata_anni,
            decorrenza_data,
            registrazione_data,
            registrazione_num,
            agenzia_entrate_sede,
            canone_contrattuale_mensile,
            istat_rate,
            arredato_pct,
            ignore_surcharges
        FROM public.contracts
        WHERE id = %s;
        """,
        (contract_id,),
    )

    assert str(row["id"]) == contract_id
    assert row["conduttore_cf"] == "BNCMRA80A01H501Z"
    assert row["contract_kind"] == "TRANSITORIO"
    assert str(row["start_date"]) == "2025-01-01"
    assert row["durata_anni"] == 4
    assert str(row["decorrenza_data"]) == "2025-02-01"
    assert str(row["registrazione_data"]) == "2025-02-05"
    assert row["registrazione_num"] == "REG-123"
    assert row["agenzia_entrate_sede"] == "PESCARA"
    assert float(row["canone_contrattuale_mensile"]) == 650.0
    assert float(row["istat_rate"]) == 5.0
    assert float(row["arredato_pct"]) == 0.1
    assert row["ignore_surcharges"] is True


def test_db_upsert_generation_contract_persists_only_allowlisted_fields_on_temp_postgres(pg_conn):
    """Generation write-back must not promote run-only fields into master DB defaults."""
    owner_cf, _ = _seed_owner(pg_conn)
    cond_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Test", "civico": "20"},
    )
    db_upsert_person(pg_conn, "BNCMRA80A01H501Z", surname="Bianchi", name="Mario", address_id=cond_addr_id)
    immobile_id = db_upsert_immobile(pg_conn, owner_cf, _make_immobile(sub=""))

    contract_id = db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "conduttore_cf": "BNCMRA80A01H501Z",
                "contract_kind": "TRANSITORIO",
                "contratto_data": "01/01/2025",
                "decorrenza_data": "01/02/2025",
                "registrazione_data": "05/02/2025",
                "registrazione_num": "REG-123",
                "agenzia_entrate_sede": "PESCARA",
                "canone_contrattuale_mensile": "650",
                "istat": 5,
                "arredato": "0.10",
                "durata_anni": "4",
                "ignore_surcharges": "yes",
            }
        ),
    )
    pg_conn.commit()

    returned_id = db_upsert_generation_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "conduttore_cf": "VRDLGI80A01H501Z",
                "contract_kind": "STUDENTI",
                "contratto_data": "09/09/2026",
                "decorrenza_data": "10/10/2026",
                "registrazione_data": "11/10/2026",
                "registrazione_num": "REG-999",
                "agenzia_entrate_sede": "CHIETI",
                "canone_contrattuale_mensile": "999",
                "istat": "2.5",
                "arredato": "0.25",
                "durata_anni": "8",
                "ignore_surcharges": "no",
            }
        ),
    )
    pg_conn.commit()

    row = _fetchone_dict(
        pg_conn,
        """
        SELECT
            id,
            conduttore_cf,
            contract_kind,
            start_date,
            durata_anni,
            decorrenza_data,
            registrazione_data,
            registrazione_num,
            agenzia_entrate_sede,
            canone_contrattuale_mensile,
            istat_rate,
            arredato_pct,
            ignore_surcharges
        FROM public.contracts
        WHERE id = %s;
        """,
        (returned_id,),
    )

    assert returned_id == contract_id
    assert row["conduttore_cf"] == "BNCMRA80A01H501Z"
    assert row["contract_kind"] == "STUDENTI"
    assert str(row["start_date"]) == "2025-01-01"
    assert row["durata_anni"] == 4
    assert str(row["decorrenza_data"]) == "2025-02-01"
    assert str(row["registrazione_data"]) == "2025-02-05"
    assert row["registrazione_num"] == "REG-123"
    assert row["agenzia_entrate_sede"] == "PESCARA"
    assert float(row["canone_contrattuale_mensile"]) == 650.0
    assert float(row["istat_rate"]) == 2.5
    assert float(row["arredato_pct"]) == 0.25
    assert row["ignore_surcharges"] is False


def test_generation_persistable_clear_markers_write_back_only_to_allowed_db_fields_on_temp_postgres(pg_conn):
    """Persistable clear markers must clear DB state without promoting run-only clears to master defaults."""
    owner_cf, owner_addr_id = _seed_owner(pg_conn)
    cond_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Conduttore", "civico": "20"},
    )
    db_upsert_person(pg_conn, "BNCMRA80A01H501Z", surname="Bianchi", name="Mario", address_id=cond_addr_id)

    visura_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Corso Roma", "civico": "12"},
    )
    real_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Montesilvano", "via_full": "Via Milano", "civico": "8", "piano": "3", "interno": "4"},
    )
    immobile_id = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(sub="", energy_class="B"),
        visura_addr_id=visura_addr_id,
        source_visura_id=None,
    )
    db_update_immobile_real_address(
        pg_conn,
        immobile_id,
        real_address_id=real_addr_id,
        energy_class="B",
    )
    db_apply_immobile_elements(pg_conn, immobile_id, ItemAdapter({"a1": "X"}))
    contract_id = db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "conduttore_cf": "BNCMRA80A01H501Z",
                "contract_kind": "STUDENTI",
                "registrazione_num": "REG-123",
                "istat": "2.5",
                "arredato": "0.25",
                "ignore_surcharges": "yes",
            }
        ),
    )
    pg_conn.commit()

    db_update_person_residence_address(pg_conn, owner_cf, None)
    db_update_immobile_real_address(
        pg_conn,
        immobile_id,
        real_address_id=None,
        energy_class="-",
        clear_real_address=True,
    )
    db_apply_immobile_elements(pg_conn, immobile_id, ItemAdapter({"a1": "-"}))
    db_upsert_generation_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "contract_kind": "TRANSITORIO",
                "istat": "-",
                "arredato": "-",
                "ignore_surcharges": "-",
                "conduttore_cf": "-",
                "registrazione_num": "-",
            }
        ),
    )
    pg_conn.commit()

    owner_row = _fetchone_dict(
        pg_conn,
        "SELECT residence_address_id FROM public.persons WHERE cf = %s;",
        (owner_cf,),
    )
    immobile_row = _fetchone_dict(
        pg_conn,
        "SELECT real_address_id, energy_class FROM public.immobili WHERE id = %s;",
        (immobile_id,),
    )
    element_rows = _fetchall_dicts(
        pg_conn,
        "SELECT grp, code, value FROM public.immobile_elements WHERE immobile_id = %s;",
        (immobile_id,),
    )
    contract_row = _fetchone_dict(
        pg_conn,
        """
        SELECT id, conduttore_cf, contract_kind, registrazione_num, istat_rate, arredato_pct, ignore_surcharges
        FROM public.contracts
        WHERE id = %s;
        """,
        (contract_id,),
    )

    assert owner_row["residence_address_id"] is None
    assert immobile_row["real_address_id"] is None
    assert immobile_row["energy_class"] is None
    assert element_rows == []
    assert contract_row["contract_kind"] == "TRANSITORIO"
    assert contract_row["conduttore_cf"] == "BNCMRA80A01H501Z"
    assert contract_row["registrazione_num"] == "REG-123"
    assert contract_row["istat_rate"] is None
    assert contract_row["arredato_pct"] is None
    assert contract_row["ignore_surcharges"] is False


def test_db_load_contract_context_returns_current_joined_shape_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, owner_addr_id = _seed_owner(pg_conn, comune="Chieti")
    cond_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Conduttore", "civico": "33"},
    )
    db_upsert_person(pg_conn, "BNCMRA80A01H501Z", surname="Bianchi", name="Mario", address_id=cond_addr_id)

    visura_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Corso Roma", "civico": "12", "piano": "1", "interno": "2", "scala": "A"},
    )
    real_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Montesilvano", "via_full": "Via Milano", "civico": "8", "piano": "3", "interno": "4"},
    )
    visura_id = db_upsert_visura(pg_conn, owner_cf, "visure-bucket", "visure/doc.pdf", "sum", fetched_now=True)
    immobile_id = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(sub=""),
        visura_addr_id=visura_addr_id,
        source_visura_id=visura_id,
    )
    db_update_immobile_real_address(pg_conn, immobile_id, real_address_id=real_addr_id, energy_class="A")
    db_upsert_immobile_elements(pg_conn, immobile_id, ItemAdapter({"a1": "X", "b2": "Y"}))

    contract_id = db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "conduttore_cf": "BNCMRA80A01H501Z",
                "contract_kind": "TRANSITORIO",
                "contratto_data": "01/01/2025",
                "registrazione_num": "REG-123",
                "agenzia_entrate_sede": "PESCARA",
            }
        ),
    )
    db_insert_canone_calc(
        pg_conn,
        contract_id,
        "pescara2018_base",
        inputs={"result": {"base_min_euro_mq": 10.5, "base_max_euro_mq": 12.0, "zona": "B1"}},
        result_mensile=650.0,
    )
    pg_conn.commit()

    ctx = db_load_contract_context(pg_conn, contract_id)

    assert ctx["contract"]["contract_kind"] == "TRANSITORIO"
    assert ctx["contract"]["immobile_id"] == immobile_id
    assert ctx["immobile"] == {
        "comune": "Montesilvano",
        "via": "Via Milano",
        "civico": "8",
        "piano": "3",
        "interno": "4",
        "energy_class": "A",
    }
    assert ctx["overrides"] == {
        "locatore_comune_res": "Chieti",
        "locatore_via": "Via Roma",
        "locatore_civico": "10",
    }
    assert ctx["parties"]["LOCATORE"] == {
        "cf": owner_cf,
        "name": "Mario",
        "surname": "Rossi",
    }
    assert ctx["parties"]["CONDUTTORE"] == {
        "cf": "BNCMRA80A01H501Z",
        "name": "Mario",
        "surname": "Bianchi",
        "comune": "Pescara",
        "via": "Via Conduttore",
    }
    assert ctx["elements"] == {"a1": "X", "b2": "Y"}
    assert ctx["canone_calc"] == {"result": {"base_min_euro_mq": 10.5, "base_max_euro_mq": 12.0, "zona": "B1"}}
    assert owner_addr_id is not None


def test_known_current_behavior_missing_contract_kind_resets_existing_contract_to_concordato_on_temp_postgres(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    immobile_id = db_upsert_immobile(pg_conn, owner_cf, _make_immobile(sub=""))

    contract_id = db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter({"contract_kind": "STUDENTI", "durata_anni": "6"}),
    )
    pg_conn.commit()

    db_upsert_contract(pg_conn, immobile_id, ItemAdapter({}))
    pg_conn.commit()

    row = _fetchone_dict(
        pg_conn,
        "SELECT id, contract_kind, durata_anni FROM public.contracts WHERE id = %s;",
        (contract_id,),
    )

    assert str(row["id"]) == contract_id
    assert row["contract_kind"] == "CONCORDATO"
    assert row["durata_anni"] == 6


def test_build_immobili_document_from_db_generates_single_client_shape_with_blank_run_only_fields(pg_conn):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn, comune="Chieti")
    cond_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Conduttore", "civico": "33"},
    )
    db_upsert_person(pg_conn, "BNCMRA80A01H501Z", surname="Bianchi", name="Mario", address_id=cond_addr_id)

    visura_addr_b = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Beta", "civico": "20"},
    )
    visura_addr_a = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Via Alfa", "civico": "11"},
    )
    real_addr_a = db_upsert_address(
        pg_conn,
        {"comune": "Montesilvano", "via_full": "Via Override", "civico": "7", "piano": "4", "interno": "9"},
    )

    immobile_b = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(foglio="20", numero="100", sub="2", categoria="A/3", rendita="€ 555.00"),
        visura_addr_id=visura_addr_b,
        source_visura_id=None,
    )
    immobile_a = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(foglio="12", numero="345", sub="7", categoria="A/2", rendita="€ 123.45"),
        visura_addr_id=visura_addr_a,
        source_visura_id=None,
    )
    db_update_immobile_real_address(pg_conn, immobile_a, real_address_id=real_addr_a, energy_class="A")
    db_upsert_immobile_elements(pg_conn, immobile_a, ItemAdapter({"a1": "X", "d12": "Y"}))

    db_upsert_contract(
        pg_conn,
        immobile_a,
        ItemAdapter(
            {
                "contract_kind": "TRANSITORIO",
                "arredato": "0.10",
                "istat": 5,
                "ignore_surcharges": "yes",
                "conduttore_cf": "BNCMRA80A01H501Z",
                "contratto_data": "01/01/2025",
                "decorrenza_data": "01/02/2025",
                "registrazione_data": "05/02/2025",
                "registrazione_num": "REG-123",
                "agenzia_entrate_sede": "PESCARA",
                "canone_contrattuale_mensile": "650",
                "durata_anni": "4",
            }
        ),
    )
    pg_conn.commit()

    document = build_immobili_document_from_db(
        pg_conn,
        owner_cf,
        metadata_defaults=ImmobiliDocumentMetadataDefaults(
            comune="PESCARA",
            tipo_catasto="F",
            ufficio_provinciale_label="PESCARA Territorio",
        ),
    )

    assert document.locatore_cf == owner_cf
    assert document.comune == "PESCARA"
    assert document.tipo_catasto == "F"
    assert document.ufficio_label == "PESCARA Territorio"
    assert document.locatore_comune_res == "Chieti"
    assert document.locatore_via == "Via Roma"
    assert document.locatore_civico == "10"

    assert [(imm.foglio, imm.numero, imm.sub) for imm in document.immobili] == [
        ("12", "345", "7"),
        ("20", "100", "2"),
    ]

    first = document.immobili[0]
    assert first.enabled is True
    assert first.rendita == "€ 123.45"
    assert first.categoria == "A/2"
    assert first.visura_comune == "Pescara"
    assert first.visura_via == "Via Alfa"
    assert first.visura_civico == "11"
    assert first.immobile_comune == "Montesilvano"
    assert first.immobile_via == "Via Override"
    assert first.immobile_civico == "7"
    assert first.immobile_piano == "4"
    assert first.immobile_interno == "9"
    assert first.energy_class == "A"
    assert first.contract_kind == "TRANSITORIO"
    assert first.arredato == 0.1
    assert first.istat == 5.0
    assert first.ignore_surcharges is True
    assert first.elements == {"a1": "X", "d12": "Y"}

    # Run-only fields must stay blank in the generated document, even if present in contracts.
    assert first.conduttore_cf is None
    assert first.contratto_data is None
    assert first.decorrenza_data is None
    assert first.registrazione_data is None
    assert first.registrazione_num is None
    assert first.agenzia_entrate_sede is None
    assert first.canone_contrattuale_mensile is None
    assert first.durata_anni is None

    second = document.immobili[1]
    assert second.energy_class is None
    assert second.contract_kind is None
    assert second.arredato is None
    assert second.istat is None
    assert second.ignore_surcharges is None
    assert second.elements == {}


def test_immobili_yaml_generator_service_serializes_stably_and_raises_not_found(temp_postgres_db, pg_conn, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    owner_cf, _ = _seed_owner(pg_conn)
    visura_addr_id = db_upsert_address(
        pg_conn,
        {"comune": "Pescara", "via_full": "Corso Roma", "civico": "12"},
    )
    immobile_id = db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(sub=""),
        visura_addr_id=visura_addr_id,
        source_visura_id=None,
    )
    db_upsert_immobile_elements(pg_conn, immobile_id, ItemAdapter({"b2": "Y"}))
    db_upsert_contract(
        pg_conn,
        immobile_id,
        ItemAdapter(
            {
                "contract_kind": "STUDENTI",
                "arredato": "0.25",
                "istat": "2.5",
                "ignore_surcharges": "no",
                "contratto_data": "09/09/2025",
            }
        ),
    )
    pg_conn.commit()

    service = ImmobiliYamlGeneratorService(connection_factory=temp_postgres_db.connect)

    yaml_text_1 = service.dump_yaml(owner_cf)
    yaml_text_2 = service.dump_yaml(owner_cf)

    assert yaml_text_1 == yaml_text_2

    payload = yaml.safe_load(yaml_text_1)
    assert list(payload.keys()) == [
        "LOCATORE_CF",
        "COMUNE",
        "TIPO_CATASTO",
        "UFFICIO_PROVINCIALE_LABEL",
        "LOCATORE_COMUNE_RES",
        "LOCATORE_VIA",
        "LOCATORE_CIVICO",
        "immobili",
    ]
    assert payload["immobili"][0]["enabled"] is True
    assert payload["immobili"][0]["FOGLIO"] == "12"
    assert payload["immobili"][0]["VISURA_VIA"] == "Corso Roma"
    assert payload["immobili"][0]["CONTRACT_KIND"] == "STUDENTI"
    assert payload["immobili"][0]["ARREDATO"] == 0.25
    assert payload["immobili"][0]["ISTAT"] == 2.5
    assert payload["immobili"][0]["IGNORE_SURCHARGES"] is False
    assert payload["immobili"][0]["B2"] == "Y"
    assert payload["immobili"][0]["CONTRATTO_DATA"] == ""
    assert payload["immobili"][0]["CANONE_CONTRATTUALE_MENSILE"] == ""
    assert payload["immobili"][0]["DURATA_ANNI"] == ""

    output_path = tmp_path / "generated" / "generated-immobili.yml"
    write_immobili_document_yaml(service.build_document(owner_cf), output_path)
    assert output_path.read_text(encoding="utf-8") == yaml_text_1

    with pytest.raises(ImmobiliDocumentNotFoundError):
        service.build_document("MISSINGCF12345")


def test_prepare_document_presence_uses_person_plus_immobile_as_db_hit_criterion(pg_conn):
    """Prepare DB hit/miss should be deterministic and aligned with generator readiness."""
    owner_cf = "RSSMRA80A01H501Z"

    missing = db_load_prepare_document_presence(pg_conn, owner_cf)

    assert missing.locatore_cf == owner_cf
    assert missing.root_found is False
    assert missing.immobili_count == 0
    assert missing.is_hit is False

    _seed_owner(pg_conn, cf=owner_cf)
    root_only = db_load_prepare_document_presence(pg_conn, owner_cf)

    assert root_only.root_found is True
    assert root_only.immobili_count == 0
    assert root_only.is_hit is False

    visura_addr_id = db_upsert_address(
        pg_conn,
        {
            "comune": "Pescara",
            "via_full": "Corso Roma",
            "civico": "12",
        },
    )
    db_upsert_immobile(
        pg_conn,
        owner_cf,
        _make_immobile(),
        visura_addr_id=visura_addr_id,
        source_visura_id=None,
    )

    hit = db_load_prepare_document_presence(pg_conn, owner_cf)

    assert hit.root_found is True
    assert hit.immobili_count == 1
    assert hit.is_hit is True
