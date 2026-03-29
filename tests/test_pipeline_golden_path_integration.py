"""Golden-path integration test для non-browser pipeline після visura download."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml
from itemadapter import ItemAdapter

import uppi.domain.storage as storage_module
import uppi.parsers.visura_pdf_parser as parser_module
from uppi.domain.clients import _parse_yaml
from uppi.domain.object_storage import ObjectStorage, ObjectStorageConfig
from uppi.docs.attestazione_template_filler import fill_underscored
from uppi.items import UppiItem
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.services.attestazione_generator import build_template_params
from uppi.services.db_repo import (
    db_load_contract_context,
    db_update_immobile_real_address,
    db_upsert_address,
    db_upsert_contract,
    db_upsert_immobile,
    db_upsert_immobile_elements,
    db_upsert_person,
    db_upsert_visura,
    immobile_from_parsed_dict,
)
from uppi.utils.item_mapper import map_yaml_to_item
from uppi.utils.parse_utils import clean_str, split_full_name


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "visura_parser"


def _load_parser_fixture(name: str) -> dict:
    """Завантажує fixture або тестовий артефакт для цього набору тестів."""
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as f:
        return json.load(f)


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

    def fetchall(self):
        """Повертає заздалегідь підготовлений список тестових рядків."""
        if self.conn.fetchall_results:
            return self.conn.fetchall_results.pop(0)
        return []


class RecordingConnection:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, *, fetchone_results=None, fetchall_results=None):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.executed = []
        self.rollback_called = False

    def cursor(self, cursor_factory=None):
        """Повертає тестовий курсор для імітації DB-доступу."""
        return RecordingCursor(self)

    def rollback(self):
        """Імітує rollback без зміни зовнішнього стану."""
        self.rollback_called = True


def _statement_params(conn: RecordingConnection, contains: str):
    """Допомагає дістати потрібний SQL-фрагмент або параметри для assert-перевірки."""
    for sql, params in conn.executed:
        if contains in sql:
            return params
    raise AssertionError(f"Statement containing {contains!r} not found. Executed: {conn.executed!r}")


def _statement_params_all(conn: RecordingConnection, contains: str) -> list:
    """Допомагає дістати потрібний SQL-фрагмент або параметри для assert-перевірки."""
    return [params for sql, params in conn.executed if contains in sql]


class _FakePage:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, text: str = "", blocks=None):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self._text = text
        self._blocks = blocks or []

    def get_text(self, mode: str):
        """Допоміжний тестовий хелпер для цього модуля."""
        if mode == "blocks":
            return self._blocks
        if mode == "text":
            return self._text
        raise AssertionError(f"Unexpected get_text mode: {mode}")


class _FakeDoc:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, pages_data):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self._pages = [_FakePage(text=p.get("text", ""), blocks=p.get("blocks", [])) for p in pages_data]

    def __len__(self):
        """Повертає кількість елементів у тестовому контейнері."""
        return len(self._pages)

    def __getitem__(self, idx: int):
        """Повертає потрібний елемент із тестового контейнера."""
        return self._pages[idx]

    def close(self):
        """Імітує закриття тестового ресурсу."""
        return None


class _FakeTable:
    """Тестовий double-об’єкт для ізоляції зовнішніх залежностей."""
    def __init__(self, rows):
        """Ініціалізує тестовий double-об’єкт для поточного сценарію."""
        self.df = pd.DataFrame(rows)


def _patch_parser_io(monkeypatch, fixture_data: dict):
    """Підміняє зовнішні залежності контрольованими тестовими double-об’єктами."""
    def fake_open(_pdf_path):
        return _FakeDoc(fixture_data.get("pages", []))

    def fake_read_pdf(_pdf_path, *, pages: str, flavor: str):
        assert flavor == "lattice"
        return [_FakeTable(rows) for rows in fixture_data.get("tables", {}).get(pages, [])]

    monkeypatch.setattr(parser_module.fitz, "open", fake_open)
    monkeypatch.setattr(parser_module.camelot, "read_pdf", fake_read_pdf)


def test_golden_path_after_visura_download_stitches_yaml_parser_db_docx_and_storage_naming(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text(
        yaml.safe_dump(
            [
                {
                    "LOCATORE_CF": "RSSMRA80A01H501Z",
                    "LOCATORE_COMUNE_RES": "Pescara",
                    "LOCATORE_VIA": "Via Roma",
                    "LOCATORE_CIVICO": "10",
                    "IMMOBILE_COMUNE": "Pescara",
                    "IMMOBILE_VIA": "Corso Roma",
                    "IMMOBILE_CIVICO": "12",
                    "IMMOBILE_PIANO": "3",
                    "IMMOBILE_INTERNO": "4",
                    "FOGLIO": "12",
                    "NUMERO": "345",
                    "SUB": "7",
                    "CONTRATTO_DATA": "01/01/2025",
                    "CONDUTTORE_NOME": "Mario Bianchi",
                    "CONDUTTORE_CF": "BNCMRA80A01H501Z",
                    "CONDUTTORE_COMUNE": "Pescara",
                    "CONDUTTORE_VIA": "Via Test 12",
                    "DECORRENZA_DATA": "01/02/2025",
                    "REGISTRAZIONE_DATA": "05/02/2025",
                    "REGISTRAZIONE_NUM": "123",
                    "AGENZIA_ENTRATE_SEDE": "PESCARA",
                    "CONTRACT_KIND": "TRANSITORIO",
                    "ARREDATO": "0.10",
                    "ENERGY_CLASS": "B",
                    "CANONE_CONTRATTUALE_MENSILE": "650",
                    "DURATA_ANNI": "4",
                    "ISTAT": 5,
                    "IGNORE_SURCHARGES": "yes",
                    "A1": "X",
                    "B2": "X",
                    "D5": "Y",
                }
            ],
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    loaded_clients = _parse_yaml(yaml_path)
    assert len(loaded_clients) == 1

    client = loaded_clients[0]
    mapped = map_yaml_to_item(client)
    mapped.setdefault("locatore_cf", client["LOCATORE_CF"])
    mapped["visura_source"] = "sister"
    mapped["visura_downloaded"] = True
    mapped["visura_download_path"] = str(tmp_path / "VISURA_RSSMRA80A01H501Z.pdf")
    mapped["nav_to_visure_catastali"] = True
    mapped["captcha_ok"] = True

    item = UppiItem(**mapped)
    adapter = ItemAdapter(item)

    fixture = _load_parser_fixture("happy_path.json")
    _patch_parser_io(monkeypatch, fixture)

    parsed_dicts = VisuraParser().parse("dummy.pdf")
    assert len(parsed_dicts) == 1
    parsed = parsed_dicts[0]

    assert adapter.get("locatore_cf") == parsed["locatore_codice_fiscale"]
    assert adapter.get("foglio") == parsed["foglio"]
    assert adapter.get("numero") == parsed["numero"]
    assert adapter.get("sub") == parsed["sub"]

    storage = ObjectStorage(
        ObjectStorageConfig(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
            visure_bucket="visure-bucket",
            attestazioni_bucket="attestazioni-bucket",
        )
    )

    conn = RecordingConnection(
        fetchone_results=[
            (11,),
            (12,),
            (21,),
            (13,),
            (31,),
            (14,),
            None,
            (41,),
        ]
    )

    locatore_cf = clean_str(adapter.get("locatore_cf"))
    loc_addr_id = db_upsert_address(
        conn,
        {
            "comune": adapter.get("locatore_comune_res"),
            "via_full": adapter.get("locatore_via"),
            "civico": adapter.get("locatore_civico"),
        },
    )
    db_upsert_person(
        conn,
        locatore_cf,
        surname=clean_str(adapter.get("locatore_surname")),
        name=clean_str(adapter.get("locatore_name")),
        address_id=loc_addr_id,
    )

    cond_full_name = clean_str(adapter.get("conduttore_nome"))
    cond_surname, cond_name = split_full_name(cond_full_name)
    cond_addr_id = db_upsert_address(
        conn,
        {
            "comune": adapter.get("conduttore_comune"),
            "via_full": adapter.get("conduttore_via") or "",
        },
    )
    db_upsert_person(
        conn,
        clean_str(adapter.get("conduttore_cf")),
        surname=cond_surname,
        name=cond_name,
        address_id=cond_addr_id,
    )

    visura_object_name = storage.visura_object_name(locatore_cf)
    visura_db_id = db_upsert_visura(
        conn,
        locatore_cf,
        storage.cfg.visure_bucket,
        visura_object_name,
        "checksum-sha-256",
        fetched_now=True,
    )

    db_upsert_person(
        conn,
        locatore_cf,
        surname=clean_str(adapter.get("locatore_surname")) or parsed.get("locatore_surname"),
        name=clean_str(adapter.get("locatore_name")) or parsed.get("locatore_name"),
        address_id=loc_addr_id,
    )

    visura_addr_id = db_upsert_address(
        conn,
        {
            "comune": parsed.get("immobile_comune"),
            "via_full": parsed.get("via_name") or parsed.get("indirizzo_raw"),
            "civico": parsed.get("via_num"),
            "piano": parsed.get("piano"),
            "interno": parsed.get("interno"),
            "scala": parsed.get("scala"),
        },
    )
    imm = immobile_from_parsed_dict(parsed)
    immobile_id = db_upsert_immobile(
        conn,
        locatore_cf,
        imm,
        visura_addr_id=visura_addr_id,
        source_visura_id=visura_db_id,
    )

    real_addr_id = db_upsert_address(
        conn,
        {
            "comune": adapter.get("immobile_comune"),
            "via_full": adapter.get("immobile_via"),
            "civico": adapter.get("immobile_civico"),
            "piano": adapter.get("immobile_piano"),
            "interno": adapter.get("immobile_interno"),
        },
    )
    db_update_immobile_real_address(
        conn,
        immobile_id,
        real_address_id=real_addr_id,
        energy_class=adapter.get("energy_class"),
    )
    db_upsert_immobile_elements(conn, immobile_id, adapter)
    contract_id = db_upsert_contract(conn, immobile_id, adapter)

    assert visura_db_id == 21
    assert immobile_id == 31
    assert contract_id == "41"

    address_inserts = _statement_params_all(conn, "INSERT INTO public.addresses")
    loc_addr_params, cond_addr_params, visura_addr_params, real_addr_params = address_inserts
    person_upserts = _statement_params_all(conn, "INSERT INTO public.persons")
    visura_params = _statement_params(conn, "INSERT INTO public.visure")
    immobile_params = _statement_params(conn, "INSERT INTO public.immobili")
    immobile_update_params = _statement_params(conn, "UPDATE public.immobili SET")
    contract_params = _statement_params(conn, "INSERT INTO public.contracts")
    immobile_element_upserts = _statement_params_all(conn, "INSERT INTO public.immobile_elements")

    assert loc_addr_params["comune"] == "Pescara"
    assert loc_addr_params["via_full"] == "Via Roma"
    assert cond_addr_params["via_full"] == "Via Test 12"
    assert visura_addr_params["via_full"] == "ROMA 10 P."
    assert real_addr_params["via_full"] == "Corso Roma"

    assert person_upserts[0] == ("RSSMRA80A01H501Z", None, None, 11)
    # known_current_behavior: split_full_name() treats the first token as surname.
    assert person_upserts[1] == ("BNCMRA80A01H501Z", "Mario", "Bianchi", 12)
    assert person_upserts[2] == ("RSSMRA80A01H501Z", "ROSSI", "MARIO", 11)

    assert visura_params["cf"] == "RSSMRA80A01H501Z"
    assert visura_params["bucket"] == "visure-bucket"
    assert visura_params["obj"] == "visure/RSSMRA80A01H501Z.pdf"
    assert visura_params["sum"] == "checksum-sha-256"
    assert isinstance(visura_params["now"], datetime)

    assert immobile_params["owner_cf"] == "RSSMRA80A01H501Z"
    assert immobile_params["source_visura_id"] == 21
    assert immobile_params["visura_addr_id"] == 13
    assert immobile_params["foglio"] == "12"
    assert immobile_params["numero"] == "345"
    assert immobile_params["sub"] == "7"
    assert immobile_params["zona_cens"] == "2"
    assert immobile_params["micro_zona"] == "5"
    assert immobile_params["categoria"] == "A/2"
    assert immobile_params["classe"] == "3"
    assert immobile_params["consistenza"] == "5 vani"
    assert immobile_params["rendita"] == "€ 123.45"
    assert immobile_params["superficie_totale"] == 98.7
    assert immobile_params["superficie_raw"] == "Totale: 98,7 Totale escluse aree scoperte**: 90,1"

    assert immobile_update_params == [14, "B", 31]
    assert immobile_element_upserts == [
        (31, "A", "1", "X"),
        (31, "B", "2", "X"),
        (31, "D", "5", "Y"),
    ]

    assert contract_params["immobile_id"] == 31
    assert contract_params["cond_cf"] == "BNCMRA80A01H501Z"
    assert contract_params["kind"] == "TRANSITORIO"
    assert contract_params["start_date"] == date(2025, 1, 1)
    assert contract_params["durata"] == 4
    assert contract_params["decorrenza"] == date(2025, 2, 1)
    assert contract_params["reg_data"] == date(2025, 2, 5)
    assert contract_params["reg_num"] == "123"
    assert contract_params["ae_sede"] == "PESCARA"
    assert contract_params["canone"] == 650.0
    assert contract_params["istat"] == 5.0
    assert contract_params["arredato"] == 0.1
    assert contract_params["ignore_surcharges"] is True

    contract_row = {
        "id": contract_id,
        "immobile_id": immobile_id,
        "contract_kind": contract_params["kind"],
        "start_date": contract_params["start_date"],
        "durata_anni": contract_params["durata"],
        "decorrenza_data": contract_params["decorrenza"],
        "registrazione_data": contract_params["reg_data"],
        "registrazione_num": contract_params["reg_num"],
        "agenzia_entrate_sede": contract_params["ae_sede"],
        "canone_contrattuale_mensile": contract_params["canone"],
        "istat_rate": contract_params["istat"],
        "arredato_pct": contract_params["arredato"],
        "ignore_surcharges": contract_params["ignore_surcharges"],
        "loc_cf": locatore_cf,
        "loc_name": parsed["locatore_name"],
        "loc_surname": parsed["locatore_surname"],
        "loc_comune": loc_addr_params["comune"],
        "loc_via": loc_addr_params["via_full"],
        "loc_civico": loc_addr_params["civico"],
        "cond_cf": clean_str(adapter.get("conduttore_cf")),
        "cond_name": cond_name,
        "cond_surname": cond_surname,
        "cond_comune": cond_addr_params["comune"],
        "cond_via": cond_addr_params["via_full"],
        "imm_comune": real_addr_params["comune"],
        "imm_via": real_addr_params["via_full"],
        "imm_civico": real_addr_params["civico"],
        "imm_piano": real_addr_params["piano"],
        "imm_interno": real_addr_params["interno"],
        "imm_energy_class": "B",
    }
    read_conn = RecordingConnection(
        fetchone_results=[contract_row, None],
        fetchall_results=[[("A", "1", "X"), ("B", "2", "X"), ("D", "5", "Y")]],
    )
    contract_ctx = db_load_contract_context(read_conn, contract_id)
    params = build_template_params(adapter, imm, contract_ctx)

    monkeypatch.setattr(storage_module, "DOWNLOADS_DIR", tmp_path / "downloads")
    attestazione_path = storage_module.get_attestazione_path(locatore_cf, contract_id, imm)
    attestazione_object_name = storage.attestazione_object_name(locatore_cf, contract_id)

    assert contract_ctx["elements"] == {"a1": "X", "b2": "X", "d5": "Y"}
    assert contract_ctx["immobile"] == {
        "comune": "Pescara",
        "via": "Corso Roma",
        "civico": "12",
        "piano": "3",
        "interno": "4",
        "energy_class": "B",
    }

    assert params["{{LOCATORE_CF}}"] == "RSSMRA80A01H501Z"
    assert params["{{LOCATORE_NOME}}"] == "Mario Rossi"
    assert params["{{LOCATORE_COMUNE_RES}}"] == "Pescara"
    assert params["{{LOCATORE_VIA}}"] == fill_underscored("Via Roma", 27).strip()
    assert params["{{IMMOBILE_COMUNE}}"] == "Pescara"
    assert params["{{IMMOBILE_VIA}}"] == "Corso Roma"
    assert params["{{IMMOBILE_CIVICO}}"] == "12"
    assert params["{{IMMOBILE_PIANO}}"] == "3"
    assert params["{{IMMOBILE_INTERNO}}"] == "4"
    assert params["{{FOGLIO}}"] == "12"
    assert params["{{NUMERO}}"] == "345"
    assert params["{{SUB}}"] == "7"
    assert params["{{CONDUTTORE_NOME}}"] == "Mario Bianchi"
    assert params["{{CONDUTTORE_CF}}"] == "BNCMRA80A01H501Z"
    assert params["{{REGISTRAZIONE_NUM}}"] == "123"
    assert params["{{A1}}"] == "X"
    assert params["{{B2}}"] == "X"
    assert params["{{D5}}"] == "Y"
    assert params["{{A_CNT}}"] == "1"
    assert params["{{B_CNT}}"] == "1"
    assert params["{{D_CNT}}"] == "1"
    assert params["{{CAN_MENSILE}}"] == "650.00"

    # known_current_behavior: slugify_immobile() preserves raw consistenza and keeps the space in "5 vani".
    assert storage_module.slugify_immobile(imm) == "F12_N345_S7_Z2_MZ5_CATA2_CL3_CONS5 vani"
    assert attestazione_path.name == (
        "ATTESTAZIONE_RSSMRA80A01H501Z_41_F12_N345_S7_Z2_MZ5_CATA2_CL3_CONS5 vani.docx"
    )
    assert attestazione_object_name == "attestazioni/RSSMRA80A01H501Z/41.docx"
