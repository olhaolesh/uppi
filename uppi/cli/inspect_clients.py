#!/usr/bin/env python3
import argparse
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from decouple import config
import psycopg
from psycopg.rows import dict_row

from uppi.domain.clients import load_clients


# =========================================================
# DB config
# =========================================================

DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")

UPPI_CLIENTS_YAML = config("UPPI_CLIENTS_YAML", default="clients/clients.yml")


# =========================================================
# helpers
# =========================================================

def get_conn() -> psycopg.Connection:
    try:
        return psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
    except Exception as e:
        raise RuntimeError(f"❌ DB connection failed: {e}") from e


def fmt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


def print_kv(key: str, value: Any, indent: int = 2):
    pad = " " * indent
    print(f"{pad}{key:30}: {fmt(value)}")


# =========================================================
# fetchers
# =========================================================

def fetch_person(conn, cf: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT cf, name, surname, created_at, updated_at
            FROM persons
            WHERE cf = %s
            """,
            (cf,),
        )
        return cur.fetchone()


def fetch_visura(conn, cf: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM visure
            WHERE cf = %s
            """,
            (cf,),
        )
        return cur.fetchone()


def fetch_immobili(conn, cf: str) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM immobili
            WHERE visura_cf = %s
            ORDER BY immobile_comune, foglio, numero, sub, id
            """,
            (cf,),
        )
        return cur.fetchall()


def fetch_contracts(conn, immobile_id: int) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM contracts
            WHERE immobile_id = %s
            ORDER BY created_at DESC
            """,
            (immobile_id,),
        )
        return cur.fetchall()


def fetch_contract_parties(conn, contract_id: str) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT cp.role, p.cf, p.name, p.surname
            FROM contract_parties cp
            JOIN persons p ON p.cf = cp.person_cf
            WHERE cp.contract_id = %s
            """,
            (contract_id,),
        )
        return cur.fetchall()


def fetch_canone(conn, contract_id: str) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM canone_calcoli
            WHERE contract_id = %s
            ORDER BY created_at DESC
            """,
            (contract_id,),
        )
        return cur.fetchall()


def fetch_overrides(conn, contract_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
            FROM contract_overrides
            WHERE contract_id = %s
            """,
            (contract_id,),
        )
        return cur.fetchone()


# =========================================================
# printers
# =========================================================

def print_block_1_yaml_hint(cf: str, imm: Dict[str, Any]):
    """
    BLOCK 1 — тільки те, що має сенс для clients.yml
    """
    print("  🔹 BLOCK 1 — Дані для clients.yml")
    print_kv("LOCATORE_CF", cf, 4)
    print_kv("IMMOBILE_COMUNE", imm.get("immobile_comune"), 4)
    print_kv("IMMOBILE_FOGLIO", imm.get("foglio"), 4)
    print_kv("IMMOBILE_NUMERO", imm.get("numero"), 4)
    print_kv("IMMOBILE_SUB", imm.get("sub"), 4)


def print_block_2_full_dump(
    imm: Dict[str, Any],
    contracts: List[Dict[str, Any]],
    conn: psycopg.Connection,
):
    print("  🔸 BLOCK 2 — Вся інформація з БД")

    print("    ▸ IMMOBILE")
    for k, v in imm.items():
        print_kv(k, v, 6)

    if not contracts:
        print("    ▸ CONTRACTS: — (немає)")
        return

    for cidx, contract in enumerate(contracts, start=1):
        print(f"    ▸ CONTRACT [{cidx}] {contract['contract_id']}")
        for k, v in contract.items():
            print_kv(k, v, 8)

        parties = fetch_contract_parties(conn, contract["contract_id"])
        print("        ▸ PARTIES")
        for p in parties:
            print_kv(f"{p['role']}", f"{p['name']} {p['surname']} ({p['cf']})", 10)

        canoni = fetch_canone(conn, contract["contract_id"])
        if canoni:
            print("        ▸ CANONE_CALCOLI")
            for calc in canoni:
                for k, v in calc.items():
                    print_kv(k, v, 10)
        else:
            print("        ▸ CANONE_CALCOLI: —")

        overrides = fetch_overrides(conn, contract["contract_id"])
        if overrides:
            print("        ▸ CONTRACT_OVERRIDES")
            for k, v in overrides.items():
                print_kv(k, v, 10)


# =========================================================
# main
# =========================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Огляд усієї наявної інформації по клієнтах з БД "
            "(persons → visure → immobili → contracts).\n"
            "Без аргументів — працює з усіма CF з clients.yml.\n"
            "З --cf — тільки з вказаним CF."
        )
    )
    parser.add_argument(
        "--cf",
        help="Codice Fiscale locatore (якщо не задано — береться з clients.yml)",
    )
    args = parser.parse_args()

    # -------------------------------------------------
    # 1) Визначаємо, з якими CF працюємо
    # -------------------------------------------------
    target_cfs: List[str] = []

    if args.cf:
        # Явно передали CF через CLI
        cf = args.cf.strip().upper()
        if not cf:
            print("❌ --cf переданий, але порожній")
            return
        target_cfs = [cf]
    else:
        # CF не передали → беремо з clients.yml
        rows = load_clients()
        if not rows:
            print(f"❌ clients.yml порожній або не знайдений ({UPPI_CLIENTS_YAML})")
            return

        for row in rows:
            cf = str(row.get("LOCATORE_CF", "")).strip().upper()
            if cf:
                target_cfs.append(cf)

        # прибираємо дублікати, зберігаючи порядок
        seen = set()
        target_cfs = [cf for cf in target_cfs if not (cf in seen or seen.add(cf))]

        if not target_cfs:
            print("❌ У clients.yml немає жодного валідного LOCATORE_CF")
            return

    # -------------------------------------------------
    # 2) Підключення до БД
    # -------------------------------------------------
    conn = get_conn()

    try:
        # -------------------------------------------------
        # 3) Основний цикл по CF
        # -------------------------------------------------
        for idx, cf in enumerate(target_cfs, start=1):
            print("=" * 80)
            print(f"[{idx}] CF: {cf}")
            print("=" * 80)

            # ---------- PERSON ----------
            person = fetch_person(conn, cf)
            if not person:
                print(f"❌ PERSONS: CF {cf} не знайдено в БД")
                continue

            print(f"Locatore: {person.get('name')} {person.get('surname')}")

            # ---------- VISURA ----------
            visura = fetch_visura(conn, cf)
            if not visura:
                print("❌ VISURA: відсутня (потрібно запускати спайдер)")
                continue

            print("\nVISURA:")
            for k, v in visura.items():
                print_kv(k, v, 2)

            # ---------- IMMOBILI ----------
            immobili = fetch_immobili(conn, cf)
            print(f"\nIMMOBILI: {len(immobili)}")

            if not immobili:
                print("⚠️ Візура є, але immobili відсутні")
                continue

            for imm_idx, imm in enumerate(immobili, start=1):
                print("\n" + "-" * 80)
                print(f"IMMOBILE [{imm_idx}] id={imm.get('id')}")
                print("-" * 80)

                # Блок 1 — підказка для clients.yml
                print_block_1_yaml_hint(cf, imm)

                # Блок 2 — повний дамп з БД
                contracts = fetch_contracts(conn, imm["id"])
                print_block_2_full_dump(imm, contracts, conn)

            print("\n")

        print("=" * 80)

    finally:
        conn.close()



if __name__ == "__main__":
    main()
