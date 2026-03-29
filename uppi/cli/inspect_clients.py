#!/usr/bin/env python3
"""CLI-утиліта для огляду поточного стану клієнтів у БД за актуальною схемою."""

import argparse
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from decouple import config
import psycopg2
from psycopg2.extras import RealDictCursor

from uppi.domain.clients import CLIENTS_FILE, load_clients


# =========================================================
# DB config
# =========================================================

DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")

# =========================================================
# helpers
# =========================================================

def get_conn():
    """Повертає підключення до PostgreSQL для support CLI."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
    except Exception as e:
        raise RuntimeError(f"❌ DB connection failed: {e}") from e


def fmt(value: Any) -> str:
    """Нормалізує значення для безпечного текстового виводу в CLI."""
    if value is None:
        return "—"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


def print_kv(key: str, value: Any, indent: int = 2):
    """Друкує одну пару ключ-значення у вирівняному вигляді."""
    pad = " " * indent
    print(f"{pad}{key:30}: {fmt(value)}")


# =========================================================
# fetchers
# =========================================================

def fetch_person(conn, cf: str) -> Optional[Dict[str, Any]]:
    """Завантажує базову інформацію про особу за CF."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
    """Повертає запис про візуру орендодавця за актуальним CF-ключем."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM visure
            WHERE locatore_cf = %s
            """,
            (cf,),
        )
        return cur.fetchone()


def fetch_immobili(conn, cf: str) -> List[Dict[str, Any]]:
    """Повертає нерухомість власника разом з поточною проєкцією адреси."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                i.*,
                COALESCE(ra.comune, va.comune) AS immobile_comune,
                COALESCE(ra.via_full, va.via_full) AS immobile_via,
                COALESCE(ra.civico, va.civico) AS immobile_civico
            FROM immobili i
            LEFT JOIN addresses va ON i.visura_address_id = va.id
            LEFT JOIN addresses ra ON i.real_address_id = ra.id
            WHERE i.owner_cf = %s
            ORDER BY COALESCE(ra.comune, va.comune), i.foglio, i.numero, i.sub, i.id
            """,
            (cf,),
        )
        return cur.fetchall()


def fetch_contracts(conn, immobile_id: int) -> List[Dict[str, Any]]:
    """Завантажує контракти, прив’язані до конкретного immobile."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def fetch_contract_participants(conn, contract_id: str) -> List[Dict[str, Any]]:
    """Будує список сторін договору з поточної схеми без legacy-таблиць."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                'LOCATORE' AS role,
                p_own.cf,
                p_own.name,
                p_own.surname
            FROM contracts c
            JOIN immobili i ON i.id = c.immobile_id
            LEFT JOIN persons p_own ON p_own.cf = i.owner_cf
            WHERE c.id = %s
              AND p_own.cf IS NOT NULL

            UNION ALL

            SELECT
                'CONDUTTORE' AS role,
                p_cond.cf,
                p_cond.name,
                p_cond.surname
            FROM contracts c
            LEFT JOIN persons p_cond ON p_cond.cf = c.conduttore_cf
            WHERE c.id = %s
              AND p_cond.cf IS NOT NULL
            """,
            (contract_id, contract_id),
        )
        return cur.fetchall()


def fetch_canone(conn, contract_id: str) -> List[Dict[str, Any]]:
    """Завантажує історію розрахунків канону для контракту."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


def fetch_address_sources(conn, immobile_id: int) -> Optional[Dict[str, Any]]:
    """Показує, які адресні джерела прив’язані до immobile у поточній схемі."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM (
                SELECT
                    i.id AS immobile_id,
                    i.visura_address_id,
                    i.real_address_id,
                    va.comune AS visura_comune,
                    va.via_full AS visura_via_full,
                    va.civico AS visura_civico,
                    ra.comune AS real_comune,
                    ra.via_full AS real_via_full,
                    ra.civico AS real_civico
                FROM immobili i
                LEFT JOIN addresses va ON va.id = i.visura_address_id
                LEFT JOIN addresses ra ON ra.id = i.real_address_id
                WHERE i.id = %s
            ) AS address_sources
            """,
            (immobile_id,),
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
    print_kv("FOGLIO", imm.get("foglio"), 4)
    print_kv("NUMERO", imm.get("numero"), 4)
    print_kv("SUB", imm.get("sub"), 4)


def print_block_2_full_dump(
    imm: Dict[str, Any],
    contracts: List[Dict[str, Any]],
    conn,
):
    """Друкує повний DB-oriented dump immobile, contracts і пов’язаних сутностей."""
    print("  🔸 BLOCK 2 — Вся інформація з БД")

    print("    ▸ IMMOBILE")
    for k, v in imm.items():
        print_kv(k, v, 6)

    if not contracts:
        print("    ▸ CONTRACTS: — (немає)")
        return

    for cidx, contract in enumerate(contracts, start=1):
        print(f"    ▸ CONTRACT [{cidx}] {contract.get('id')}")
        for k, v in contract.items():
            print_kv(k, v, 8)

        parties = fetch_contract_participants(conn, contract["id"])
        print("        ▸ PARTIES")
        for p in parties:
            print_kv(f"{p['role']}", f"{p['name']} {p['surname']} ({p['cf']})", 10)

        canoni = fetch_canone(conn, contract["id"])
        if canoni:
            print("        ▸ CANONE_CALCOLI")
            for calc in canoni:
                for k, v in calc.items():
                    print_kv(k, v, 10)
        else:
            print("        ▸ CANONE_CALCOLI: —")

        address_sources = fetch_address_sources(conn, imm["id"])
        if address_sources:
            print("        ▸ ADDRESS_SOURCES")
            for k, v in address_sources.items():
                print_kv(k, v, 10)


# =========================================================
# main
# =========================================================

def main():
    """Запускає CLI-огляд по одному або всіх CF із clients.yml."""
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
            print(f"❌ clients.yml порожній або не знайдений ({CLIENTS_FILE})")
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
