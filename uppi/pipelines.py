# uppi/pipelines.py
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import Error as Psycopg2Error
from itemadapter import ItemAdapter
from decouple import config

from uppi.domain.db import get_pg_connection
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage
from uppi.domain.storage import get_attestazione_path, get_client_dir, get_visura_path
from uppi.docs.visura_pdf_parser import VisuraParser

from uppi.utils.audit import mask_username, safe_unlink, sha256_file, sha256_text, format_person_fullname
from uppi.utils.parse_utils import clean_str, clean_sub,  parse_date, safe_float, to_bool_or_none
from uppi.utils.db_utils.key_normalize import normalize_element_key

# Твої існуючі модулі для DOCX (я не змінюю API, тільки викликаю)
from uppi.docs.attestazione_template_filler import fill_attestazione_template, underscored

# Якщо ці модулі є — робимо canone; якщо ні — не валимо весь пайплайн
try:
    from uppi.domain.pescara2018_calc import compute_base_canone, CanoneCalculationError
    from uppi.domain.canone_models import CanoneInput, ContractKind
except Exception:  # pragma: no cover
    compute_base_canone = None
    CanoneCalculationError = Exception
    CanoneInput = None
    ContractKind = None


logger = logging.getLogger(__name__)

AE_USERNAME = config("AE_USERNAME", default="").strip()
TEMPLATE_VERSION = config("TEMPLATE_VERSION", default="pescara2018_v1").strip()

PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS = config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS", default="True").strip().lower() == "true"
DELETE_LOCAL_VISURA_AFTER_UPLOAD = config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False").strip().lower() == "true"


# ============================================================================
# DB COLUMN SETS (жорстко по схемі БД, не по dataclass)
# ============================================================================

IMMOBILI_DB_COLUMNS = [
    "table_num_immobile",
    "sez_urbana",
    "foglio",
    "numero",
    "sub",
    "zona_cens",
    "micro_zona",
    "categoria",
    "classe",
    "consistenza",
    "rendita",
    "superficie_totale",
    "superficie_escluse",
    "superficie_raw",
    "immobile_comune",
    "immobile_comune_code",
    "via_type",
    "via_name",
    "via_num",
    "scala",
    "interno",
    "piano",
    "indirizzo_raw",
    "dati_ulteriori",
]


ELEMENT_KEYS = (
    ["a1", "a2"]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)


# ============================================================================
# Helpers
# ============================================================================

def immobile_from_parsed_dict(d: Dict[str, Any]) -> Immobile:
    """
    Безпечне створення Immobile з dict парсера.
    """
    # Нормалізація типів
    if "superficie_totale" in d:
        d["superficie_totale"] = safe_float(d.get("superficie_totale"))
    if "superficie_escluse" in d:
        d["superficie_escluse"] = safe_float(d.get("superficie_escluse"))

    # Повертаємо dataclass (може містити більше полів, ніж БД — не проблема)
    return Immobile(**d)


def immobile_db_row(imm: Immobile) -> Dict[str, Any]:
    """
    Формує dict ТІЛЬКИ з колонок таблиці public.immobili + робить нормалізацію.

    Важливо:
    - sub завжди повертаємо як '' (не NULL), щоб коректно працював UNIQUE/ON CONFLICT.
    - foglio/numero/інші строкові поля чистимо від пробілів/переносів.
    - superficie_* приводимо до float або None.
    """
    row: Dict[str, Any] = {}

    for col in IMMOBILI_DB_COLUMNS:
        raw = getattr(imm, col, None)

        if col == "sub":
            row[col] = clean_sub(raw)
            continue

        if col in ("superficie_totale", "superficie_escluse"):
            row[col] = safe_float(raw)
            continue

        # більшість текстових
        row[col] = clean_str(raw)

    # Додатково: foglio/numero бажано завжди як текст (у БД колонки TEXT)
    # Інакше можуть проскакувати 11 як int, що не страшно, але краще стабільно.
    if row.get("foglio") is not None:
        row["foglio"] = str(row["foglio"]).strip()
    if row.get("numero") is not None:
        row["numero"] = str(row["numero"]).strip()

    return row


def find_local_visura_pdf(cf: str, adapter: ItemAdapter) -> Optional[Path]:
    """
    Знаходимо PDF для парсингу/аплоаду:
      1) visura_download_path з item
      2) downloads/<cf>/VISURA_<cf>.pdf
      3) latest DOC_*.pdf у каталозі клієнта
    """
    p = clean_str(adapter.get("visura_download_path"))
    if p:
        path = Path(p)
        if path.exists():
            return path

    fallback = get_visura_path(cf)
    if fallback.exists():
        return fallback

    client_dir = get_client_dir(cf)
    candidates = sorted(client_dir.glob("DOC_*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    # інколи spider качає просто *.pdf
    any_pdf = sorted(client_dir.glob("*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if any_pdf:
        return any_pdf[0]

    return None


def filter_immobiles_by_yaml(immobiles: List[Tuple[int, Immobile]], adapter: ItemAdapter) -> List[Tuple[int, Immobile]]:
    """
    Фільтр об'єктів по YAML критеріям.
    """
    foglio_f = clean_str(adapter.get("foglio"))
    numero_f = clean_str(adapter.get("numero"))
    sub_f = clean_str(adapter.get("sub"))
    categoria_f = clean_str(adapter.get("categoria"))
    rendita_f = clean_str(adapter.get("rendita"))
    superficie_f = safe_float(adapter.get("superficie_totale"))

    out: List[Tuple[int, Immobile]] = []
    for imm_id, imm in immobiles:
        if foglio_f and str(getattr(imm, "foglio", "") or "") != foglio_f:
            continue
        if numero_f and str(getattr(imm, "numero", "") or "") != numero_f:
            continue
        if sub_f and str(getattr(imm, "sub", "") or "") != sub_f:
            continue
        if categoria_f and str(getattr(imm, "categoria", "") or "") != categoria_f:
            continue
        if rendita_f and str(getattr(imm, "rendita", "") or "") != rendita_f:
            continue
        if superficie_f is not None:
            st = getattr(imm, "superficie_totale", None)
            if st is not None:
                try:
                    if float(st) != float(superficie_f):
                        continue
                except Exception:
                    pass
        out.append((imm_id, imm))
        logger.info(f"[PIPELINE] Immobile ID={imm_id} пройшов фільтр YAML. {out}")
    return out


# ============================================================================
# DB operations (в pipeline, але конекшн — у uppi/domain/db.py)
# ============================================================================

def db_upsert_person(conn, cf: str, surname: Optional[str], name: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.persons (cf, surname, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (cf) DO UPDATE
            SET
              surname = COALESCE(EXCLUDED.surname, persons.surname),
              name    = COALESCE(EXCLUDED.name, persons.name);
            """,
            (cf, surname, name),
        )


def db_upsert_visura(conn, cf: str, pdf_bucket: str, pdf_object: str, checksum_sha256: Optional[str], fetched_now: bool) -> None:
    with conn.cursor() as cur:
        if fetched_now:
            cur.execute(
                """
                INSERT INTO public.visure (cf, pdf_bucket, pdf_object, checksum_sha256, fetched_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (cf) DO UPDATE
                SET
                  pdf_bucket      = EXCLUDED.pdf_bucket,
                  pdf_object      = EXCLUDED.pdf_object,
                  checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256),
                  fetched_at      = now();
                """,
                (cf, pdf_bucket, pdf_object, checksum_sha256),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.visure (cf, pdf_bucket, pdf_object, checksum_sha256)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cf) DO UPDATE
                SET
                  pdf_bucket      = EXCLUDED.pdf_bucket,
                  pdf_object      = EXCLUDED.pdf_object,
                  checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256);
                """,
                (cf, pdf_bucket, pdf_object, checksum_sha256),
            )


def db_upsert_immobile(conn, visura_cf: str, imm: Immobile) -> int:
    """
    Upsert одного immobile в public.immobili і повернути його id.

    Працює через ON CONFLICT на канонічному ключі:
        (visura_cf, foglio, numero, sub)

    Важливо:
    - sub у схемі має бути NOT NULL DEFAULT '' (NULL -> ''), інакше ON CONFLICT
      не буде матчитись.
    - foglio та numero мають бути заповнені. Якщо їх нема, ми не можемо
      стабільно ідентифікувати об'єкт => кидаємо ValueError.
    """
    row: Dict[str, Any] = immobile_db_row(imm)

    # --- Нормалізація ключових полів для conflict target ---
    # sub: унікальність тримаємо на '' замість NULL
    sub = row.get("sub")
    row["sub"] = (sub or "").strip()

    foglio = (row.get("foglio") or "").strip()
    numero = (row.get("numero") or "").strip()
    row["foglio"] = foglio or None  # можна лишити як string; але якщо порожнє - None
    row["numero"] = numero or None

    if not foglio or not numero:
        # Без foglio/numero ми не можемо гарантувати правильний upsert.
        # Можна було б fallback-нути на INSERT без ON CONFLICT, але це створить сміття/дублікати.
        raise ValueError(
            f"Cannot upsert immobile without foglio+numero. "
            f"Got foglio={foglio!r}, numero={numero!r}, sub={row['sub']!r}, visura_cf={visura_cf!r}"
        )

    # --- SQL parts ---
    # Колонки, які вставляємо (visura_cf + всі поля з immobile_db_row)
    cols = ["visura_cf"] + list(row.keys())
    placeholders = ", ".join(["%s"] * len(cols))

    # На конфлікті оновлюємо ВСІ поля з row (крім created_at/updated_at, якщо вони раптом там з'являться)
    # updated_at у тебе і так оновлюється тригером BEFORE UPDATE.
    update_keys = [k for k in row.keys() if k not in ("created_at", "updated_at")]
    set_sql = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_keys])

    sql = f"""
        INSERT INTO public.immobili ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (visura_cf, foglio, numero, sub)
        DO UPDATE SET
            {set_sql}
        RETURNING id;
    """

    params = [visura_cf] + [row[k] for k in row.keys()]

    try:
        logger.debug("[DB] immobile key visura_cf=%s foglio=%r numero=%r sub=%r",
             visura_cf, row.get("foglio"), row.get("numero"), row.get("sub"))
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rec = cur.fetchone()
            if not rec:
                raise RuntimeError("UPSERT immobile did not return id (unexpected).")
            return int(rec[0])

    except Psycopg2Error as e:
        # rollback роби вище (у pipeline), але тут лог дає контекст
        logger.exception(
            "[DB] db_upsert_immobile failed visura_cf=%s foglio=%r numero=%r sub=%r: %s",
            visura_cf,
            foglio,
            numero,
            row.get("sub"),
            e,
        )
        raise


def db_prune_old_immobili_without_contracts(conn, visura_cf: str, keep_ids: List[int]) -> int:
    if not PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS:
        return 0
    if not keep_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.immobili i
            WHERE i.visura_cf=%s
              AND NOT (i.id = ANY(%s))
              AND NOT EXISTS (
                SELECT 1 FROM public.contracts c WHERE c.immobile_id = i.id
              );
            """,
            (visura_cf, keep_ids),
        )
        return cur.rowcount


def db_load_immobili(conn, visura_cf: str) -> List[Tuple[int, Immobile]]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
              id,
              table_num_immobile,
              sez_urbana,
              foglio,
              numero,
              sub,
              zona_cens,
              micro_zona,
              categoria,
              classe,
              consistenza,
              rendita,
              superficie_totale,
              superficie_escluse,
              superficie_raw,
              immobile_comune,
              immobile_comune_code,
              via_type,
              via_name,
              via_num,
              scala,
              interno,
              piano,
              indirizzo_raw,
              dati_ulteriori
            FROM public.immobili
            WHERE visura_cf=%s
            ORDER BY id;
            """,
            (visura_cf,),
        )
        rows = cur.fetchall()

    out: List[Tuple[int, Immobile]] = []
    for r in rows:
        d = dict(r)
        imm_id = int(d.pop("id"))
        out.append((imm_id, Immobile(**d)))
    return out


def db_get_latest_contract_id(conn, immobile_id: int) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT contract_id
            FROM public.contracts
            WHERE immobile_id=%s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (immobile_id,),
        )
        r = cur.fetchone()
        return str(r[0]) if r else None


def db_create_contract(conn, immobile_id: int) -> str:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO public.contracts (immobile_id) VALUES (%s) RETURNING contract_id;", (immobile_id,))
        return str(cur.fetchone()[0])


def db_update_contract_fields(conn, contract_id: str, adapter: ItemAdapter) -> None:
    contract_kind = clean_str(adapter.get("contract_kind"))
    if contract_kind:
        contract_kind = contract_kind.upper()

    start_date = parse_date(adapter.get("decorrenza_data")) or parse_date(adapter.get("contratto_data"))
    durata_anni = None
    if clean_str(adapter.get("durata_anni")):
        try:
            durata_anni = int(str(adapter.get("durata_anni")))
        except Exception:
            durata_anni = None

    arredato = to_bool_or_none(adapter.get("arredato"))
    energy_class = clean_str(adapter.get("energy_class"))
    if energy_class:
        energy_class = energy_class.upper()

    canone_contr = safe_float(adapter.get("canone_contrattuale_mensile"))

    updates: List[str] = []
    params: List[Any] = []

    def add(col: str, val: Any) -> None:
        if val is None:
            return
        updates.append(f"{col}=%s")
        params.append(val)

    add("contract_kind", contract_kind)
    add("start_date", start_date)
    add("durata_anni", durata_anni)
    add("arredato", arredato)
    add("energy_class", energy_class)
    add("canone_contrattuale_mensile", canone_contr)

    if not updates:
        return

    params.append(contract_id)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE public.contracts SET {', '.join(updates)} WHERE contract_id=%s;", params)


def db_upsert_contract_parties(conn, contract_id: str, locatore_cf: str, conduttore_cf: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.contract_parties (contract_id, role, person_cf)
            VALUES (%s, 'LOCATORE', %s)
            ON CONFLICT (contract_id, role) DO UPDATE
            SET person_cf = EXCLUDED.person_cf;
            """,
            (contract_id, locatore_cf),
        )

        if conduttore_cf:
            cur.execute(
                """
                INSERT INTO public.contract_parties (contract_id, role, person_cf)
                VALUES (%s, 'CONDUTTORE', %s)
                ON CONFLICT (contract_id, role) DO UPDATE
                SET person_cf = EXCLUDED.person_cf;
                """,
                (contract_id, conduttore_cf),
            )


def db_upsert_contract_overrides(conn, contract_id: str, adapter: ItemAdapter) -> None:
    """
    Реальна адреса об'єкта і адреса locatore з YAML -> contract_overrides.
    Оновлюємо тільки непорожні значення (щоб не затирати старі).
    """
    data = {
        "immobile_comune_override": clean_str(adapter.get("immobile_comune")),
        "immobile_via_override": clean_str(adapter.get("immobile_via")),
        "immobile_civico_override": clean_str(adapter.get("immobile_civico")),
        "immobile_piano_override": clean_str(adapter.get("immobile_piano")),
        "immobile_interno_override": clean_str(adapter.get("immobile_interno")),
        "locatore_comune_res": clean_str(adapter.get("locatore_comune_res")),
        "locatore_via": clean_str(adapter.get("locatore_via")),
        "locatore_civico": clean_str(adapter.get("locatore_civico")),
    }

    cols = [k for k, v in data.items() if v is not None]
    if not cols:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.contract_overrides (contract_id)
            VALUES (%s)
            ON CONFLICT (contract_id) DO NOTHING;
            """,
            (contract_id,),
        )

        set_sql = ", ".join([f"{c}=%s" for c in cols])
        params = [data[c] for c in cols] + [contract_id]
        cur.execute(f"UPDATE public.contract_overrides SET {set_sql} WHERE contract_id=%s;", params)


def db_apply_contract_elements(conn, contract_id: str, adapter: ItemAdapter) -> None:
    """
    З YAML:
      - якщо значення "-" -> DELETE (contract_id, grp, code)
      - якщо непорожнє -> UPSERT value
      - якщо ключа нема -> не чіпаємо
    """
    with conn.cursor() as cur:
        for k in ELEMENT_KEYS:
            raw = adapter.get(k)
            if raw is None:
                continue
            val = str(raw).strip()
            if val == "":
                continue

            grp = k[0].upper()
            code = k.upper()

            if val == "-":
                cur.execute(
                    "DELETE FROM public.contract_elements WHERE contract_id=%s AND grp=%s AND code=%s;",
                    (contract_id, grp, code),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.contract_elements (contract_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (contract_id, grp, code) DO UPDATE
                    SET value = EXCLUDED.value;
                    """,
                    (contract_id, grp, code, val),
                )


def db_load_contract_context(conn, contract_id: str) -> Dict[str, Any]:
    """
    Завантажує контекст контракту для генерації шаблону:
      - contract   (contracts)
      - overrides  (contract_overrides)
      - elements   (contract_elements -> dict a1..d13)
      - parties    (contract_parties join persons)
    """
    ctx: Dict[str, Any] = {
        "contract": {},
        "overrides": {},
        "elements": {},
        "parties": {},  # <-- ВАЖЛИВО
        "canone_calc": None,
    }

    with conn.cursor() as cur:
        # --- contract ---
        cur.execute(
            """
            SELECT contract_kind, start_date, durata_anni, arredato, energy_class, canone_contrattuale_mensile
            FROM public.contracts
            WHERE contract_id=%s;
            """,
            (contract_id,),
        )
        r = cur.fetchone()
        if r:
            contract_kind, start_date, durata_anni, arredato, energy_class, canone = r
            ctx["contract"] = {
                "contract_kind": contract_kind,
                "start_date": start_date.isoformat() if start_date else None,
                "durata_anni": durata_anni,
                "arredato": arredato,
                "energy_class": energy_class,
                "canone_contrattuale_mensile": str(canone) if canone is not None else None,
            }

        # --- overrides ---
        cur.execute(
            """
            SELECT
              immobile_comune_override,
              immobile_via_override,
              immobile_civico_override,
              immobile_piano_override,
              immobile_interno_override,
              locatore_comune_res,
              locatore_via,
              locatore_civico
            FROM public.contract_overrides
            WHERE contract_id=%s;
            """,
            (contract_id,),
        )
        o = cur.fetchone()
        if o:
            (
                ic, iv, civ, p, intr,
                lc, lv, lciv
            ) = o
            ctx["overrides"] = {
                "immobile_comune_override": ic,
                "immobile_via_override": iv,
                "immobile_civico_override": civ,
                "immobile_piano_override": p,
                "immobile_interno_override": intr,
                "locatore_comune_res": lc,
                "locatore_via": lv,
                "locatore_civico": lciv,
            }

        # --- elements ---
        cur.execute(
            "SELECT grp, code, value FROM public.contract_elements WHERE contract_id=%s;",
            (contract_id,),
        )
        elements: Dict[str, str] = {}
        for grp, code, value in cur.fetchall():
            key = normalize_element_key(str(grp or ""), str(code or ""))
            if not key:
                continue
            elements[key] = "" if value is None else str(value)
        ctx["elements"] = elements

        # --- parties (JOIN persons) ---
        cur.execute(
            """
            SELECT cp.role, p.cf, p.surname, p.name
            FROM public.contract_parties cp
            JOIN public.persons p ON p.cf = cp.person_cf
            WHERE cp.contract_id = %s;
            """,
            (contract_id,),
        )
        for role, cf, surname, name in cur.fetchall():
            ctx["parties"][role] = {"cf": cf, "surname": surname, "name": name}

        # --- latest canone_calcoli ---
        cur.execute(
            """
            SELECT inputs::text
            FROM public.canone_calcoli
            WHERE contract_id=%s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (contract_id,),
        )
        r = cur.fetchone()
        if r and r[0]:
            try:
                ctx["canone_calc"] = json.loads(r[0])  # {"canone_input": {...}, "result": {...}}
            except Exception:
                ctx["canone_calc"] = None

    logger.info(
        "[DB] ctx loaded contract_id=%s parties=%s elements=%d overrides=%s",
        contract_id,
        list((ctx.get("parties") or {}).keys()),
        len(ctx.get("elements") or {}),
        bool(ctx.get("overrides")),
    )
    return ctx


def db_insert_canone_calc(conn, contract_id: str, method: str, inputs: Dict[str, Any], result_mensile: Optional[float]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.canone_calcoli (contract_id, method, inputs, result_mensile)
            VALUES (%s, %s, %s, %s);
            """,
            (contract_id, method, psycopg2.extras.Json(inputs), result_mensile),
        )


def db_insert_attestazione_log(
    conn,
    contract_id: str,
    status: str,
    output_bucket: str,
    output_object: str,
    params_snapshot: Dict[str, Any],
    error: Optional[str],
) -> None:
    masked = mask_username(AE_USERNAME)
    sha = sha256_text(AE_USERNAME)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.attestazioni (
              contract_id,
              author_login_masked,
              author_login_sha256,
              template_version,
              output_bucket,
              output_object,
              params_snapshot,
              status,
              error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                contract_id,
                masked,
                sha,
                TEMPLATE_VERSION,
                output_bucket,
                output_object,
                psycopg2.extras.Json(params_snapshot),
                status,
                error,
            ),
        )


# ============================================================================
# Template params builder (під твою логіку: реальна адреса тільки з overrides)
# ============================================================================

def build_template_params(adapter: ItemAdapter, imm: Immobile, contract_ctx: Dict[str, Any]) -> Dict[str, str]:
    params: Dict[str, str] = {}

    overrides = contract_ctx.get("overrides") or {}
    elements = contract_ctx.get("elements") or {}
    contract = contract_ctx.get("contract") or {}
    parties = contract_ctx.get("parties") or {}

    # -------------------------
    # LOCATORE (from DB parties)
    # -------------------------
    loc = parties.get("LOCATORE") or {}

    loc_cf = clean_str(loc.get("cf") or adapter.get("locatore_cf") or adapter.get("codice_fiscale") or "")
    loc_name = loc.get("name")
    loc_surname = loc.get("surname")

    # LOCATORE
    params["{{LOCATORE_CF}}"] = loc_cf
    params["{{LOCATORE_NOME}}"] = format_person_fullname(loc_name, loc_surname)
    params["{{LOCATORE_COMUNE_RES}}"] = str(overrides.get("locatore_comune_res") or adapter.get("locatore_comune_res") or "")
    params["{{LOCATORE_VIA}}"] = str(overrides.get("locatore_via") or adapter.get("locatore_via") or "")
    params["{{LOCATORE_CIVICO}}"] = str(overrides.get("locatore_civico") or adapter.get("locatore_civico") or "")

    # IMMOBILE (real address ONLY overrides)
    params["{{IMMOBILE_COMUNE}}"] = str(overrides.get("immobile_comune_override") or "")
    params["{{IMMOBILE_VIA}}"] = str(overrides.get("immobile_via_override") or "")
    params["{{IMMOBILE_CIVICO}}"] = str(overrides.get("immobile_civico_override") or "")
    params["{{IMMOBILE_PIANO}}"] = str(overrides.get("immobile_piano_override") or "")
    params["{{IMMOBILE_INTERNO}}"] = str(overrides.get("immobile_interno_override") or "")

    # Dati catastali (parsed)
    params["{{FOGLIO}}"] = str(getattr(imm, "foglio", "") or "")
    params["{{NUMERO}}"] = str(getattr(imm, "numero", "") or "")
    params["{{SUB}}"] = str(getattr(imm, "sub", "") or "")
    params["{{RENDITA}}"] = str(getattr(imm, "rendita", "") or "")
    params["{{SUPERFICIE_TOTALE}}"] = str(getattr(imm, "superficie_totale", "") or "")
    params["{{CATEGORIA}}"] = str(getattr(imm, "categoria", "") or "")

    # APP row
    params["{{APP_FOGL}}"] = params["{{FOGLIO}}"]
    params["{{APP_PART}}"] = params["{{NUMERO}}"]
    params["{{APP_SUB}}"] = params["{{SUB}}"]
    params["{{APP_REND}}"] = params["{{RENDITA}}"]
    params["{{APP_SCAT}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{APP_SRIP}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{APP_CAT}}"] = params["{{CATEGORIA}}"]

    # TOTAL row
    params["{{TOT_SCAT}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{TOT_SRIP}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{TOT_CAT}}"] = params["{{CATEGORIA}}"]

    # GAR/PST placeholders
    for prefix in ("GAR", "PST"):
        for suffix in ("FOGL", "PART", "SUB", "REND", "SCAT", "SRIP", "CAT"):
            params[f"{{{{{prefix}_{suffix}}}}}"] = ""

    # Contract / registration data
    params["{{CONTRATTO_DATA}}"] = str(adapter.get("contratto_data") or contract.get("start_date") or "")
    params["{{DECORRENZA_DATA}}"] = str(adapter.get("decorrenza_data") or contract.get("start_date") or "")
    params["{{REGISTRAZIONE_DATA}}"] = str(adapter.get("registrazione_data") or "")
    params["{{REGISTRAZIONE_NUM}}"] = str(adapter.get("registrazione_num") or "")
    params["{{AGENZIA_ENTRATE_SEDE}}"] = str(adapter.get("agenzia_entrate_sede") or "")

    # Tenant
    params["{{CONDUTTORE_NOME}}"] = str(adapter.get("conduttore_nome") or "")
    params["{{CONDUTTORE_CF}}"] = str(adapter.get("conduttore_cf") or "")
    params["{{CONDUTTORE_COMUNE}}"] = str(adapter.get("conduttore_comune") or "")
    params["{{CONDUTTORE_VIA}}"] = str(adapter.get("conduttore_via") or "")

    # A/B/C/D elements (з БД, не напряму з YAML)
    for key in ELEMENT_KEYS:
        v = str(elements.get(key, "") or "")
        params[f"{{{{{key}}}}}"] = v
        params[f"{{{{{key.upper()}}}}}"] = v

    # counts
    def cnt(keys: List[str]) -> int:
        return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

    params["{{A_CNT}}"] = str(cnt(["a1", "a2"]))
    params["{{B_CNT}}"] = str(cnt([f"b{i}" for i in range(1, 6)]))
    params["{{C_CNT}}"] = str(cnt([f"c{i}" for i in range(1, 8)]))
    params["{{D_CNT}}"] = str(cnt([f"d{i}" for i in range(1, 14)]))

    # CANONE placeholders default (покриваємо шаблон навіть якщо calc не зробили)
    for ph in [
        "CAN_ZONA", "CAN_SUBFASCIA",
        "CAN_MQ", "CAN_MQ_ANNUO", "CAN_TOTALE_ANNUO",
        "CAN_ARREDATO", "CAN_CLASSE_A", "CAN_CLASSE_B",
        "CAN_ENERGY", "CAN_DURATA", "CAN_TRANSITORIO", "CAN_STUDENTI",
        "CAN_ANNUO_VAR_MIN", "CAN_ANNUO_VAR_MAX",
        "CAN_MENSILE_VAR_MIN", "CAN_MENSILE_VAR_MAX",
        "CAN_MENSILE",
    ]:
        params[f"{{{{{ph}}}}}"] = ""

    def _fmt_num(x, decimals=2) -> str:
        if x is None:
            return ""
        try:
            v = float(x)
        except Exception:
            return str(x)
        # без фанатизму: 2 знаки після коми
        return f"{v:.{decimals}f}"

    def _pct_str(p: float) -> str:
        # p = 0.15 -> "+15%"
        sign = "+" if p > 0 else ""
        return f"{sign}{int(round(p*100))}%"

    can = contract_ctx.get("canone_calc") or {}
    cin = can.get("canone_input") or {}
    res = can.get("result") or {}

    if res:
        zona = res.get("zona")
        subfascia = res.get("subfascia")
        base_euro_mq = res.get("base_euro_mq")

        mq = cin.get("superficie_catastale")
        if mq is None:
            mq = getattr(imm, "superficie_totale", None)

        params["{{CAN_ZONA}}"] = "" if zona is None else str(zona)
        params["{{CAN_SUBFASCIA}}"] = "" if subfascia is None else str(subfascia)
        params["{{CAN_MQ}}"] = _fmt_num(mq, 0)  # м² зазвичай без копійок
        params["{{CAN_MQ_ANNUO}}"] = _fmt_num(base_euro_mq, 2)

        # базовий annuo (з твого result)
        params["{{CAN_TOTALE_ANNUO}}"] = _fmt_num(res.get("canone_base_annuo") or res.get("canone_finale_annuo"), 2)

        # ---- Відсоткові “надбавки/знижки” для рядків таблиці ----
        delta_pct = 0.0

        arredato = bool(cin.get("arredato"))
        if arredato:
            params["{{CAN_ARREDATO}}"] = _pct_str(0.15)
            delta_pct += 0.15

        energy = (cin.get("energy_class") or "").strip().upper()
        if energy == "A":
            params["{{CAN_CLASSE_A}}"] = _pct_str(0.08)
            delta_pct += 0.08
        elif energy == "B":
            params["{{CAN_CLASSE_B}}"] = _pct_str(0.04)
            delta_pct += 0.04
        elif energy == "E":
            params["{{CAN_ENERGY}}"] = _pct_str(-0.02)
            delta_pct += -0.02
        elif energy == "F":
            params["{{CAN_ENERGY}}"] = _pct_str(-0.04)
            delta_pct += -0.04
        elif energy == "G":
            params["{{CAN_ENERGY}}"] = _pct_str(-0.06)
            delta_pct += -0.06

        durata = cin.get("durata_anni")
        try:
            durata = int(durata) if durata is not None else None
        except Exception:
            durata = None

        if durata == 4:
            params["{{CAN_DURATA}}"] = _pct_str(0.05); delta_pct += 0.05
        elif durata == 5:
            params["{{CAN_DURATA}}"] = _pct_str(0.08); delta_pct += 0.08
        elif durata is not None and durata >= 6:
            params["{{CAN_DURATA}}"] = _pct_str(0.10); delta_pct += 0.10

        raw_kind = str(cin.get("contract_kind") or "")
        kind = raw_kind.split(".")[-1].upper()  # "ContractKind.CONCORDATO" -> "CONCORDATO"
        if kind == "TRANSITORIO":
            params["{{CAN_TRANSITORIO}}"] = _pct_str(0.15); delta_pct += 0.15
        elif kind == "STUDENTI":
            params["{{CAN_STUDENTI}}"] = _pct_str(0.20); delta_pct += 0.20

        # ---- Мін/макс після відсотків ----
        # ВАЖЛИВО: для цього треба знати min/max €/mq діапазону.
        # Якщо ти їх не зберіг у result — буде fallback на base_euro_mq (тобто min=max).
        min_eur = res.get("base_min_euro_mq", base_euro_mq)
        max_eur = res.get("base_max_euro_mq", base_euro_mq)

        try:
            mq_f = float(mq or 0.0)
            min_annuo = float(min_eur or 0.0) * mq_f
            max_annuo = float(max_eur or 0.0) * mq_f
            min_annuo_v = min_annuo * (1.0 + delta_pct)
            max_annuo_v = max_annuo * (1.0 + delta_pct)

            params["{{CAN_ANNUO_VAR_MIN}}"] = _fmt_num(min_annuo_v, 2)
            params["{{CAN_ANNUO_VAR_MAX}}"] = _fmt_num(max_annuo_v, 2)
            params["{{CAN_MENSILE_VAR_MIN}}"] = _fmt_num(min_annuo_v / 12.0, 2)
            params["{{CAN_MENSILE_VAR_MAX}}"] = _fmt_num(max_annuo_v / 12.0, 2)
        except Exception:
            pass

    # ---- Узгоджений між сторонами (рядок 22 таблиці) ----
    agreed = adapter.get("canone_contrattuale_mensile") or contract.get("canone_contrattuale_mensile")
    if agreed is not None:
        params["{{CAN_MENSILE}}"] = _fmt_num(agreed, 2)

    return params


# ============================================================================
# Pipeline
# ============================================================================

class UppiPipeline:
    """
    Новий пайплайн під нормалізовану схему БД:
      persons -> visure -> immobili -> contracts (+ parties/overrides/elements) -> canone_calcoli -> attestazioni
    """

    def __init__(self):
        self.storage = ObjectStorage()

        self.template_path = (
            Path(__file__).resolve().parents[1]
            / "attestazione_template"
            / "template_attestazione_pescara.docx"
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        locatore_cf = clean_str(adapter.get("locatore_cf") or adapter.get("codice_fiscale"))
        if not locatore_cf:
            spider.logger.error("[PIPELINE] Item без locatore_cf/codice_fiscale: %r", item)
            return item

        cond_cf = clean_str(adapter.get("conduttore_cf"))

        # Візура: звідки прийшла
        visura_source = clean_str(adapter.get("visura_source")) or "unknown"
        visura_downloaded = bool(adapter.get("visura_downloaded"))
        force_update_visura = bool(adapter.get("force_update_visura"))

        spider.logger.info(
            "[PIPELINE] CF=%s source=%s downloaded=%s force_update_visura=%s",
            locatore_cf, visura_source, visura_downloaded, force_update_visura
        )

        conn = get_pg_connection()
        try:
            # --- 1) persons upsert ---
            # locatore name/surname беремо з парсера (якщо буде), але зараз вставимо що є
            db_upsert_person(
                conn,
                locatore_cf,
                surname=clean_str(adapter.get("locatore_surname")),
                name=clean_str(adapter.get("locatore_name")),
            )
            if cond_cf:
                db_upsert_person(
                    conn,
                    cond_cf,
                    surname=clean_str(adapter.get("conduttore_surname")),
                    name=clean_str(adapter.get("conduttore_name")),
                )

            # --- 2) PDF: знайти локально, якщо в цьому запуску скачали ---
            pdf_path = None
            fetched_now = False
            checksum = None
            pdf_to_delete: Path | None = None

            if visura_source == "sister" and visura_downloaded:
                pdf_path = find_local_visura_pdf(locatore_cf, adapter)

                if pdf_path is None:
                    raise FileNotFoundError(f"Visura PDF not found for CF={locatore_cf} (downloaded=True)")

                checksum = sha256_file(pdf_path)

                # upload to S3 (MinIO/R2)
                bucket = self.storage.cfg.visure_bucket
                obj_name = self.storage.visura_object_name(locatore_cf)
                self.storage.upload_file(bucket, obj_name, pdf_path, content_type="application/pdf")
                fetched_now = True

                # upsert visura in DB
                db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum, fetched_now=True)

                pdf_to_delete = pdf_path

            else:
                # Якщо не sister — не чіпаємо fetched_at, але тримаємо canonical bucket/object
                bucket = self.storage.cfg.visure_bucket
                obj_name = self.storage.visura_object_name(locatore_cf)
                db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum_sha256=None, fetched_now=False)

                # Якщо файл є локально, а ми в cache-режимі — теж можемо прибрати (за бажанням)
                maybe_local = find_local_visura_pdf(locatore_cf, adapter)
                if maybe_local is not None:
                    pdf_to_delete = maybe_local

            # --- 3) Parse PDF -> upsert immobili ---
            keep_ids: List[int] = []
            if fetched_now and pdf_path is not None:
                parser = VisuraParser()
                parsed_dicts = parser.parse(pdf_path)

                if not parsed_dicts:
                    spider.logger.warning("[PIPELINE] No immobili parsed from PDF CF=%s", locatore_cf)
                else:
                    # також збагачуємо persons з PDF, якщо є locatore
                    loc_surname = clean_str(parsed_dicts[0].get("locatore_surname"))
                    loc_name = clean_str(parsed_dicts[0].get("locatore_name"))
                    if loc_surname or loc_name:
                        db_upsert_person(conn, locatore_cf, surname=loc_surname, name=loc_name)

                    for d in parsed_dicts:
                        logger.info(
                            "[PIPELINE] PARSED immobile: foglio=%r numero=%r sub=%r categoria=%r sup=%r indirizzo=%r",
                            d.get("foglio"), d.get("numero"), d.get("sub"), d.get("categoria"),
                            d.get("superficie_totale"), d.get("via_name") or d.get("indirizzo_raw"),
                        )
                        imm = immobile_from_parsed_dict(d)

                        logger.info(
                            "[PIPELINE] IMM OBJ: foglio=%r numero=%r sub=%r categoria=%r sup=%r",
                            imm.foglio, imm.numero, imm.sub, imm.categoria, imm.superficie_totale
                        )

                        imm_id = db_upsert_immobile(conn, locatore_cf, imm)

                        logger.info("[PIPELINE] UPSERT immobile -> id=%r", imm_id)

                        keep_ids.append(imm_id)

                    logger.info("[PIPELINE] keep_ids=%s (count=%d)", keep_ids, len(keep_ids))
                    
                    deleted = db_prune_old_immobili_without_contracts(conn, locatore_cf, keep_ids)
                    if deleted:
                        spider.logger.info("[DB] Pruned %d old immobili (no contracts) CF=%s", deleted, locatore_cf)

            # --- 4) Load immobili from DB (canonical) ---
            immobili_db = db_load_immobili(conn, locatore_cf)
            if not immobili_db:
                spider.logger.error("[PIPELINE] No immobili in DB for CF=%s", locatore_cf)
                conn.commit()
                return item

            # --- 5) Filter by YAML to select immobile(s) ---
            selected = filter_immobiles_by_yaml(immobili_db, adapter)
            if not selected:
                spider.logger.warning("[PIPELINE] No immobili matched YAML filter CF=%s", locatore_cf)
                conn.commit()
                return item

            # --- 6) For each selected immobile: contract + overrides + elements + generate attestazione ---
            for immobile_id, imm in selected:
                force_new_contract = bool(adapter.get("force_new_contract") or False)
                contract_id = None if force_new_contract else db_get_latest_contract_id(conn, immobile_id)
                if not contract_id:
                    contract_id = db_create_contract(conn, immobile_id)

                db_update_contract_fields(conn, contract_id, adapter)
                db_upsert_contract_parties(conn, contract_id, locatore_cf, cond_cf)
                db_upsert_contract_overrides(conn, contract_id, adapter)
                db_apply_contract_elements(conn, contract_id, adapter)

                contract_ctx = db_load_contract_context(conn, contract_id)

                # --- 7) Canone calc (optional) ---
                canone_snapshot: Dict[str, Any] = {}
                canone_result_snapshot: Optional[Dict[str, Any]] = None

                if compute_base_canone is not None and CanoneInput is not None and ContractKind is not None:
                    try:
                        elements = contract_ctx.get("elements") or {}
                        def cnt(keys: List[str]) -> int:
                            return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

                        raw_kind = clean_str(adapter.get("contract_kind")) or "CONCORDATO"
                        raw_kind = raw_kind.upper()
                        try:
                            kind_enum = ContractKind[raw_kind]
                        except Exception:
                            kind_enum = ContractKind.CONCORDATO

                        sup = getattr(imm, "superficie_totale", None)
                        if sup is None:
                            sup = safe_float(adapter.get("superficie_totale"))

                        can_in = CanoneInput(
                            superficie_catastale=float(sup or 0.0),
                            micro_zona=clean_str(getattr(imm, "micro_zona", None)),
                            foglio=clean_str(getattr(imm, "foglio", None)),
                            categoria_catasto=clean_str(getattr(imm, "categoria", None)),
                            classe_catasto=clean_str(getattr(imm, "classe", None)),
                            count_a=cnt(["a1", "a2"]),
                            count_b=cnt([f"b{i}" for i in range(1, 6)]),
                            count_c=cnt([f"c{i}" for i in range(1, 8)]),
                            count_d=cnt([f"d{i}" for i in range(1, 14)]),
                            arredato=bool(to_bool_or_none(adapter.get("arredato"))),
                            energy_class=clean_str(adapter.get("energy_class")),
                            contract_kind=kind_enum,
                            durata_anni=int(adapter.get("durata_anni") or 3),
                        )

                        canone_snapshot = {
                            "superficie_catastale": can_in.superficie_catastale,
                            "micro_zona": can_in.micro_zona,
                            "foglio": can_in.foglio,
                            "categoria_catasto": can_in.categoria_catasto,
                            "classe_catasto": can_in.classe_catasto,
                            "count_a": can_in.count_a,
                            "count_b": can_in.count_b,
                            "count_c": can_in.count_c,
                            "count_d": can_in.count_d,
                            "arredato": can_in.arredato,
                            "energy_class": can_in.energy_class,
                            "contract_kind": str(can_in.contract_kind),
                            "durata_anni": can_in.durata_anni,
                        }

                        can_res = compute_base_canone(can_in)
                        canone_result_snapshot = {
                            "zona": getattr(can_res, "zona", None),
                            "subfascia": getattr(can_res, "subfascia", None),
                            "base_min_euro_mq": getattr(can_res, "base_min_euro_mq", None),
                            "base_max_euro_mq": getattr(can_res, "base_max_euro_mq", None),
                            "base_euro_mq": getattr(can_res, "base_euro_mq", None),
                            "canone_base_annuo": getattr(can_res, "canone_base_annuo", None),
                            "canone_finale_annuo": getattr(can_res, "canone_finale_annuo", None),
                            "canone_finale_mensile": getattr(can_res, "canone_finale_mensile", None),
                        }

                        # write canone_calcoli
                        result_m = None
                        try:
                            result_m = float(getattr(can_res, "canone_finale_mensile", None))
                        except Exception:
                            result_m = None

                        db_insert_canone_calc(
                            conn,
                            contract_id=contract_id,
                            method="pescara2018_base",
                            inputs={"canone_input": canone_snapshot, "result": canone_result_snapshot},
                            result_mensile=result_m,
                        )

                        # Update contract_ctx with canone_calc
                        contract_ctx = db_load_contract_context(conn, contract_id)

                    except CanoneCalculationError as e:
                        spider.logger.warning("[CANONE] Logical error contract=%s immobile_id=%s: %s", contract_id, immobile_id, e)
                    except Exception as e:
                        spider.logger.exception("[CANONE] Unexpected error contract=%s immobile_id=%s: %s", contract_id, immobile_id, e)

                # --- 8) Build template params + generate DOCX ---
                logger.info("[PIPELINE] contract_ctx.parties=%r", contract_ctx.get("parties"))
                params = build_template_params(adapter, imm, contract_ctx)

                output_path = get_attestazione_path(locatore_cf, contract_id, imm)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Генерація DOCX
                try:
                    fill_attestazione_template(
                        template_path=str(self.template_path),
                        output_folder=str(output_path.parent),
                        filename=output_path.name,
                        params=params,
                        underscored=underscored,
                    )
                except Exception as e:
                    spider.logger.exception("[PIPELINE] DOCX generation failed contract=%s: %s", contract_id, e)
                    # лог в БД
                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="failed",
                        output_bucket=self.storage.cfg.attestazioni_bucket,
                        output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                        params_snapshot={"locatore_cf": locatore_cf, "contract_id": contract_id, "error_stage": "docx_generation"},
                        error=str(e)[:5000],
                    )
                    continue

                # Upload DOCX
                try:
                    out_bucket = self.storage.cfg.attestazioni_bucket
                    out_obj = self.storage.attestazione_object_name(locatore_cf, contract_id)
                    self.storage.upload_file(
                        out_bucket,
                        out_obj,
                        output_path,
                        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )

                    params_snapshot = {
                        "locatore_cf": locatore_cf,
                        "immobile_id": immobile_id,
                        "contract_id": contract_id,
                        "yaml_item": dict(adapter.asdict()),
                        "immobile_parsed": immobile_db_row(imm),
                        "contract_ctx": contract_ctx,
                        "template_version": TEMPLATE_VERSION,
                        "author_login_masked": mask_username(AE_USERNAME),
                        "canone_input": canone_snapshot,
                        "canone_result": canone_result_snapshot,
                        "output": {"bucket": out_bucket, "object": out_obj},
                    }

                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="generated",
                        output_bucket=out_bucket,
                        output_object=out_obj,
                        params_snapshot=params_snapshot,
                        error=None,
                    )

                    spider.logger.info("[PIPELINE] Attestazione OK contract=%s -> %s/%s", contract_id, out_bucket, out_obj)

                except Exception as e:
                    spider.logger.exception("[PIPELINE] Upload/log attestazione failed contract=%s: %s", contract_id, e)
                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="failed",
                        output_bucket=self.storage.cfg.attestazioni_bucket,
                        output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                        params_snapshot={"locatore_cf": locatore_cf, "contract_id": contract_id, "error_stage": "upload_or_log"},
                        error=str(e)[:5000],
                    )

            conn.commit()
            if DELETE_LOCAL_VISURA_AFTER_UPLOAD and pdf_to_delete is not None:
                safe_unlink(pdf_to_delete)
            return item

        except psycopg2.Error as e:
            spider.logger.exception("[PIPELINE] DB error CF=%s: %s", locatore_cf, e)
            try:
                conn.rollback()
            except Exception:
                pass
            return item

        except Exception as e:
            spider.logger.exception("[PIPELINE] Fatal error CF=%s: %s", locatore_cf, e)
            try:
                conn.rollback()
            except Exception:
                pass
            return item

        finally:
            conn.close()
