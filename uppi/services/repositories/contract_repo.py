"""Тонкий repository-модуль для контрактів і joined context read paths."""

from __future__ import annotations

import json
from typing import Any, Dict

import psycopg2.extras
from itemadapter import ItemAdapter

from uppi.services.policies.contract_patch_policy import build_contract_patch_decision
from uppi.services.repositories.common import logger
from uppi.utils.db_utils.key_normalize import normalize_element_key


def db_upsert_contract(conn, immobile_id: int, adapter: ItemAdapter) -> str:
    """Створює або оновлює контракт, зберігаючи поточну patch/fallback semantics."""
    old_contract = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id,
                   contract_kind, durata_anni, arredato_pct, istat_rate, ignore_surcharges,
                   start_date, decorrenza_data, registrazione_data,
                   registrazione_num, agenzia_entrate_sede, canone_contrattuale_mensile,
                   conduttore_cf
            FROM public.contracts
            WHERE immobile_id = %s
            ORDER BY created_at DESC LIMIT 1
            """,
            (immobile_id,),
        )
        row = cur.fetchone()
        if row:
            old_contract = dict(row)

    contract_id = old_contract.get("id")
    decision = build_contract_patch_decision(immobile_id, adapter, old_contract)
    params = decision.params
    if decision.kind_was_unknown:
        logger.warning(f"[DB] Unknown contract kind '{adapter.get('contract_kind')}', defaulting to CONCORDATO")

    with conn.cursor() as cur:
        if not contract_id:
            sql = """
            INSERT INTO public.contracts (
                immobile_id, conduttore_cf, contract_kind, start_date, durata_anni,
                decorrenza_data, registrazione_data, registrazione_num, agenzia_entrate_sede,
                canone_contrattuale_mensile, istat_rate, arredato_pct, ignore_surcharges
            ) VALUES (
                %(immobile_id)s, %(cond_cf)s, %(kind)s, %(start_date)s, %(durata)s,
                %(decorrenza)s, %(reg_data)s, %(reg_num)s, %(ae_sede)s,
                %(canone)s, %(istat)s, %(arredato)s, %(ignore_surcharges)s
            ) RETURNING id;
            """
            cur.execute(sql, params)
            contract_id = cur.fetchone()[0]
        else:
            sql = """
            UPDATE public.contracts SET
                conduttore_cf = %(cond_cf)s,
                contract_kind = %(kind)s,
                start_date    = %(start_date)s,
                durata_anni   = %(durata)s,
                decorrenza_data      = %(decorrenza)s,
                registrazione_data   = %(reg_data)s,
                registrazione_num    = %(reg_num)s,
                agenzia_entrate_sede = %(ae_sede)s,
                canone_contrattuale_mensile = %(canone)s,
                istat_rate    = %(istat)s,
                arredato_pct  = %(arredato)s,
                ignore_surcharges = %(ignore_surcharges)s,
                updated_at = now()
            WHERE id = %(id)s;
            """
            cur.execute(sql, {**params, "id": contract_id})

        return str(contract_id)


def db_load_contract_context(conn, contract_id: str) -> Dict[str, Any]:
    """Завантажує joined context контракту в current shape для генератора й процесора."""
    ctx: Dict[str, Any] = {
        "contract": {},
        "overrides": {},
        "elements": {},
        "parties": {},
        "canone_calc": None,
        "immobile": {},
    }

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql_contract = """
        SELECT
            c.*,
            p_own.cf as loc_cf, p_own.name as loc_name, p_own.surname as loc_surname,
            a_own.comune as loc_comune, a_own.via_full as loc_via, a_own.civico as loc_civico,
            p_cond.cf as cond_cf, p_cond.name as cond_name, p_cond.surname as cond_surname,
            a_cond.comune as cond_comune, a_cond.via_full as cond_via,
            COALESCE(ra.comune, va.comune) as imm_comune,
            COALESCE(ra.via_full, va.via_full) as imm_via,
            COALESCE(ra.civico, va.civico) as imm_civico,
            COALESCE(ra.piano, va.piano) as imm_piano,
            COALESCE(ra.interno, va.interno) as imm_interno,
            i.energy_class as imm_energy_class
        FROM public.contracts c
        JOIN public.immobili i ON c.immobile_id = i.id
        LEFT JOIN public.persons p_own ON i.owner_cf = p_own.cf
        LEFT JOIN public.addresses a_own ON p_own.residence_address_id = a_own.id
        LEFT JOIN public.persons p_cond ON c.conduttore_cf = p_cond.cf
        LEFT JOIN public.addresses a_cond ON p_cond.residence_address_id = a_cond.id
        LEFT JOIN public.addresses va ON i.visura_address_id = va.id
        LEFT JOIN public.addresses ra ON i.real_address_id = ra.id
        WHERE c.id = %s;
        """
        cur.execute(sql_contract, (contract_id,))
        row = cur.fetchone()

        if row:
            ctx["contract"] = dict(row)
            ctx["immobile"] = {
                "comune": row["imm_comune"],
                "via": row["imm_via"],
                "civico": row["imm_civico"],
                "piano": row["imm_piano"],
                "interno": row["imm_interno"],
                "energy_class": row["imm_energy_class"],
            }
            ctx["overrides"] = {
                "locatore_comune_res": row["loc_comune"],
                "locatore_via": row["loc_via"],
                "locatore_civico": row["loc_civico"],
            }
            ctx["parties"]["LOCATORE"] = {
                "cf": row["loc_cf"],
                "name": row["loc_name"],
                "surname": row["loc_surname"],
            }
            if row["cond_cf"]:
                ctx["parties"]["CONDUTTORE"] = {
                    "cf": row["cond_cf"],
                    "name": row["cond_name"],
                    "surname": row["cond_surname"],
                    "comune": row["cond_comune"],
                    "via": row["cond_via"],
                }

            immobile_id = row["immobile_id"]
            cur.execute(
                "SELECT grp, code, value FROM public.immobile_elements WHERE immobile_id=%s;",
                (immobile_id,),
            )
            elements: Dict[str, str] = {}
            for grp, code, value in cur.fetchall():
                key = normalize_element_key(str(grp or ""), str(code or ""))
                if not key:
                    continue
                elements[key] = "" if value is None else str(value)
            ctx["elements"] = elements

        cur.execute(
            """
            SELECT inputs::text
            FROM public.canone_calcoli
            WHERE contract_id=%s
            ORDER BY calculated_at DESC
            LIMIT 1;
            """,
            (contract_id,),
        )
        result = cur.fetchone()
        if result and result[0]:
            try:
                ctx["canone_calc"] = json.loads(result[0])
            except Exception:
                ctx["canone_calc"] = None

    return ctx
