"""Тонкий repository-модуль для `immobili` і пов'язаних елементів."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from itemadapter import ItemAdapter

from uppi.domain.immobile import Immobile
from uppi.services.policies.immobile_patch_policy import (
    build_real_address_update_plan,
    iter_apply_element_mutations,
    iter_upsert_element_mutations,
)
from uppi.services.repositories.common import (
    ELEMENT_KEYS,
    immobile_db_row,
    logger,
)


def db_upsert_immobile(
    conn,
    owner_cf: str,
    imm: Immobile,
    visura_addr_id: Optional[int] = None,
    source_visura_id: Optional[int] = None,
) -> int:
    """Вставляє або оновлює `immobili`, зберігаючи поточну SQL-semantics."""
    row = immobile_db_row(imm)

    foglio = row.get("foglio")
    numero = row.get("numero")
    if not foglio or not numero:
        raise ValueError(
            f"Cannot upsert immobile without foglio+numero. "
            f"Got foglio={foglio!r}, numero={numero!r}, owner_cf={owner_cf!r}"
        )

    sub = row.get("sub") or ""

    sql = """
    INSERT INTO public.immobili (
        owner_cf, source_visura_id, visura_address_id,
        sez_urbana, foglio, numero, sub,
        zona_cens, micro_zona, categoria, classe, consistenza, rendita,
        superficie_totale, superficie_escluse, superficie_raw
    )
    VALUES (
        %(owner_cf)s, %(source_visura_id)s, %(visura_addr_id)s,
        %(sez_urbana)s, %(foglio)s, %(numero)s, %(sub)s,
        %(zona_cens)s, %(micro_zona)s, %(categoria)s, %(classe)s, %(consistenza)s, %(rendita)s,
        %(superficie_totale)s, %(superficie_escluse)s, %(superficie_raw)s
    )
    ON CONFLICT (owner_cf, foglio, numero, sub) DO UPDATE
    SET
        source_visura_id   = COALESCE(EXCLUDED.source_visura_id, immobili.source_visura_id),
        visura_address_id  = COALESCE(EXCLUDED.visura_address_id, immobili.visura_address_id),
        zona_cens          = COALESCE(EXCLUDED.zona_cens, immobili.zona_cens),
        micro_zona         = COALESCE(EXCLUDED.micro_zona, immobili.micro_zona),
        categoria          = COALESCE(EXCLUDED.categoria, immobili.categoria),
        classe             = COALESCE(EXCLUDED.classe, immobili.classe),
        consistenza        = COALESCE(EXCLUDED.consistenza, immobili.consistenza),
        rendita            = COALESCE(EXCLUDED.rendita, immobili.rendita),
        superficie_totale  = COALESCE(EXCLUDED.superficie_totale, immobili.superficie_totale),
        superficie_escluse = COALESCE(EXCLUDED.superficie_escluse, immobili.superficie_escluse),
        superficie_raw     = COALESCE(EXCLUDED.superficie_raw, immobili.superficie_raw),
        updated_at         = now()
    RETURNING id;
    """

    params = {
        "owner_cf": owner_cf,
        "source_visura_id": source_visura_id,
        "visura_addr_id": visura_addr_id,
        "sez_urbana": row.get("sez_urbana"),
        "foglio": foglio,
        "numero": numero,
        "sub": sub,
        "zona_cens": row.get("zona_cens"),
        "micro_zona": row.get("micro_zona"),
        "categoria": row.get("categoria"),
        "classe": row.get("classe"),
        "consistenza": row.get("consistenza"),
        "rendita": row.get("rendita"),
        "superficie_totale": row.get("superficie_totale"),
        "superficie_escluse": row.get("superficie_escluse"),
        "superficie_raw": row.get("superficie_raw"),
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    except psycopg2.Error as exc:
        logger.error(f"[DB] Immobile upsert failed for CF={owner_cf} F={foglio} N={numero} S={sub}: {exc}")
        raise


def db_upsert_immobile_elements(conn, immobile_id: int, adapter: ItemAdapter):
    """Оновлює елементи A-D через current smart patch semantics на рівні SQL."""
    with conn.cursor() as cur:
        for mutation in iter_upsert_element_mutations(adapter):
            if mutation.action == "delete":
                cur.execute(
                    """
                    DELETE FROM public.immobile_elements
                    WHERE immobile_id = %s AND grp = %s AND code = %s
                    """,
                    (immobile_id, mutation.grp, mutation.code),
                )
            elif mutation.action == "upsert":
                cur.execute(
                    """
                    INSERT INTO public.immobile_elements (immobile_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (immobile_id, grp, code)
                    DO UPDATE SET value = EXCLUDED.value
                    """,
                    (immobile_id, mutation.grp, mutation.code, mutation.value),
                )


def db_update_immobile_real_address(
    conn,
    immobile_id: int,
    real_address_id: Optional[int] = None,
    energy_class: Optional[str] = None,
) -> None:
    """Оновлює real-address і `energy_class`, не змінюючи current patch semantics."""
    updates, params = build_real_address_update_plan(
        real_address_id=real_address_id,
        energy_class=energy_class,
    )

    if not updates:
        return

    sql = f"""
    UPDATE public.immobili
    SET {', '.join(updates)}, updated_at = now()
    WHERE id = %s
    """
    params.append(immobile_id)

    with conn.cursor() as cur:
        try:
            cur.execute(sql, params)
        except psycopg2.Error as exc:
            logger.error(f"[DB] Помилка оновлення immobili: {exc}")
            conn.rollback()
            raise


def db_load_immobili(conn, owner_cf: str) -> List[Tuple[int, Immobile]]:
    """Завантажує `immobili` з joined адресами у shape, сумісному з пайплайном."""
    sql = """
    SELECT
      i.id,
      i.sez_urbana, i.foglio, i.numero, i.sub,
      i.zona_cens, i.micro_zona, i.categoria, i.classe, i.consistenza, i.rendita,
      i.superficie_totale, i.superficie_escluse, i.superficie_raw,
      i.energy_class,
      va.comune as v_comune, va.via_full as v_via, va.civico as v_civico,
      va.piano as v_piano, va.interno as v_interno, va.scala as v_scala,
      ra.comune as r_comune, ra.via_full as r_via, ra.civico as r_civico,
      ra.piano as r_piano, ra.interno as r_interno
    FROM public.immobili i
    LEFT JOIN public.addresses va ON i.visura_address_id = va.id
    LEFT JOIN public.addresses ra ON i.real_address_id = ra.id
    WHERE i.owner_cf = %s
    ORDER BY i.foglio, i.numero, i.sub;
    """

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (owner_cf,))
        rows = cur.fetchall()

    out: List[Tuple[int, Immobile]] = []
    for row in rows:
        d = dict(row)
        imm_id = int(d.pop("id"))
        imm_obj = Immobile(
            sez_urbana=d["sez_urbana"],
            foglio=d["foglio"],
            numero=d["numero"],
            sub=d["sub"],
            zona_cens=d["zona_cens"],
            micro_zona=d["micro_zona"],
            categoria=d["categoria"],
            classe=d["classe"],
            consistenza=d["consistenza"],
            rendita=d["rendita"],
            superficie_totale=float(d["superficie_totale"]) if d["superficie_totale"] else None,
            superficie_escluse=float(d["superficie_escluse"]) if d["superficie_escluse"] else None,
            superficie_raw=d["superficie_raw"],
            energy_class=d["energy_class"],
            immobile_comune=d["v_comune"],
            via_name=d["v_via"],
            via_num=d["v_civico"],
            piano=d["v_piano"],
            interno=d["v_interno"],
            scala=d["v_scala"],
            immobile_comune_override=d["r_comune"],
            immobile_via_override=d["r_via"],
            immobile_civico_override=d["r_civico"],
            immobile_piano_override=d["r_piano"],
            immobile_interno_override=d["r_interno"],
        )
        out.append((imm_id, imm_obj))

    return out


def db_prune_old_immobili_without_contracts(conn, owner_cf: str, keep_ids: List[int], enabled: bool) -> int:
    """Видаляє старі `immobili` без контрактів, якщо pruning увімкнено."""
    if not enabled or not keep_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.immobili i
            WHERE i.owner_cf=%s
              AND NOT (i.id = ANY(%s))
              AND NOT EXISTS (
                SELECT 1 FROM public.contracts c WHERE c.immobile_id = i.id
              );
            """,
            (owner_cf, keep_ids),
        )
        return cur.rowcount


def db_apply_immobile_elements(conn, immobile_id: int, adapter: ItemAdapter) -> None:
    """Оновлює елементи `immobile` через current apply-semantics."""
    with conn.cursor() as cur:
        for mutation in iter_apply_element_mutations(adapter, ELEMENT_KEYS):
            if mutation.action == "delete":
                cur.execute(
                    "DELETE FROM public.immobile_elements WHERE immobile_id=%s AND grp=%s AND code=%s;",
                    (immobile_id, mutation.grp, mutation.code),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.immobile_elements (immobile_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (immobile_id, grp, code) DO UPDATE
                    SET value = EXCLUDED.value;
                    """,
                    (immobile_id, mutation.grp, mutation.code, mutation.value),
                )
