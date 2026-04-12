"""Prepare-oriented read model for building single-client `immobili.yml` from DB."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import psycopg2.extras

from uppi.services.repositories.common import logger
from uppi.utils.db_utils.key_normalize import normalize_element_key


@dataclass(frozen=True)
class PrepareDocumentRootRow:
    """Client/root data needed for the generated `immobili.yml` document."""

    locatore_cf: str
    locatore_comune_res: str | None
    locatore_via: str | None
    locatore_civico: str | None


@dataclass(frozen=True)
class PrepareDocumentPresence:
    """Deterministic DB hit/miss status for prepare-by-CF decisions."""

    locatore_cf: str
    root_found: bool
    immobili_count: int

    @property
    def is_hit(self) -> bool:
        """A prepare DB hit requires both the owner row and at least one immobile."""
        return self.root_found and self.immobili_count > 0


@dataclass(frozen=True)
class PrepareDocumentImmobileRow:
    """One DB-backed immobile row ready to be mapped into the YAML document."""

    immobile_id: int
    foglio: str
    numero: str
    sub: str
    rendita: str | None
    superficie_totale: float | None
    categoria: str | None
    visura_comune: str | None
    visura_via: str | None
    visura_civico: str | None
    immobile_comune: str | None
    immobile_via: str | None
    immobile_civico: str | None
    immobile_piano: str | None
    immobile_interno: str | None
    energy_class: str | None
    contract_kind: str | None
    arredato: float | None
    istat: float | None
    ignore_surcharges: bool | None


def db_load_prepare_document_presence(conn, owner_cf: str) -> PrepareDocumentPresence:
    """Loads the prepare DB hit/miss criterion without relying on side effects."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                EXISTS(
                    SELECT 1
                    FROM public.persons p
                    WHERE p.cf = %s
                ) AS root_found,
                (
                    SELECT COUNT(*)
                    FROM public.immobili i
                    WHERE i.owner_cf = %s
                ) AS immobili_count;
            """,
            (owner_cf, owner_cf),
        )
        row = cur.fetchone()

    return PrepareDocumentPresence(
        locatore_cf=str(owner_cf),
        root_found=bool(row["root_found"]),
        immobili_count=int(row["immobili_count"] or 0),
    )


def db_load_prepare_document_root(conn, owner_cf: str) -> PrepareDocumentRootRow | None:
    """Loads root client data without inventing fallback business values."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                p.cf AS locatore_cf,
                a.comune AS locatore_comune_res,
                a.via_full AS locatore_via,
                a.civico AS locatore_civico
            FROM public.persons p
            LEFT JOIN public.addresses a ON a.id = p.residence_address_id
            WHERE p.cf = %s;
            """,
            (owner_cf,),
        )
        row = cur.fetchone()

    if not row:
        return None

    return PrepareDocumentRootRow(
        locatore_cf=str(row["locatore_cf"]),
        locatore_comune_res=row["locatore_comune_res"],
        locatore_via=row["locatore_via"],
        locatore_civico=row["locatore_civico"],
    )


def db_load_prepare_document_immobili(conn, owner_cf: str) -> list[PrepareDocumentImmobileRow]:
    """Loads one deterministic list of immobili plus allowlisted persistable contract fields."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                i.id AS immobile_id,
                i.foglio,
                i.numero,
                COALESCE(i.sub, '') AS sub,
                i.rendita,
                i.superficie_totale,
                i.categoria,
                va.comune AS visura_comune,
                va.via_full AS visura_via,
                va.civico AS visura_civico,
                ra.comune AS immobile_comune,
                ra.via_full AS immobile_via,
                ra.civico AS immobile_civico,
                ra.piano AS immobile_piano,
                ra.interno AS immobile_interno,
                i.energy_class,
                latest_contract.contract_kind,
                latest_contract.arredato_pct,
                latest_contract.istat_rate,
                latest_contract.ignore_surcharges
            FROM public.immobili i
            LEFT JOIN public.addresses va ON va.id = i.visura_address_id
            LEFT JOIN public.addresses ra ON ra.id = i.real_address_id
            LEFT JOIN LATERAL (
                SELECT
                    c.contract_kind,
                    c.arredato_pct,
                    c.istat_rate,
                    c.ignore_surcharges
                FROM public.contracts c
                WHERE c.immobile_id = i.id
                ORDER BY c.updated_at DESC, c.created_at DESC, c.id DESC
                LIMIT 1
            ) latest_contract ON TRUE
            WHERE i.owner_cf = %s
            ORDER BY i.foglio, i.numero, COALESCE(i.sub, ''), i.id;
            """,
            (owner_cf,),
        )
        rows = cur.fetchall()

    out: list[PrepareDocumentImmobileRow] = []
    for row in rows:
        out.append(
            PrepareDocumentImmobileRow(
                immobile_id=int(row["immobile_id"]),
                foglio=str(row["foglio"]),
                numero=str(row["numero"]),
                sub=str(row["sub"] or ""),
                rendita=row["rendita"],
                superficie_totale=float(row["superficie_totale"]) if row["superficie_totale"] is not None else None,
                categoria=row["categoria"],
                visura_comune=row["visura_comune"],
                visura_via=row["visura_via"],
                visura_civico=row["visura_civico"],
                immobile_comune=row["immobile_comune"],
                immobile_via=row["immobile_via"],
                immobile_civico=row["immobile_civico"],
                immobile_piano=row["immobile_piano"],
                immobile_interno=row["immobile_interno"],
                energy_class=row["energy_class"],
                contract_kind=row["contract_kind"],
                arredato=float(row["arredato_pct"]) if row["arredato_pct"] is not None else None,
                istat=float(row["istat_rate"]) if row["istat_rate"] is not None else None,
                ignore_surcharges=row["ignore_surcharges"],
            )
        )

    return out


def db_load_prepare_document_elements(
    conn,
    immobile_ids: list[int],
) -> Dict[int, Dict[str, str]]:
    """Loads A/B/C/D elements keyed by immobile id in canonical lower-case shape."""
    if not immobile_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT immobile_id, grp, code, value
            FROM public.immobile_elements
            WHERE immobile_id = ANY(%s)
            ORDER BY immobile_id, grp, code;
            """,
            (immobile_ids,),
        )
        rows = cur.fetchall()

    elements_by_immobile: Dict[int, Dict[str, str]] = {immobile_id: {} for immobile_id in immobile_ids}
    for immobile_id, grp, code, value in rows:
        normalized_key = normalize_element_key(str(grp or ""), str(code or ""))
        if not normalized_key:
            logger.warning(
                "[PREPARE_DOC] Skipping non-normalizable element for immobile_id=%s grp=%r code=%r",
                immobile_id,
                grp,
                code,
            )
            continue
        elements_by_immobile.setdefault(int(immobile_id), {})[normalized_key] = "" if value is None else str(value)

    return elements_by_immobile
