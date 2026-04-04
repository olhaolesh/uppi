"""Тонкий repository-модуль для адресних SQL-операцій."""

from __future__ import annotations

from typing import Any, Dict, Optional

from uppi.services.repositories.common import logger
from uppi.utils.parse_utils import clean_str


def db_upsert_address(conn, addr_data: Dict[str, Any]) -> Optional[int]:
    """Знаходить існуючу адресу або створює нову й повертає її ID."""
    comune = clean_str(addr_data.get("comune"))
    via_full = clean_str(addr_data.get("via_full"))
    if not via_full:
        via_type = clean_str(addr_data.get("via_type"))
        via_name = clean_str(addr_data.get("via_name"))
        if via_type and via_name:
            via_full = f"{via_type} {via_name}"
        elif via_name:
            via_full = via_name

    if not comune or not via_full:
        return None

    civico = clean_str(addr_data.get("civico"))
    piano = clean_str(addr_data.get("piano"))
    interno = clean_str(addr_data.get("interno"))
    scala = clean_str(addr_data.get("scala"))

    sql = """
    INSERT INTO public.addresses (comune, via_full, civico, piano, interno, scala)
    VALUES (
        %(comune)s,
        %(via_full)s,
        COALESCE(%(civico)s, 'SNC'),
        %(piano)s,
        %(interno)s,
        %(scala)s
    )
    ON CONFLICT (content_hash) DO UPDATE
    SET created_at = public.addresses.created_at
    RETURNING id;
    """

    params = {
        "comune": comune,
        "via_full": via_full,
        "civico": civico,
        "piano": piano,
        "interno": interno,
        "scala": scala,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            res = cur.fetchone()
            if res:
                return res[0]
            cur.execute(
                r"SELECT id FROM public.addresses WHERE content_hash = md5(upper(trim(%s)) || '|' || upper(trim(regexp_replace(%s, '\s+', ' ', 'g'))) || '|' || upper(trim(COALESCE(%s, 'SNC'))))",
                (comune, via_full, civico),
            )
            res_fallback = cur.fetchone()
            return res_fallback[0] if res_fallback else None
    except Exception as exc:
        logger.error(f"[DB] Address upsert failed: {exc}")
        raise
