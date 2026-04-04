"""Тонкий repository-модуль для стану та метаданих візури."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from psycopg2 import Error as Psycopg2Error

from uppi.services.repositories.common import logger


def db_upsert_visura(
    conn,
    cf: str,
    pdf_bucket: str,
    pdf_object: str,
    checksum_sha256: Optional[str],
    fetched_now: bool,
) -> int:
    """Зберігає метадані візури й повертає її ID."""
    now = datetime.now() if fetched_now else None

    sql = """
        INSERT INTO public.visure (locatore_cf, pdf_bucket, pdf_object, checksum_sha256, fetched_at)
        VALUES (%(cf)s, %(bucket)s, %(obj)s, %(sum)s, %(now)s)
        ON CONFLICT (locatore_cf) DO UPDATE
        SET
          pdf_bucket      = EXCLUDED.pdf_bucket,
          pdf_object      = EXCLUDED.pdf_object,
          checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256),
          fetched_at      = COALESCE(EXCLUDED.fetched_at, visure.fetched_at),
          updated_at      = now()
        RETURNING id;
    """

    params = {
        "cf": cf,
        "bucket": pdf_bucket,
        "obj": pdf_object,
        "sum": checksum_sha256,
        "now": now,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            if result:
                return result[0]
            raise RuntimeError(f"Failed to upsert visura for CF: {cf}")
    except Psycopg2Error as exc:
        logger.error(f"[DB] db_upsert_visura error: {exc}")
        raise


@dataclass(frozen=True)
class VisuraState:
    """Стислий зріз стану актуальної візури для policy-рішень."""

    cf: str
    pdf_bucket: Optional[str]
    pdf_object: Optional[str]
    fetched_at: Optional[Any]
    id: Optional[int] = None


def fetch_visura_state(conn, cf: str) -> Optional[VisuraState]:
    """Завантажує поточний стан візури для конкретного орендодавця."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT locatore_cf, pdf_bucket, pdf_object, fetched_at, id
            FROM public.visure
            WHERE locatore_cf = %s;
            """,
            (cf,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return VisuraState(cf=row[0], pdf_bucket=row[1], pdf_object=row[2], fetched_at=row[3], id=row[4])
