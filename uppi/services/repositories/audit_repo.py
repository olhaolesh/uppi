"""Тонкий repository-модуль для audit і розрахункових логів."""

from __future__ import annotations

from typing import Any, Dict, Optional

import psycopg2.extras

from uppi.utils.parse_utils import safe_float


def db_insert_canone_calc(
    conn,
    contract_id: str,
    method: str,
    inputs: Dict[str, Any],
    result_mensile: Optional[float],
) -> None:
    """Логує розрахунок канону без зміни поточного SQL-контракту."""
    res = inputs.get("result") or {}
    min_val = safe_float(res.get("base_min_euro_mq"))
    max_val = safe_float(res.get("base_max_euro_mq"))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.canone_calcoli (contract_id, inputs, min_val, max_val, result_mensile)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (contract_id, calculated_at) DO NOTHING;
            """,
            (contract_id, psycopg2.extras.Json(inputs), min_val, max_val, result_mensile),
        )


def db_insert_attestazione_log(
    conn,
    contract_id: str,
    status: str,
    output_bucket: str,
    output_object: str,
    params_snapshot: Dict[str, Any],
    error: Optional[str],
    author_login_masked: str,
    author_login_sha256: str,
    template_version: str,
) -> None:
    """Логує факт генерації атестації в поточну audit-схему."""
    full_snapshot = params_snapshot.copy()
    full_snapshot.update(
        {
            "error": error,
            "author_masked": author_login_masked,
            "template_version": template_version,
        }
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.attestazioni (
              contract_id,
              output_bucket,
              output_object,
              full_data_snapshot,
              author_hash,
              status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                contract_id,
                output_bucket,
                output_object,
                psycopg2.extras.Json(full_snapshot),
                author_login_sha256,
                status,
            ),
        )
