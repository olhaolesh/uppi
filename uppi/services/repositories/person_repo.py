"""Тонкий repository-модуль для записів про осіб."""

from __future__ import annotations

from typing import Optional


def db_upsert_person(
    conn,
    cf: str,
    surname: Optional[str],
    name: Optional[str],
    address_id: Optional[int] = None,
) -> None:
    """Створює або оновлює особу, зберігаючи current COALESCE-semantics."""
    if not cf:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.persons (cf, surname, name, residence_address_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cf) DO UPDATE
            SET
              surname = COALESCE(EXCLUDED.surname, persons.surname),
              name    = COALESCE(EXCLUDED.name, persons.name),
              residence_address_id = COALESCE(EXCLUDED.residence_address_id, persons.residence_address_id),
              updated_at = now();
            """,
            (cf, surname, name, address_id),
        )


def db_update_person_residence_address(
    conn,
    cf: str,
    address_id: Optional[int],
) -> None:
    """Replace the current residence address pointer, allowing explicit clears."""
    if not cf:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.persons
            SET residence_address_id = %s,
                updated_at = now()
            WHERE cf = %s;
            """,
            (address_id, cf),
        )
