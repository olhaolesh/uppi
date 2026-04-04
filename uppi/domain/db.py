"""Тонкі runtime-хелпери для PostgreSQL з additive seam для DI foundation."""

from __future__ import annotations

import logging
from typing import Callable

import psycopg2
from decouple import config
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from psycopg2 import OperationalError, InterfaceError

from uppi.config.app_config import DatabaseConfig


logger = logging.getLogger(__name__)

DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")
DB_SSL_MODE = config("DB_SSL_MODE", default="prefer")


def get_default_database_config() -> DatabaseConfig:
    """Повертає canonical DB-конфіг, сумісний з поточними module defaults.

    Current source лишається env-driven. Future provider-backed runtime
    (наприклад SSM/Secrets Manager) має постачати той самий `DatabaseConfig`
    без зміни connection contract нижче.
    """
    return DatabaseConfig(
        host=DB_HOST,
        port=int(DB_PORT),
        name=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        ssl_mode=DB_SSL_MODE,
    )


def build_pg_connection_kwargs(db_config: DatabaseConfig | None = None) -> dict[str, object]:
    """Формує kwargs для psycopg2.connect з explicit або canonical DB-конфігу."""
    return (db_config or get_default_database_config()).connect_kwargs()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    retry=retry_if_exception_type((OperationalError, InterfaceError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def get_pg_connection(
    db_config: DatabaseConfig | None = None,
    *,
    connect_factory: Callable[..., psycopg2.extensions.connection] | None = None,
):
    """
    Повертає новий psycopg2 connection з поточними default semantics.

    Важливо:
    - autocommit = False (транзакції керуються явно)
    - при виключеннях нехай падає, бо це критична інфраструктура
    - factory seam already дозволяє future runtime/provider work без зміни
      transaction ownership
    Retry:
    - тільки transient network / channel errors
    - тільки на connect()
    """
    resolved_connect_factory = connect_factory or psycopg2.connect
    try:
        conn = resolved_connect_factory(**build_pg_connection_kwargs(db_config))
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        logger.exception("[DB] Не вдалося підключитися до PostgreSQL: %s", e)
        raise


def db_has_visura(cf: str) -> bool:
    """
    Повертає True, якщо візура для заданого CF існує в таблиці visure.
    """
    conn = None
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM public.visure WHERE locatore_cf = %s LIMIT 1;",
                (cf,),
            )
            exists = cur.fetchone() is not None
            logger.debug("[DB] db_has_visura(%s) → %s", cf, exists)
            conn.commit()
            return exists
    except psycopg2.Error as e:
        logger.exception("[DB] Помилка при перевірці visura для %s: %s", cf, e)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn is not None:
            conn.close()
