# uppi/domain/db.py
from __future__ import annotations

import logging

import psycopg2
from decouple import config

logger = logging.getLogger(__name__)

DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")
DB_SSL_MODE = config("DB_SSL_MODE", default="prefer")


def get_pg_connection():
    """
    Отримати новий конекшн до PostgreSQL (psycopg2).

    Важливо:
    - autocommit = False (транзакції керуються явно)
    - виключення не ковтаємо: нехай падає, бо це критична інфраструктура
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode=DB_SSL_MODE,
        )
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
            cur.execute("SELECT 1 FROM public.visure WHERE cf = %s LIMIT 1;", (cf,))
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
