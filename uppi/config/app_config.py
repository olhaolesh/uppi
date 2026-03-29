"""Прості dataclass-конфіги для bootstrap-етапу застосунку."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from decouple import config


@dataclass(frozen=True)
class DatabaseConfig:
    """Параметри підключення до PostgreSQL."""
    host: str
    port: int
    name: str
    user: str
    password: str
    ssl_mode: str



@dataclass(frozen=True)
class AppConfig:
    """Агрегує верхньорівневі налаштування застосунку."""
    database: DatabaseConfig


    @staticmethod
    def _parse_int(value: str | None) -> Optional[int]:
        """Перетворює необов’язковий рядок на ціле число або None."""
        if value is None:
            return None
        value = str(value).strip()
        if value == "":
            return None
        return int(value)

    @classmethod
    def from_env(cls) -> "AppConfig":

        """Створює конфіг застосунку з поточних env-параметрів."""
        db = DatabaseConfig(
            host=config("DB_HOST", default="localhost"),
            port=int(config("DB_PORT", default="5432")),
            name=config("DB_NAME", default="uppi_db"),
            user=config("DB_USER", default="uppi_user"),
            password=config("DB_PASSWORD", default="uppi_password"),
            ssl_mode=config("DB_SSL_MODE", default="prefer"),
        )
        return cls(database=db)
