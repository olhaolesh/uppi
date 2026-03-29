"""CLI-утиліта для ініціалізації поточної схеми PostgreSQL із SQL-файлу."""

from pathlib import Path

import psycopg2
from decouple import config

SCHEMA_FILE = Path(__file__).resolve().with_name("uppi_schema.sql")


def execute_sql_file(filename, db_config):
    """
    Виконує SQL-запити з вказаного файлу для ініціалізації схеми бази даних.
    """
    conn = None
    cursor = None
    filename = Path(filename)
    
    try:
        print(f"Спроба підключення до {db_config['host']}...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Читання файлу {filename}...")
        with filename.open('r', encoding='utf-8') as f:
            sql_script = f.read()
        
        print("Виконання SQL-запитів...")
        cursor.execute(sql_script)
        
        conn.commit()
        print("Схему успішно створено!")
        
    except Exception as e:
        print(f"❌ Помилка при виконанні: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn: 
            conn.close()
        print("З'єднання закрите.")


def build_db_config():
    """Будує конфіг підключення до БД з поточних env-параметрів."""
    return {
        "dbname": config("DB_NAME", default="uppi_db"),
        "user": config("DB_USER", default="uppi_user"),
        "password": config("DB_PASSWORD", default="uppi_password"),
        "host": config("DB_HOST", default="localhost"),
        "port": config("DB_PORT", default="5432"),
        "sslmode": config("DB_SSL_MODE", default="prefer"),
    }


#  НАЛАШТУВАННЯ БАЗИ ДАНИХ З .ENV ФАЙЛУ
config_db = build_db_config()

if __name__ == "__main__":
    execute_sql_file(SCHEMA_FILE, config_db)
