"""
Scrapy pipelines for the Uppi project.

Тут відбувається "зведення" всіх шарів:
- YAML (clients.yml через map_yaml_to_item),
- SISTER/PDF (VisuraParser),
- БД (збереження/зчитування immobili),
- MinIO (зберігання PDF-візур),
- генерація DOCX-атестацйї.

Результат — заповнений UppiItem, з яким вже можна:
- робити DOCX,
- будувати звіти,
- дебажити весь флоу.
"""

from pathlib import Path
from typing import Dict, List, Any
import re

import logging

from itemadapter import ItemAdapter
from minio import Minio
from minio.error import S3Error
from decouple import config

from uppi.domain.db import db_has_visura, _get_pg_connection
from uppi.docs.visura_pdf_parser import VisuraParser
from uppi.domain.storage import get_visura_path, get_attestazione_path
from uppi.docs.attestazione_template_filler import fill_attestazione_template, underscored
from uppi.domain.immobile import Immobile

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MinIO/S3 configuration (зберігання PDF-візур)
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = config("MINIO_ENDPOINT", default="localhost:9000")
MINIO_ACCESS_KEY = config("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = config("MINIO_SECRET_KEY", default="minioadmin")
MINIO_SECURE = config("MINIO_SECURE", default="False").lower() == "true"
MINIO_BUCKET = config("MINIO_BUCKET", default="visure")

_minio_client: Minio | None = None


def _get_minio_client() -> Minio:
    """
    Повертає singleton-клієнт MinIO.
    Створює бакет, якщо він ще не існує.
    """
    global _minio_client
    if _minio_client is not None:
        return _minio_client

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        # гарантуємо, що бакет існує
        if not client.bucket_exists(MINIO_BUCKET):
            logger.info(
                "[MINIO] Bucket %r не існує, пробуємо створити на %s",
                MINIO_BUCKET,
                MINIO_ENDPOINT,
            )
            client.make_bucket(MINIO_BUCKET)
        _minio_client = client
        logger.info("[MINIO] Підключення до %s, bucket=%s готове", MINIO_ENDPOINT, MINIO_BUCKET)
        return _minio_client
    except S3Error as e:
        logger.exception("[MINIO] Помилка роботи з MinIO: %s", e)
        raise
    except Exception as e:
        logger.exception("[MINIO] Неочікувана помилка ініціалізації MinIO: %s", e)
        raise


# ---------------------------------------------------------------------------
# DB helpers: завантаження/збереження visura + immobili
# ---------------------------------------------------------------------------
def load_immobiles_from_db(cf: str) -> List[Immobile]:
    """
    Завантажити всі Immobile для візури конкретного CF з таблиці immobili.

    ВАЖЛИВО: SQL повністю відповідає поточній схемі таблиці `immobili`
    і полям dataclass `Immobile`.
    """
    conn = None
    try:
        conn = _get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    -- базові поля таблиці (з візури)
                    table_num_immobile,
                    sez_urbana,
                    foglio,
                    numero,
                    sub,
                    zona_cens,
                    micro_zona,
                    categoria,
                    classe,
                    consistenza,
                    superficie_totale,
                    superficie_escluse,
                    superficie_raw,
                    rendita,

                    -- адреса з візури
                    immobile_comune,
                    immobile_comune_code,
                    via_type,
                    via_name,
                    via_num,
                    scala,
                    interno,
                    piano,
                    indirizzo_raw,
                    dati_ulteriori,

                    -- дані локатора з візури
                    locatore_surname,
                    locatore_name,
                    locatore_codice_fiscale,

                    -- OVERRIDE (реальна адреса об'єкта)
                    immobile_comune_override,
                    immobile_via_override,
                    immobile_civico_override,
                    immobile_piano_override,
                    immobile_interno_override,

                    -- адреса локатора (з YAML, збережена в БД)
                    locatore_comune_res,
                    locatore_via,
                    locatore_civico
                FROM immobili
                WHERE visura_cf = %s
                ORDER BY id;
                """,
                (cf,),
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.exception("[DB] Не вдалося прочитати immobili для %s: %s", cf, e)
        return []
    finally:
        if conn is not None:
            conn.close()

    immobiles: List[Immobile] = []
    for row in rows:
        (
            table_num_immobile,
            sez_urbana,
            foglio,
            numero,
            sub,
            zona_cens,
            micro_zona,
            categoria,
            classe,
            consistenza,
            superficie_totale,
            superficie_escluse,
            superficie_raw,
            rendita,
            immobile_comune,
            immobile_comune_code,
            via_type,
            via_name,
            via_num,
            scala,
            interno,
            piano,
            indirizzo_raw,
            dati_ulteriori,
            locatore_surname,
            locatore_name,
            locatore_codice_fiscale,
            immobile_comune_override,
            immobile_via_override,
            immobile_civico_override,
            immobile_piano_override,
            immobile_interno_override,
            locatore_comune_res,
            locatore_via,
            locatore_civico,
        ) = row

        immobiles.append(
            Immobile(
                table_num_immobile=table_num_immobile,
                sez_urbana=sez_urbana,
                foglio=foglio,
                numero=numero,
                sub=sub,
                zona_cens=zona_cens,
                micro_zona=micro_zona,
                categoria=categoria,
                classe=classe,
                consistenza=consistenza,
                superficie_totale=superficie_totale,
                superficie_escluse=superficie_escluse,
                superficie_raw=superficie_raw,
                rendita=rendita,
                immobile_comune=immobile_comune,
                immobile_comune_code=immobile_comune_code,
                via_type=via_type,
                via_name=via_name,
                via_num=via_num,
                scala=scala,
                interno=interno,
                piano=piano,
                indirizzo_raw=indirizzo_raw,
                dati_ulteriori=dati_ulteriori,
                locatore_surname=locatore_surname,
                locatore_name=locatore_name,
                locatore_codice_fiscale=locatore_codice_fiscale,
                immobile_comune_override=immobile_comune_override,
                immobile_via_override=immobile_via_override,
                immobile_civico_override=immobile_civico_override,
                immobile_piano_override=immobile_piano_override,
                immobile_interno_override=immobile_interno_override,
                locatore_comune_res=locatore_comune_res,
                locatore_via=locatore_via,
                locatore_civico=locatore_civico,
            )
        )

    logger.debug("[DB] Завантажено %d immobili для %s", len(immobiles), cf)
    return immobiles


def save_visura(cf: str, immobiles: List[Immobile], pdf_path: Path) -> None:
    """
    Зберігає:
    - PDF-візуру в MinIO (visure/<cf>.pdf),
    - метадані в таблиці visure,
    - усі immobili в таблиці immobili.

    Після успішного запису локальний PDF видаляється.

    при FORCE_UPDATE_VISURA старі override-и не губляться, 
    навіть якщо в актуальному YAML ти не передав адресу повторно
    """
    object_name = f"visure/{cf}.pdf"

    # 1. Заливаємо PDF у MinIO
    try:
        client = _get_minio_client()
        logger.info(
            "[MINIO] Завантажуємо PDF візури для %s у bucket=%s, object=%s",
            cf,
            MINIO_BUCKET,
            object_name,
        )
        client.fput_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            file_path=str(pdf_path),
            content_type="application/pdf",
        )
    except S3Error as e:
        logger.exception("[MINIO] Помилка при завантаженні PDF для %s: %s", cf, e)
        raise
    except Exception as e:
        logger.exception("[MINIO] Неочікувана помилка при завантаженні PDF для %s: %s", cf, e)
        raise

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # 0. Зчитуємо старі override-и та адресу локатора для цього CF
                cur.execute(
                    """
                    SELECT
                        foglio,
                        numero,
                        sub,
                        immobile_comune_override,
                        immobile_via_override,
                        immobile_civico_override,
                        immobile_piano_override,
                        immobile_interno_override,
                        locatore_comune_res,
                        locatore_via,
                        locatore_civico
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                old_rows = cur.fetchall()
                old_map: dict[tuple[str, str, str], dict[str, Any]] = {}

                for (
                    foglio,
                    numero,
                    sub,
                    immobile_comune_override,
                    immobile_via_override,
                    immobile_civico_override,
                    immobile_piano_override,
                    immobile_interno_override,
                    locatore_comune_res,
                    locatore_via,
                    locatore_civico,
                ) in old_rows:
                    key = (
                        str(foglio) if foglio is not None else "",
                        str(numero) if numero is not None else "",
                        str(sub) if sub is not None else "",
                    )
                    old_map[key] = {
                        "immobile_comune_override": immobile_comune_override,
                        "immobile_via_override": immobile_via_override,
                        "immobile_civico_override": immobile_civico_override,
                        "immobile_piano_override": immobile_piano_override,
                        "immobile_interno_override": immobile_interno_override,
                        "locatore_comune_res": locatore_comune_res,
                        "locatore_via": locatore_via,
                        "locatore_civico": locatore_civico,
                    }

                # 1. upsert у visure (метадані PDF)
                cur.execute(
                    """
                    INSERT INTO visure (cf, pdf_bucket, pdf_object, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (cf) DO UPDATE
                    SET pdf_bucket = EXCLUDED.pdf_bucket,
                        pdf_object = EXCLUDED.pdf_object,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    (cf, MINIO_BUCKET, object_name),
                )

                # 2. чистимо старі immobili для цього cf
                cur.execute("DELETE FROM immobili WHERE visura_cf = %s;", (cf,))

                insert_sql = """
                    INSERT INTO immobili (
                        visura_cf,
                        table_num_immobile,
                        sez_urbana,
                        foglio,
                        numero,
                        sub,
                        zona_cens,
                        micro_zona,
                        categoria,
                        classe,
                        consistenza,
                        superficie_totale,
                        superficie_escluse,
                        superficie_raw,
                        rendita,
                        immobile_comune,
                        immobile_comune_code,
                        via_type,
                        via_name,
                        via_num,
                        scala,
                        interno,
                        piano,
                        indirizzo_raw,
                        dati_ulteriori,
                        locatore_surname,
                        locatore_name,
                        locatore_codice_fiscale,
                        immobile_comune_override,
                        immobile_via_override,
                        immobile_civico_override,
                        immobile_piano_override,
                        immobile_interno_override,
                        locatore_comune_res,
                        locatore_via,
                        locatore_civico
                    ) VALUES (
                        %s,  -- visura_cf
                        %s,  -- table_num_immobile
                        %s,  -- sez_urbana
                        %s,  -- foglio
                        %s,  -- numero
                        %s,  -- sub
                        %s,  -- zona_cens
                        %s,  -- micro_zona
                        %s,  -- categoria
                        %s,  -- classe
                        %s,  -- consistenza
                        %s,  -- superficie_totale
                        %s,  -- superficie_escluse
                        %s,  -- superficie_raw
                        %s,  -- rendita
                        %s,  -- immobile_comune
                        %s,  -- immobile_comune_code
                        %s,  -- via_type
                        %s,  -- via_name
                        %s,  -- via_num
                        %s,  -- scala
                        %s,  -- interno
                        %s,  -- piano
                        %s,  -- indirizzo_raw
                        %s,  -- dati_ulteriori
                        %s,  -- locatore_surname
                        %s,  -- locatore_name
                        %s,  -- locatore_codice_fiscale
                        %s,  -- immobile_comune_override
                        %s,  -- immobile_via_override
                        %s,  -- immobile_civico_override
                        %s,  -- immobile_piano_override
                        %s,  -- immobile_interno_override
                        %s,  -- locatore_comune_res
                        %s,  -- locatore_via
                        %s   -- locatore_civico
                    );
                """

                for imm in immobiles:
                    # Підтягуємо старі override-и й адресу локатора, якщо вони були
                    key = (
                        str(getattr(imm, "foglio", "") or ""),
                        str(getattr(imm, "numero", "") or ""),
                        str(getattr(imm, "sub", "") or ""),
                    )
                    old = old_map.get(key)
                    if old:
                        for field in (
                            "immobile_comune_override",
                            "immobile_via_override",
                            "immobile_civico_override",
                            "immobile_piano_override",
                            "immobile_interno_override",
                            "locatore_comune_res",
                            "locatore_via",
                            "locatore_civico",
                        ):
                            current_val = getattr(imm, field, None)
                            if current_val in (None, "") and old.get(field) is not None:
                                setattr(imm, field, old[field])

                    cur.execute(
                        insert_sql,
                        (
                            cf,
                            getattr(imm, "table_num_immobile", None),
                            getattr(imm, "sez_urbana", None),
                            getattr(imm, "foglio", None),
                            getattr(imm, "numero", None),
                            getattr(imm, "sub", None),
                            getattr(imm, "zona_cens", None),
                            getattr(imm, "micro_zona", None),
                            getattr(imm, "categoria", None),
                            getattr(imm, "classe", None),
                            getattr(imm, "consistenza", None),
                            getattr(imm, "superficie_totale", None),
                            getattr(imm, "superficie_escluse", None),
                            getattr(imm, "superficie_raw", None),
                            getattr(imm, "rendita", None),
                            getattr(imm, "immobile_comune", None),
                            getattr(imm, "immobile_comune_code", None),
                            getattr(imm, "via_type", None),
                            getattr(imm, "via_name", None),
                            getattr(imm, "via_num", None),
                            getattr(imm, "scala", None),
                            getattr(imm, "interno", None),
                            getattr(imm, "piano", None),
                            getattr(imm, "indirizzo_raw", None),
                            getattr(imm, "dati_ulteriori", None),
                            getattr(imm, "locatore_surname", None),
                            getattr(imm, "locatore_name", None),
                            getattr(imm, "locatore_codice_fiscale", None),
                            getattr(imm, "immobile_comune_override", None),
                            getattr(imm, "immobile_via_override", None),
                            getattr(imm, "immobile_civico_override", None),
                            getattr(imm, "immobile_piano_override", None),
                            getattr(imm, "immobile_interno_override", None),
                            getattr(imm, "locatore_comune_res", None),
                            getattr(imm, "locatore_via", None),
                            getattr(imm, "locatore_civico", None),
                        ),
                    )

        logger.info(
            "[DB] Збережено visura + %d immobili для %s (PDF: %s/%s)",
            len(immobiles),
            cf,
            MINIO_BUCKET,
            object_name,
        )
    except Exception as e:
        logger.exception("[DB] Помилка при збереженні visura для %s: %s", cf, e)
        raise
    finally:
        if conn is not None:
            conn.close()
        try:
            pdf_path.unlink()
            logger.debug("[PIPELINE] Локальний PDF видалено: %s", pdf_path)
        except FileNotFoundError:
            logger.debug("[PIPELINE] Локальний PDF вже відсутній: %s", pdf_path)
        except Exception as e:
            logger.warning("[PIPELINE] Не вдалося видалити локальний PDF %s: %s", pdf_path, e)


# ---------------------------------------------------------------------------
# Оновлення override-адрес/locatore в БД з YAML
# ---------------------------------------------------------------------------
def _clean_str(value: Any) -> str | None:
    """
    Приводить до рядка й відкидає "порожні" значення.

    None       -> None
    "" / "  "  -> None
    інше       -> stripped str(...)
    """
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def upsert_overrides_from_yaml(cf: str, adapter: ItemAdapter) -> None:
    """
    Оновлює в таблиці `immobili`:
    - реальну адресу об'єкта (immobile_*_override) — ТІЛЬКИ для конкретного immobile;
    - адресу локатора (locatore_*) — ДЛЯ ВСІХ immobili з цим CF.

    Логіка:
    1) якщо в YAML немає жодного IMMOBILE_* і жодного LOCATORE_* → нічого не робимо;
    2) якщо є IMMOBILE_*:
         - якщо у CF кілька immobili і немає FOGLIO/NUMERO/SUB → не оновлюємо override-адресу;
         - інакше оновлюємо ті рядки, що відповідають ключу (foglio/numero/sub);
    3) якщо є LOCATORE_*:
         - оновлюємо locatore_* для ВСІХ рядків visura_cf = cf.
    """

    # --- нормалізація YAML-значень ---
    immobile_comune = _clean_str(adapter.get("immobile_comune"))
    immobile_via = _clean_str(adapter.get("immobile_via"))
    immobile_civico = _clean_str(adapter.get("immobile_civico"))
    immobile_piano = _clean_str(adapter.get("immobile_piano"))
    immobile_interno = _clean_str(adapter.get("immobile_interno"))

    locatore_comune_res = _clean_str(adapter.get("locatore_comune_res"))
    locatore_via = _clean_str(adapter.get("locatore_via"))
    locatore_civico = _clean_str(adapter.get("locatore_civico"))

    any_obj_override = any(
        v is not None
        for v in (
            immobile_comune,
            immobile_via,
            immobile_civico,
            immobile_piano,
            immobile_interno,
        )
    )
    any_locatore = any(
        v is not None for v in (locatore_comune_res, locatore_via, locatore_civico)
    )

    # Немає жодних даних для оновлення
    if not any_obj_override and not any_locatore:
        return

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # Дивимось, скільки immobili є для цього CF
                cur.execute(
                    """
                    SELECT id, foglio, numero, sub
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                rows = cur.fetchall()

                if not rows:
                    logger.warning(
                        "[DB] upsert_overrides_from_yaml: для %s немає записів у immobili "
                        "(visura ще не збережена?)",
                        cf,
                    )
                    return

                # -----------------------------
                # 1) OVERRIDE РЕАЛЬНОЇ АДРЕСИ ОБ'ЄКТА
                # -----------------------------
                if any_obj_override:
                    foglio = _clean_str(adapter.get("foglio"))
                    numero = _clean_str(adapter.get("numero"))
                    sub = _clean_str(adapter.get("sub"))

                    where_clauses = ["visura_cf = %s"]
                    where_params: list[Any] = [cf]

                    if foglio is not None:
                        where_clauses.append("foglio = %s")
                        where_params.append(foglio)
                    if numero is not None:
                        where_clauses.append("numero = %s")
                        where_params.append(numero)
                    if sub is not None:
                        where_clauses.append("sub = %s")
                        where_params.append(sub)

                    # Немає FOGLIO/NUMERO/SUB, але кілька об'єктів — не знаємо, який оновлювати
                    if len(where_clauses) == 1 and len(rows) > 1:
                        logger.warning(
                            "[DB] upsert_overrides_from_yaml: для %s є %d immobili, "
                            "але FOGLIO/NUMERO/SUB у YAML не задані — "
                            "пропускаємо оновлення реальної адреси об'єкта, "
                            "щоб не присвоїти одну адресу всім об'єктам.",
                            cf,
                            len(rows),
                        )
                    else:
                        sql_obj = f"""
                            UPDATE immobili
                            SET
                                immobile_comune_override  = COALESCE(%s, immobile_comune_override),
                                immobile_via_override     = COALESCE(%s, immobile_via_override),
                                immobile_civico_override  = COALESCE(%s, immobile_civico_override),
                                immobile_piano_override   = COALESCE(%s, immobile_piano_override),
                                immobile_interno_override = COALESCE(%s, immobile_interno_override)
                            WHERE {" AND ".join(where_clauses)};
                        """
                        params_obj: list[Any] = [
                            immobile_comune,
                            immobile_via,
                            immobile_civico,
                            immobile_piano,
                            immobile_interno,
                            *where_params,
                        ]
                        cur.execute(sql_obj, params_obj)
                        logger.debug(
                            "[DB] upsert_overrides_from_yaml: для %s оновлено override-адресу об'єкта (%d рядків)",
                            cf,
                            cur.rowcount,
                        )

                # -----------------------------
                # 2) АДРЕСА ЛОКАТОРА ДЛЯ ВСІХ IMMOBILI ЦЬОГО CF
                # -----------------------------
                if any_locatore:
                    sql_loc = """
                        UPDATE immobili
                        SET
                            locatore_comune_res = COALESCE(%s, locatore_comune_res),
                            locatore_via        = COALESCE(%s, locatore_via),
                            locatore_civico     = COALESCE(%s, locatore_civico)
                        WHERE visura_cf = %s;
                    """
                    cur.execute(
                        sql_loc,
                        [locatore_comune_res, locatore_via, locatore_civico, cf],
                    )
                    logger.debug(
                        "[DB] upsert_overrides_from_yaml: для %s оновлено адресу локатора (%d рядків)",
                        cf,
                        cur.rowcount,
                    )

    except Exception as e:
        logger.exception("[DB] Помилка в upsert_overrides_from_yaml(%s): %s", cf, e)
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Фільтрація Immobile за бажаними критеріями
# ---------------------------------------------------------------------------
def filter_immobiles(immobiles: List[Immobile], adapter: ItemAdapter) -> List[Immobile]:
    """
    Фільтрує список Immobile на основі параметрів в item:
    foglio, numero, sub, rendita, superficie_totale, categoria.

    Порожні/відсутні параметри ігноруються.
    """
    filtered: List[Immobile] = []
    for imm in immobiles:
        ok = True

        foglio = adapter.get("foglio")
        if foglio and imm.foglio != str(foglio):
            ok = False

        numero = adapter.get("numero")
        if numero and imm.numero != str(numero):
            ok = False

        sub = adapter.get("sub")
        if sub and imm.sub != str(sub):
            ok = False

        categoria = adapter.get("categoria")
        if categoria and imm.categoria != str(categoria):
            ok = False

        rendita = adapter.get("rendita")
        if rendita and imm.rendita != str(rendita):
            ok = False

        superficie = adapter.get("superficie_totale")
        if (
            superficie
            and imm.superficie_totale is not None
            and float(superficie) != imm.superficie_totale
        ):
            ok = False

        if ok:
            filtered.append(imm)

    logger.debug(
        "[PIPELINE] Відібрано %d/%d immobili згідно критеріїв item",
        len(filtered),
        len(immobiles),
    )
    return filtered


# ---------------------------------------------------------------------------
# Побудова params для шаблону attestazione
# ---------------------------------------------------------------------------
def _to_str(value: Any) -> str:
    """
    Безпечне приведення до рядка:
    - None -> ""
    - інші типи -> str(...)
    """
    if value is None:
        return ""
    return str(value)

def _pick_override(imm: Immobile, override_attr: str) -> str:
    """
    Для полів реальної адреси об'єкта:
    беремо ТІЛЬКИ значення override з БД (imm.immobile_*_override).
    Якщо його немає — повертаємо "" (у шаблоні буде тільки підкреслення).
    """
    val = getattr(imm, override_attr, None)
    return _to_str(val)


def _pick_locatore_field(
    imm: Immobile,
    adapter: ItemAdapter,
    yaml_field: str,
    imm_attr: str,
) -> str:
    """
    Для адреси локатора:
    1) YAML у поточному запуску (локатор може змінити адресу)
    2) якщо YAML немає — беремо те, що вже лежить у БД (imm.locatore_*)
    3) інакше "".
    """
    yaml_val = adapter.get(yaml_field)
    if yaml_val not in (None, ""):
        return _to_str(yaml_val)

    db_val = getattr(imm, imm_attr, None)
    if db_val not in (None, ""):
        return _to_str(db_val)

    return ""

def build_params(adapter: ItemAdapter, imm: Immobile) -> Dict[str, str]:
    """
    ...
    - Адреса об'єкта — ТІЛЬКИ з imm.immobile_*_override (БД),
      YAML сюди не лізе.
    """
    params: Dict[str, str] = {}

    # -----------------------------
    # LOCATORE (власник)
    # -----------------------------
    loc_cf = adapter.get("locatore_cf") or getattr(imm, "locatore_codice_fiscale", None)
    params["{{LOCATORE_CF}}"] = _to_str(loc_cf)

    surname = imm.locatore_surname
    name = imm.locatore_name
    locatore_nome = " ".join(p for p in (name, surname) if p)
    params["{{LOCATORE_NOME}}"] = _to_str(locatore_nome)

    params["{{LOCATORE_COMUNE_RES}}"] = _pick_locatore_field(
        imm, adapter, "locatore_comune_res", "locatore_comune_res"
    )
    params["{{LOCATORE_VIA}}"] = _pick_locatore_field(
        imm, adapter, "locatore_via", "locatore_via"
    )
    params["{{LOCATORE_CIVICO}}"] = _pick_locatore_field(
        imm, adapter, "locatore_civico", "locatore_civico"
    )

    # -----------------------------
    # IMMOBILE (реальна адреса, ТІЛЬКИ override з БД)
    # -----------------------------
    params["{{IMMOBILE_COMUNE}}"] = _pick_override(imm, "immobile_comune_override")
    params["{{IMMOBILE_VIA}}"] = _pick_override(imm, "immobile_via_override")
    params["{{IMMOBILE_CIVICO}}"] = _pick_override(imm, "immobile_civico_override")
    params["{{IMMOBILE_PIANO}}"] = _pick_override(imm, "immobile_piano_override")
    params["{{IMMOBILE_INTERNO}}"] = _pick_override(imm, "immobile_interno_override")

    # -----------------------------
    # Дані катасто (з візури, збережені в БД)
    # -----------------------------
    params["{{FOGLIO}}"] = _to_str(imm.foglio)
    params["{{NUMERO}}"] = _to_str(imm.numero)
    params["{{SUB}}"] = _to_str(imm.sub)
    params["{{RENDITA}}"] = _to_str(imm.rendita)
    params["{{SUPERFICIE_TOTALE}}"] = _to_str(imm.superficie_totale)
    params["{{CATEGORIA}}"] = _to_str(imm.categoria)

    # -----------------------------
    # DATI CATASTALI – рядок APPARTAMENTO
    # -----------------------------
    # Базуємося на одному Immobile, для якого генеруємо цю attestazione.
    # У шаблоні:
    #   APP_FOGL  → FOGLIO
    #   APP_PART  → PARTICELLA (у нас це numero)
    #   APP_SUB   → SUB
    #   APP_REND  → RENDITA
    #   APP_SCAT  → SUPERFICIE CATASTALE
    #   APP_SRIP  → SUPERFICIE RIPARAMETRATA (поки що = SCAT)
    #   APP_CAT   → CATEGORIA
    params["{{APP_FOGL}}"] = _to_str(imm.foglio)
    params["{{APP_PART}}"] = _to_str(imm.numero)
    params["{{APP_SUB}}"] = _to_str(imm.sub)
    params["{{APP_REND}}"] = _to_str(imm.rendita)
    params["{{APP_SCAT}}"] = _to_str(imm.superficie_totale)
    # Поки немає формули riparametrata — ставимо таку саму площу
    params["{{APP_SRIP}}"] = _to_str(imm.superficie_totale)
    params["{{APP_CAT}}"] = _to_str(imm.categoria)

    # -----------------------------
    # DATI CATASTALI – рядок TOTALE SUPERFICIE
    # -----------------------------
    # Зараз кожна attestazione генерується для ОДНОГО immobile,
    # тому тотали = значення по цьому ж об'єкту.
    params["{{TOT_SCAT}}"] = _to_str(imm.superficie_totale)
    params["{{TOT_SRIP}}"] = _to_str(imm.superficie_totale)
    params["{{TOT_CAT}}"] = _to_str(imm.categoria)


    # -----------------------------
    # Дані договору (поки що тільки з YAML)
    # -----------------------------
    params["{{CONTRATTO_DATA}}"] = _to_str(adapter.get("contratto_data"))

    # -----------------------------
    # CONDUTTORE (орендар) — теж поки що тільки з YAML
    # -----------------------------
    params["{{CONDUTTORE_NOME}}"] = _to_str(adapter.get("conduttore_nome"))
    params["{{CONDUTTORE_CF}}"] = _to_str(adapter.get("conduttore_cf"))
    params["{{CONDUTTORE_COMUNE}}"] = _to_str(adapter.get("conduttore_comune"))
    params["{{CONDUTTORE_VIA}}"] = _to_str(adapter.get("conduttore_via"))

    # -----------------------------
    # Дані реєстрації
    # -----------------------------
    params["{{DECORRENZA_DATA}}"] = _to_str(adapter.get("decorrenza_data"))
    params["{{REGISTRAZIONE_DATA}}"] = _to_str(adapter.get("registrazione_data"))
    params["{{REGISTRAZIONE_NUM}}"] = _to_str(adapter.get("registrazione_num"))
    params["{{AGENZIA_ENTRATE_SEDE}}"] = _to_str(adapter.get("agenzia_entrate_sede"))

    # -----------------------------
    # A/B/C/D чекбокси — як і раніше
    # -----------------------------
    for key, value in adapter.items():
        if not isinstance(key, str):
            continue
        if not re.fullmatch(r"[abcd][0-9]+", key):
            continue

        v_str = _to_str(value)
        params[f"{{{{{key}}}}}"] = v_str
        params[f"{{{{{key.upper()}}}}}"] = v_str

    return params


# ---------------------------------------------------------------------------
# Основний Scrapy pipeline
# ---------------------------------------------------------------------------

class UppiPipeline:
    """
    Pipeline, який:
    - для item з visura_source='sister' парсить PDF-візуру, зберігає в БД+MinIO;
    - для item з visura_source='db_cache' тягне immobili з БД;
    - генерує DOCX-атестації для вибраних об'єктів.
    """

    template_path: Path

    def __init__(self):
        # Шлях до шаблону DOCX
        self.template_path = (
            Path(__file__).resolve().parents[1]
            / "attestazione_template"
            / "template_attestazione_pescara.docx"
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        cf = adapter.get("locatore_cf") or adapter.get("codice_fiscale")

        if not cf:
            spider.logger.error("[PIPELINE] Item без locatore_cf/codice_fiscale: %r", item)
            return item

        source = adapter.get("visura_source")
        visura_downloaded = bool(adapter.get("visura_downloaded"))
        visura_download_path = adapter.get("visura_download_path")
        force_update = (
            bool(adapter.get("force_update_visura"))
            or bool(adapter.get("FORCE_UPDATE_VISURA"))
            or bool(getattr(spider, "force_update_visura", False))
        )

        spider.logger.info(
            "[PIPELINE] CF=%s, visura_source=%r, downloaded=%s, force_update=%s",
            cf,
            source,
            visura_downloaded,
            force_update,
        )

        immobiles: List[Immobile] = []

        # ------------------------------------------------------------------
        # 1) Item з SISTER: очікуємо, що павук вже скачав PDF
        # ------------------------------------------------------------------
        if source == "sister":
            if not visura_downloaded or not visura_download_path:
                spider.logger.error(
                    "[PIPELINE] CF=%s, visura_source='sister', але немає PDF "
                    "(downloaded=%s, path=%r)",
                    cf,
                    visura_downloaded,
                    visura_download_path,
                )

                # fallback: якщо в БД вже щось є і force_update=False — спробуємо використати БД
                if not force_update and db_has_visura(cf):
                    spider.logger.warning(
                        "[PIPELINE] CF=%s, fallback на БД для immobili через відсутній PDF",
                        cf,
                    )
                    # Спочатку оновлюємо overrides з YAML, потім читаємо з БД
                    upsert_overrides_from_yaml(cf, adapter)
                    immobiles = load_immobiles_from_db(cf)
                else:
                    return item
            else:
                pdf_path = Path(visura_download_path)
                if not pdf_path.exists():
                    spider.logger.error(
                        "[PIPELINE] CF=%s, очікуваний PDF не знайдено: %s",
                        cf,
                        pdf_path,
                    )
                    # fallback на get_visura_path — раптом шляхи не збіглися
                    candidate = get_visura_path(cf)
                    if candidate.exists():
                        spider.logger.warning(
                            "[PIPELINE] CF=%s, використовую fallback шлях PDF: %s",
                            cf,
                            candidate,
                        )
                        pdf_path = candidate
                    else:
                        return item

                parser = VisuraParser()
                try:
                    imm_dicts = parser.parse(str(pdf_path))
                except Exception as e:
                    spider.logger.exception(
                        "[PIPELINE] Помилка парсингу PDF для %s (%s): %s",
                        cf,
                        pdf_path,
                        e,
                    )
                    return item

                if not imm_dicts:
                    spider.logger.warning(
                        "[PIPELINE] Після парсингу PDF для %s не знайдено жодного immobile",
                        cf,
                    )
                    return item

                # Спочатку — сирі immobiles з парсера
                raw_immobiles = [Immobile(**d) for d in imm_dicts]
                spider.logger.info(
                    "[PIPELINE] Розпарсено %d immobili з PDF для %s",
                    len(raw_immobiles),
                    cf,
                )

                # Зберігаємо в БД + MinIO
                try:
                    save_visura(cf, raw_immobiles, pdf_path)
                except Exception:
                    # помилка вже залогована всередині save_visura
                    return item

                # Після збереження — докидуємо overrides з YAML у БД
                upsert_overrides_from_yaml(cf, adapter)

                # І ТІЛЬКИ ТЕПЕР беремо canonical immobiles з БД
                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] Після save_visura/overrides в БД немає immobilі для %s",
                        cf,
                    )
                    return item

        # ------------------------------------------------------------------
        # 2) Item з DB cache: visura_source='db_cache'
        # ------------------------------------------------------------------
        elif source == "db_cache":
            spider.logger.info(
                "[PIPELINE] CF=%s, visura_source='db_cache' → тягнемо immobili з БД",
                cf,
            )

            # Спочатку оновлюємо overrides з YAML (якщо є)
            upsert_overrides_from_yaml(cf, adapter)

            immobiles = load_immobiles_from_db(cf)
            if not immobiles:
                spider.logger.warning(
                    "[PIPELINE] В БД немає immobilі для існуючої візури %s",
                    cf,
                )
                return item

        # ------------------------------------------------------------------
        # 3) Backward-compat: item без visura_source
        # ------------------------------------------------------------------
        else:
            has_in_db = db_has_visura(cf)
            spider.logger.info(
                "[PIPELINE] CF=%s, visura_source is None, has_in_db=%s, force_update=%s",
                cf,
                has_in_db,
                force_update,
            )

            if force_update or not has_in_db:
                # Маємо перерахувати візуру з локального PDF
                pdf_path = get_visura_path(cf)
                if not pdf_path.exists():
                    spider.logger.error(
                        "[PIPELINE] CF=%s, очікуваний PDF (backward) не знайдено: %s",
                        cf,
                        pdf_path,
                    )
                    return item

                parser = VisuraParser()
                try:
                    imm_dicts = parser.parse(str(pdf_path))
                except Exception as e:
                    spider.logger.exception(
                        "[PIPELINE] Помилка парсингу PDF (backward) для %s (%s): %s",
                        cf,
                        pdf_path,
                        e,
                    )
                    return item

                if not imm_dicts:
                    spider.logger.warning(
                        "[PIPELINE] Після парсингу PDF (backward) для %s не знайдено жодного immobile",
                        cf,
                    )
                    return item

                raw_immobiles = [Immobile(**d) for d in imm_dicts]
                spider.logger.info(
                    "[PIPELINE] (backward) Розпарсено %d immobili з PDF для %s",
                    len(raw_immobiles),
                    cf,
                )

                try:
                    save_visura(cf, raw_immobiles, pdf_path)
                except Exception:
                    return item

                # Після збереження — YAML-overrides в БД
                upsert_overrides_from_yaml(cf, adapter)

                # І далі працюємо вже з canonical даними з БД
                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] (backward) Після save_visura/overrides в БД немає immobilі для %s",
                        cf,
                    )
                    return item
            else:
                # Візура вже є в БД, PDF не чіпаємо
                # Але YAML може містити нові overrides → оновлюємо й читаємо
                upsert_overrides_from_yaml(cf, adapter)

                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] (backward) В БД немає immobilі для існуючої візури %s",
                        cf,
                    )
                    return item

        # ------------------------------------------------------------------
        # 4) Фільтрація immobili згідно критеріїв і генерація DOCX
        # ------------------------------------------------------------------
        selected_immobiles = filter_immobiles(immobiles, adapter)
        if not selected_immobiles:
            spider.logger.warning(
                "[PIPELINE] Для %s жоден immobile не пройшов фільтр. Аттестації не створені.",
                cf,
            )
            return item

        for imm in selected_immobiles:
            params = build_params(adapter, imm)
            output_path = get_attestazione_path(cf, imm)
            output_folder = output_path.parent
            output_folder.mkdir(parents=True, exist_ok=True)

            spider.logger.info(
                "[PIPELINE] Генеруємо attestazione для %s → %s", cf, output_path
            )
            try:
                fill_attestazione_template(
                    template_path=str(self.template_path),
                    output_folder=str(output_folder),
                    filename=output_path.name,
                    params=params,
                    underscored=underscored,
                )
            except Exception as e:
                spider.logger.exception(
                    "[PIPELINE] Помилка при генерації attestazione для %s (%s): %s",
                    cf,
                    output_path,
                    e,
                )

        return item
