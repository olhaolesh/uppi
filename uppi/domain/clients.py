# uppi/domain/clients.py

from __future__ import annotations

from dataclasses import dataclass, asdict, field, fields
from pathlib import Path
from typing import Any, Dict, List

import logging
import yaml

logger = logging.getLogger(__name__)

# Де лежать YAML-клієнти
CLIENTS_DIR = Path(__file__).resolve().parents[2] / "clients"
CLIENTS_FILE = CLIENTS_DIR / "clients.yml"

# Значення за замовчуванням для запиту в SISTER
DEFAULT_COMUNE = "PESCARA"
DEFAULT_TIPO_CATASTO = "F"
DEFAULT_UFFICIO = "PESCARA Territorio"


@dataclass
class Client:
    """
    Нормалізована модель клієнта.

    Цей клас потрібен:
    - щоб мати центральне місце для всіх полів з YAML;
    - щоб один раз коректно застосувати дефолти;
    - щоб ловити помилки типу "неіснуючий аргумент" на рівні Python
      замість тихого сміття в dict.

    Назовні ми все одно повертаємо dict, щоб павук та map_yaml_to_item
    могли працювати через client["LOCATORE_CF"] / client.get("locatore_cf") і т.д.
    """

    # Єдиний обов’язковий код: і для запиту візури, і для {{LOCATORE_CF}}
    locatore_cf: str

    # Прапорець для примусового оновлення візури
    force_update_visura: bool | None = None

    # Налаштування для пошуку у SISTER із значеннями за замовчуванням
    comune: str = DEFAULT_COMUNE
    tipo_catasto: str = DEFAULT_TIPO_CATASTO

    # У YAML це поле називається UFFICIO_PROVINCIALE_LABEL,
    # у коді ми тримаємо коротше — ufficio_label
    ufficio_label: str = DEFAULT_UFFICIO

    # Дані орендодавця (місце проживання)
    locatore_comune_res: str | None = None
    locatore_via: str | None = None
    locatore_civico: str | None = None

    # Дані про нерухомість
    immobile_comune: str | None = None
    immobile_via: str | None = None
    immobile_civico: str | None = None
    immobile_piano: str | None = None
    immobile_interno: str | None = None
    # характеристики нерухомості
    foglio: str | None = None
    numero: str | None = None
    sub: str | None = None
    rendita: str | None = None
    superficie_totale: str | None = None
    categoria: str | None = None

    # Дані договору
    contratto_data: str | None = None

    # Дані орендаря
    conduttore_nome: str | None = None
    conduttore_cf: str | None = None
    conduttore_comune: str | None = None
    conduttore_via: str | None = None

    # Дані реєстрації
    decorrenza_data: str | None = None
    registrazione_data: str | None = None
    registrazione_num: str | None = None
    agenzia_entrate_sede: str | None = None

    # все інше, що прийшло з YAML, але не описане вище
    extra: Dict[str, Any] = field(default_factory=dict)

    # Елементи типу A/B
    a1: str | None = None
    a2: str | None = None
    b1: str | None = None
    b2: str | None = None
    b3: str | None = None
    b4: str | None = None
    b5: str | None = None

    # Елементи типу C
    c1: str | None = None
    c2: str | None = None
    c3: str | None = None
    c4: str | None = None
    c5: str | None = None
    c6: str | None = None
    c7: str | None = None

    # Елементи типу D
    d1: str | None = None
    d2: str | None = None
    d3: str | None = None
    d4: str | None = None
    d5: str | None = None
    d6: str | None = None
    d7: str | None = None
    d8: str | None = None
    d9: str | None = None
    d10: str | None = None
    d11: str | None = None
    d12: str | None = None
    d13: str | None = None


# Множина всіх snake_case-полів Client, щоб автоматично мапити YAML
_CLIENT_FIELD_NAMES = {f.name for f in fields(Client)}


def _normalize_key(key: str) -> str:
    """
    Нормалізує ключі з YAML:

    - LOCATORE_CF -> locatore_cf
    - UFFICIO_PROVINCIALE_LABEL -> ufficio_provinciale_label
    - FORCE_UPDATE_VISURA -> force_update_visura
    - foglio -> foglio

    Технічно тут достатньо .lower(), бо в YAML підкреслення вже є.
    """
    return key.strip().lower()


def _bool_like(value: Any) -> bool:
    """
    Агресивна нормалізація булевого прапорця з YAML.

    True для:
        - True
        - 1 / "1"
        - "true" / "yes" / "y" / "t" (у будь-якому регістрі)
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "y", "t"}


def _parse_yaml(path: Path) -> List[Dict[str, Any]]:
    """
    Читає YAML, будує Client, але повертає список dict'ів,
    щоб павук і далі працював через client.get("...").

    На виході кожен елемент виглядає приблизно так:

    {
        # верхньорегістрові ключі, зручні для старого коду
        "LOCATORE_CF": "...",
        "COMUNE": "PESCARA",
        "TIPO_CATASTO": "F",
        "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
        "FORCE_UPDATE_VISURA": True/False,

        # snake_case-поля з Client
        "locatore_cf": "...",
        "comune": "PESCARA",
        "tipo_catasto": "F",
        "ufficio_label": "PESCARA Territorio",
        "force_update_visura": True/False,
        "locatore_comune_res": "...",
        ...,
        "a1": "X",
        "d13": "X",

        # будь-які нестандарні поля з YAML
        "SOME_CUSTOM_FIELD": "...",
        ...
    }
    """
    clients: List[Dict[str, Any]] = []

    if not path.exists():
        logger.error("[CLIENTS] Файл clients.yml не знайдено: %s", path)
        return clients

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except Exception as e:
        logger.exception("[CLIENTS] Неможливо прочитати %s: %s", path, e)
        return clients

    if not isinstance(data, list):
        logger.error("[CLIENTS] Очікував список у %s, а отримав %r", path, type(data))
        return clients

    for raw in data:
        if not isinstance(raw, dict):
            logger.warning("[CLIENTS] Пропускаю не-словник у YAML: %r", raw)
            continue

        # CF можемо прийняти як LOCATORE_CF або locatore_cf
        cf = raw.get("LOCATORE_CF") or raw.get("locatore_cf")
        if not cf:
            logger.error("[CLIENTS] Запис без LOCATORE_CF: %r", raw)
            continue

        kwargs: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}

        for k, v in raw.items():
            norm = _normalize_key(k)

            # LOCATORE_CF обробили вже, тут не перезаписуємо
            if norm == "locatore_cf":
                continue

            # special-case: UFFICIO_PROVINCIALE_LABEL -> ufficio_label
            if norm == "ufficio_provinciale_label":
                kwargs["ufficio_label"] = v
                continue

            # force_update* → нормалізуємо в bool
            if norm in {"force_update_visura", "force_update"}:
                kwargs["force_update_visura"] = _bool_like(v)
                continue

            # Всі інші ключі, які збігаються з полями Client, кладемо в kwargs
            if norm in _CLIENT_FIELD_NAMES:
                # напр. "comune", "tipo_catasto", "foglio", "a1", "d13", ...
                kwargs[norm] = v
            else:
                # решту зберігаємо як є (оригінальне імʼя з YAML),
                # потім додамо у фінальний dict
                extra[k] = v

        try:
            client_obj = Client(
                locatore_cf=cf,
                **kwargs,
            )
        except TypeError as e:
            # Ловимо випадки типу "неіснуюче поле в Client"
            logger.error(
                "[CLIENTS] Неможливо створити Client для %s (kwargs=%r): %s",
                cf,
                kwargs,
                e,
            )
            continue
        except Exception as e:
            logger.exception(
                "[CLIENTS] Неочікувана помилка при створенні Client для %s: %s",
                cf,
                e,
            )
            continue

        # asdict дає нам усі snake_case-поля + extra як окремий dict
        client_dict = asdict(client_obj)

        # extra-філд у dataclass тримає дубль; ми виносимо його наверх
        nested_extra = client_dict.pop("extra", {}) or {}
        # 1) додаємо оригінальні поля з YAML, щоб нічого не загубити
        client_dict.update(extra)
        # 2) додаємо те, що було в Client.extra (якщо щось туди потрапило)
        client_dict.update(nested_extra)

        # Гарантовано додаємо верхньорегістрові ключі, які очікує павук
        client_dict["LOCATORE_CF"] = client_obj.locatore_cf
        client_dict["COMUNE"] = client_obj.comune or DEFAULT_COMUNE
        client_dict["TIPO_CATASTO"] = client_obj.tipo_catasto or DEFAULT_TIPO_CATASTO
        client_dict["UFFICIO_PROVINCIALE_LABEL"] = (
            client_obj.ufficio_label or DEFAULT_UFFICIO
        )
        client_dict["FORCE_UPDATE_VISURA"] = bool(client_obj.force_update_visura)

        # І дубль у snake_case, щоб map_yaml_to_item не мучився
        client_dict["locatore_cf"] = client_obj.locatore_cf
        client_dict["comune"] = client_obj.comune or DEFAULT_COMUNE
        client_dict["tipo_catasto"] = client_obj.tipo_catasto or DEFAULT_TIPO_CATASTO
        client_dict["ufficio_label"] = client_obj.ufficio_label or DEFAULT_UFFICIO
        client_dict["force_update_visura"] = bool(client_obj.force_update_visura)

        clients.append(client_dict)

    logger.info("[CLIENTS] Завантажено %d клієнтів із %s", len(clients), path)
    return clients


def load_clients() -> List[Dict[str, Any]]:
    """
    Публічна точка входу: повертає список dict'ів, якими користується павук.

    Гарантії:
    - завжди є:
        * client["LOCATORE_CF"]
        * client["COMUNE"]
        * client["TIPO_CATASTO"]
        * client["UFFICIO_PROVINCIALE_LABEL"]
        * client["FORCE_UPDATE_VISURA"]
    - та їхні snake_case-аналогі:
        * client["locatore_cf"]
        * client["comune"]
        * client["tipo_catasto"]
        * client["ufficio_label"]
        * client["force_update_visura"]
    - усі інші поля з YAML теж збережені.
    """
    return _parse_yaml(CLIENTS_FILE)