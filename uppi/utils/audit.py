# uppi/utils/audit.py
from __future__ import annotations

from datetime import time
import hashlib
import json
import logging
from pathlib import Path
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


def mask_username(login: str) -> str:
    """
    Маскуємо логін, щоб не зливати секрети.
    """
    if not login:
        return "unknown"
    s = str(login).strip()
    if not s:
        return "unknown"
    if "@" in s:
        local, domain = s.split("@", 1)
        head = local[:2]
        return f"{head}***@{domain}"
    return f"{s[:2]}***"


def sha256_text(text: str) -> Optional[str]:
    if not text:
        return None
    s = str(text)
    if not s:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    """
    SHA256 для файлу (наприклад, PDF).
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_json_dumps(obj: Any) -> str:
    """
    Стабільний JSON для snapshot/хешів.
    """
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------
# person name formatting
# ------------------------------------------------------------

_ROMAN_NUMERALS = {
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
    "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX",
}

def _smart_title_token(token: str) -> str:
    """
    Title-case для одного токена, з підтримкою:
    - апострофів: D'AMICO -> D'Amico
    - дефісів: ANNA-MARIA -> Anna-Maria
    - римських цифр: II -> II
    """
    t = token.strip()
    if not t:
        return ""

    # Римські цифри лишаємо як є
    if t.upper() in _ROMAN_NUMERALS:
        return t.upper()

    # Обробка дефісів
    hy_parts = t.split("-")
    out_hy_parts = []
    for hp in hy_parts:
        hp = hp.strip()
        if not hp:
            continue

        # Обробка апострофів
        if "'" in hp:
            ap_parts = hp.split("'")
            ap_out = []
            for ap in ap_parts:
                ap = ap.strip()
                if not ap:
                    ap_out.append("")
                    continue
                ap_out.append(ap[:1].upper() + ap[1:].lower())
            out_hy_parts.append("'".join(ap_out))
        else:
            out_hy_parts.append(hp[:1].upper() + hp[1:].lower())

    return "-".join(out_hy_parts)


def smart_title(text: Optional[str]) -> str:
    """
    Нормалізує рядок у "кожне слово з великої":
    - прибирає зайві пробіли
    - підтримує D'AMICO / ANNA-MARIA
    """
    if text is None:
        return ""
    s = str(text).strip()
    if not s:
        return ""

    # Зводимо множинні пробіли
    s = re.sub(r"\s+", " ", s)

    tokens = s.split(" ")
    out = [_smart_title_token(tok) for tok in tokens if tok != ""]
    return " ".join(out)


def format_person_fullname(name: Optional[str], surname: Optional[str]) -> str:
    """
    Склеює ім'я + прізвище у людський вигляд.

    Повертає:
      "Mariarita Cecamore"
    """
    n = smart_title(name)
    sn = smart_title(surname)

    if n and sn:
        return f"{n} {sn}"
    if n:
        return n
    if sn:
        return sn
    return ""

# ------------------------------------------------------------
# END: person name formatting
# ------------------------------------------------------------

# ------------------------------------------------------------
# Видалення .pdf файлу visure після завантаження
# ------------------------------------------------------------
def safe_unlink(path: Path, retries: int = 3, delay_sec: float = 0.2) -> bool:
    """
    Безпечно видаляє файл (з ретраями), щоб пережити ситуації типу "файл ще зайнятий".
    Повертає True якщо видалено або файлу вже нема.
    """
    try:
        path = Path(path)
    except Exception:
        logger.warning("[FS] safe_unlink got non-path: %r", path)
        return False

    for i in range(retries):
        try:
            if not path.exists():
                logger.info("[FS] File already absent: %s", path)
                return True
            path.unlink()
            logger.info("[FS] Deleted local file: %s", path)
            return True
        except Exception as e:
            logger.warning("[FS] Cannot delete %s (attempt %d/%d): %s", path, i + 1, retries, e)
            time.sleep(delay_sec)

    return False
# ------------------------------------------------------------
# END: Видалення .pdf файлу visure після завантаження
# ------------------------------------------------------------