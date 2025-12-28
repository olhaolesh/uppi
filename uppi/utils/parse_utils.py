# uppi/utils/parse_utils.py
from __future__ import annotations

import re
from datetime import date
from typing import Any, Optional


def clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).replace("\n", " ").strip()
    return s if s != "" else None

def clean_sub(v: Any) -> str:
    """sub з visura в БД у нас канонічно не NULL. None/порожнє -> ''."""
    if v is None:
        return ""
    s = str(v).replace("\n", " ").strip()
    return s  # може бути ''

def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "":
        return None
    # кома як десятковий роздільник
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def to_bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


def parse_date(v: Any) -> Optional[date]:
    """
    Підтримка:
      - YYYY-MM-DD
      - DD/MM/YYYY
    """
    s = clean_str(v)
    if not s:
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        try:
            d, m, y = s.split("/")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    return None
