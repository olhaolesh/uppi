"""Базові pure-function правила для current smart patch semantics."""

from __future__ import annotations

from typing import Any


def resolve_patch_value(yaml_val: Any, db_val: Any, default: Any = None) -> Any:
    """Повертає нове, старе або explicit-delete значення для поточного patch контракту."""
    s_val = str(yaml_val).strip() if yaml_val is not None else ""

    if s_val == "-":
        return default

    if s_val:
        return yaml_val

    return db_val
