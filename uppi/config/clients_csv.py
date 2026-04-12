"""Typed models for the future bulk `clients.csv` input surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


def _normalize_header(header: str | None) -> str:
    """Normalizes CSV headers to an uppercase canonical shape."""
    return str(header or "").strip().upper()


def _normalize_cell(value: str | None) -> str:
    """Trims CSV cell values and collapses `None` to an empty string."""
    return str(value or "").strip()


@dataclass(frozen=True)
class BulkClientCsvRow:
    """One normalized row from `clients.csv` for future bulk import orchestration."""

    row_number: int
    locatore_cf: str
    values: dict[str, str]
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: Mapping[str, str | None], *, row_number: int) -> "BulkClientCsvRow | None":
        """Builds a normalized CSV row or returns `None` for blank lines."""
        values = {
            _normalize_header(header): _normalize_cell(value)
            for header, value in raw.items()
            if header is not None
        }

        if not any(values.values()):
            return None

        locatore_cf = values.get("LOCATORE_CF") or values.get("CODICE_FISCALE") or ""
        if not locatore_cf:
            raise ValueError(f"clients.csv row {row_number} is missing LOCATORE_CF")

        extra = {
            header: value
            for header, value in values.items()
            if header not in {"LOCATORE_CF", "CODICE_FISCALE"}
        }

        return cls(
            row_number=row_number,
            locatore_cf=locatore_cf,
            values=values,
            extra=extra,
        )
