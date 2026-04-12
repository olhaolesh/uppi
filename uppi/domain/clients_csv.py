"""Loads normalized rows from `clients.csv` for future bulk import mode."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from uppi.config.app_config import ClientsCsvSourceConfig
from uppi.config.clients_csv import BulkClientCsvRow

logger = logging.getLogger(__name__)

CLIENTS_CSV_DIR = Path(__file__).resolve().parents[2] / "clients"
CLIENTS_CSV_FILE = CLIENTS_CSV_DIR / "clients.csv"


def default_clients_csv_source_config() -> ClientsCsvSourceConfig:
    """Returns the default source abstraction for bulk CSV input."""
    return ClientsCsvSourceConfig.from_env(
        repo_root=CLIENTS_CSV_DIR.parent,
        default_clients_file=CLIENTS_CSV_FILE,
    )


def load_clients_csv(
    path: Path | None = None,
    *,
    source_config: ClientsCsvSourceConfig | None = None,
) -> list[BulkClientCsvRow]:
    """Reads and normalizes `clients.csv` without attaching generation semantics."""
    resolved_source_config = source_config or default_clients_csv_source_config()
    resolved_path = Path(path) if path is not None else resolved_source_config.clients_file

    if not resolved_path.exists():
        raise FileNotFoundError(f"clients.csv file not found: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        if reader.fieldnames is None:
            return []

        rows: list[BulkClientCsvRow] = []
        for row_number, raw_row in enumerate(reader, start=2):
            row = BulkClientCsvRow.from_raw(raw_row, row_number=row_number)
            if row is not None:
                rows.append(row)

    logger.info("[CLIENTS_CSV] Loaded %d rows from %s", len(rows), resolved_path)
    return rows
