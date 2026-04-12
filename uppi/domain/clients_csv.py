"""Loads normalized rows from `clients.csv` for future bulk import mode."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from uppi.config.app_config import ClientsCsvSourceConfig
from uppi.config.clients_csv import BulkClientCsvInvalidRow, BulkClientCsvRow, BulkClientsCsvLoadResult

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
    return list(load_clients_csv_with_issues(path=path, source_config=source_config).rows)


def load_clients_csv_with_issues(
    path: Path | None = None,
    *,
    source_config: ClientsCsvSourceConfig | None = None,
) -> BulkClientsCsvLoadResult:
    """Reads `clients.csv` and keeps invalid-row diagnostics for bulk orchestration."""
    resolved_source_config = source_config or default_clients_csv_source_config()
    resolved_path = Path(path) if path is not None else resolved_source_config.clients_file

    if not resolved_path.exists():
        raise FileNotFoundError(f"clients.csv file not found: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        if reader.fieldnames is None:
            return BulkClientsCsvLoadResult(rows=(), invalid_rows=(), total_rows=0)

        rows: list[BulkClientCsvRow] = []
        invalid_rows: list[BulkClientCsvInvalidRow] = []
        total_rows = 0
        for row_number, raw_row in enumerate(reader, start=2):
            total_rows += 1
            parsed = BulkClientCsvRow.parse_raw(raw_row, row_number=row_number)
            if parsed is None:
                continue
            if isinstance(parsed, BulkClientCsvInvalidRow):
                invalid_rows.append(parsed)
                continue
            rows.append(parsed)

    logger.info(
        "[CLIENTS_CSV] Loaded %d valid rows and %d invalid rows from %s",
        len(rows),
        len(invalid_rows),
        resolved_path,
    )
    return BulkClientsCsvLoadResult(
        rows=tuple(rows),
        invalid_rows=tuple(invalid_rows),
        total_rows=total_rows,
    )
