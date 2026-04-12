#!/usr/bin/env python3
"""CLI entry point for bulk CSV import-only mode."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from uppi.domain.exceptions import BulkImportModeError
from uppi.services.bulk_import_clients_csv import BulkImportClientsCsvService, format_bulk_import_summary


def build_parser() -> argparse.ArgumentParser:
    """Build the narrow CLI contract for bulk CSV import-only mode."""
    parser = argparse.ArgumentParser(
        description=(
            "Run the import-only boundary for each Codice Fiscale found in clients.csv. "
            "This mode updates DB state only and never generates `immobili.yml`."
        )
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to the bulk clients.csv input file.",
    )
    parser.add_argument(
        "--force-update-visura",
        action="store_true",
        help="Force visura refresh for every imported CF.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop after the first import-only failure instead of continuing.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run bulk CSV import-only mode and return a process-friendly exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    service = BulkImportClientsCsvService()

    try:
        result = service.run(
            Path(args.csv),
            force_update_visura=bool(args.force_update_visura),
            fail_fast=bool(args.fail_fast),
        )
    except BulkImportModeError as exc:
        print(f"Bulk import failed: {exc}", file=sys.stderr)
        return 1

    print(format_bulk_import_summary(result))
    return 1 if result.has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
