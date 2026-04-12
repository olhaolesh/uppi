#!/usr/bin/env python3
"""CLI entry point for prepare-by-CF."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from uppi.domain.exceptions import PrepareInputError, PrepareModeError
from uppi.services.prepare_by_cf import PrepareByCfService


def build_parser() -> argparse.ArgumentParser:
    """Build the narrow CLI contract for single-client prepare."""
    parser = argparse.ArgumentParser(
        description=(
            "Prepare one single-client `immobili.yml` from DB and, when needed, "
            "refresh data through the import-only browser path."
        )
    )
    parser.add_argument(
        "--cf",
        required=True,
        help="Codice Fiscale of the locatore to prepare.",
    )
    parser.add_argument(
        "--force-update-visura",
        action="store_true",
        help="Force the import-only refresh even when prepare sees a DB hit.",
    )
    parser.add_argument(
        "--output",
        help="Optional output path for the generated `immobili.yml` (relative paths resolve from the repo root).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run prepare-by-CF and return a process-friendly exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    service = PrepareByCfService()

    try:
        result = service.prepare(
            args.cf,
            force_update_visura=bool(args.force_update_visura),
            output_path=Path(args.output) if args.output else None,
        )
    except PrepareInputError as exc:
        print(f"Prepare input error: {exc}", file=sys.stderr)
        return 2
    except PrepareModeError as exc:
        print(f"Prepare failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Prepared LOCATORE_CF={result.locatore_cf} -> {result.output_path} "
        f"(decision={result.decision}, import_performed={result.import_performed})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
