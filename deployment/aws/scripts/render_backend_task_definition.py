#!/usr/bin/env python3
"""Render ECS task definition JSON from a placeholder template and local values file."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


EXACT_PLACEHOLDER_RE = re.compile(r"^\{\{([A-Z0-9_]+)\}\}$")
INLINE_PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _replace_string(value: str, mapping: dict[str, Any]) -> Any:
    exact_match = EXACT_PLACEHOLDER_RE.fullmatch(value)
    if exact_match:
        key = exact_match.group(1)
        if key not in mapping:
            raise KeyError(f"Missing placeholder value for {key}")
        return mapping[key]

    def replace_inline(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in mapping:
            raise KeyError(f"Missing placeholder value for {key}")
        return str(mapping[key])

    return INLINE_PLACEHOLDER_RE.sub(replace_inline, value)


def _render(value: Any, mapping: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return _replace_string(value, mapping)
    if isinstance(value, list):
        return [_render(item, mapping) for item in value]
    if isinstance(value, dict):
        return {key: _render(item, mapping) for key, item in value.items()}
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template",
        default=str(Path(__file__).resolve().parents[1] / "ecs_task_definition.backend.template.json"),
        help="Path to ECS task definition template JSON.",
    )
    parser.add_argument(
        "--values-file",
        required=True,
        help="Path to local JSON file with placeholder values.",
    )
    parser.add_argument(
        "--output",
        help="Optional output file path. Defaults to stdout.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    template_path = Path(args.template).resolve()
    values_path = Path(args.values_file).resolve()

    template_data = _load_json(template_path)
    values = _load_json(values_path)

    if not isinstance(values, dict):
        print("Values file must contain a JSON object.", file=sys.stderr)
        return 1

    try:
        rendered = _render(template_data, values)
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    output = json.dumps(rendered, ensure_ascii=True, indent=2) + "\n"
    if args.output:
        output_path = Path(args.output).resolve()
        output_path.write_text(output, encoding="utf-8")
    else:
        sys.stdout.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
