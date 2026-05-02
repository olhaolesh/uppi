#!/usr/bin/env python3
"""Validate Stage 10B deployment templates without calling AWS APIs."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


PLACEHOLDER_RE = re.compile(r"\{\{[A-Z0-9_]+\}\}")
ACCOUNT_ID_RE = re.compile(r"\b\d{12}\b")
ACCESS_KEY_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")

REQUIRED_JSON_TEMPLATES = [
    "ecs_task_definition.backend.template.json",
    "iam_policies/ecs_task_execution_policy.template.json",
    "iam_policies/uppi_backend_task_policy.template.json",
]

REQUIRED_TASK_ENV_NAMES = {
    "UPPI_WEB_APP_NAME",
    "UPPI_WEB_APP_VERSION",
    "UPPI_WEB_ENV",
    "DB_PORT",
    "DB_NAME",
    "DB_SSL_MODE",
    "S3_ENDPOINT",
    "VISURE_BUCKET",
    "ATTESTAZIONI_BUCKET",
    "UPPI_LOG_LEVEL",
}

REQUIRED_TASK_SECRET_NAMES = {
    "UPPI_WEB_AUTH_USERNAME",
    "UPPI_WEB_AUTH_PASSWORD",
    "UPPI_WEB_AUTH_PIN",
    "UPPI_WEB_SESSION_SECRET",
    "DB_HOST",
    "DB_USER",
    "DB_PASSWORD",
    "AE_USERNAME",
    "AE_PASSWORD",
    "AE_PIN",
    "TWO_CAPTCHA_API_KEY",
}


def _load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _validate_text(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    if not PLACEHOLDER_RE.search(text):
        errors.append(f"{path}: template has no placeholders")
    if ACCOUNT_ID_RE.search(text):
        errors.append(f"{path}: possible real AWS account id detected")
    if ACCESS_KEY_RE.search(text):
        errors.append(f"{path}: possible real AWS access key detected")
    return errors


def _validate_task_definition(path: Path) -> list[str]:
    errors: list[str] = []
    data = _load_json(path)
    if not isinstance(data, dict):
        return [f"{path}: task definition must be a JSON object"]

    for key in ("family", "requiresCompatibilities", "containerDefinitions", "executionRoleArn", "taskRoleArn"):
        if key not in data:
            errors.append(f"{path}: missing top-level key '{key}'")

    containers = data.get("containerDefinitions")
    if not isinstance(containers, list) or not containers:
        return errors + [f"{path}: containerDefinitions must be a non-empty array"]

    container = containers[0]
    if not isinstance(container, dict):
        return errors + [f"{path}: first container definition must be an object"]

    for key in ("image", "environment", "secrets", "logConfiguration", "healthCheck"):
        if key not in container:
            errors.append(f"{path}: missing container key '{key}'")

    env_names = {item["name"] for item in container.get("environment", []) if isinstance(item, dict) and "name" in item}
    missing_env = sorted(REQUIRED_TASK_ENV_NAMES - env_names)
    if missing_env:
        errors.append(f"{path}: missing expected environment names: {', '.join(missing_env)}")

    secret_names = {item["name"] for item in container.get("secrets", []) if isinstance(item, dict) and "name" in item}
    missing_secrets = sorted(REQUIRED_TASK_SECRET_NAMES - secret_names)
    if missing_secrets:
        errors.append(f"{path}: missing expected secret names: {', '.join(missing_secrets)}")

    health_check = container.get("healthCheck", {})
    command = " ".join(health_check.get("command", [])) if isinstance(health_check, dict) else ""
    if "/health/ready" not in command:
        errors.append(f"{path}: health check does not reference /health/ready")

    log_configuration = container.get("logConfiguration", {})
    if isinstance(log_configuration, dict):
        options = log_configuration.get("options", {})
        if not isinstance(options, dict) or "awslogs-group" not in options or "awslogs-region" not in options:
            errors.append(f"{path}: awslogs-group/awslogs-region options are required")

    return errors


def _validate_policy_template(path: Path) -> list[str]:
    errors: list[str] = []
    data = _load_json(path)
    if not isinstance(data, dict):
        return [f"{path}: IAM policy must be a JSON object"]
    statements = data.get("Statement")
    if not isinstance(statements, list) or not statements:
        errors.append(f"{path}: Statement must be a non-empty array")
    return errors


def main() -> int:
    template_root = Path(__file__).resolve().parents[1]
    errors: list[str] = []

    for relative_path in REQUIRED_JSON_TEMPLATES:
        path = template_root / relative_path
        if not path.exists():
            errors.append(f"{path}: required template file is missing")
            continue
        errors.extend(_validate_text(path))
        if path.name.startswith("ecs_task_definition"):
            errors.extend(_validate_task_definition(path))
        else:
            errors.extend(_validate_policy_template(path))

    if errors:
        print("VALIDATION_FAILED")
        for error in errors:
            print(error)
        return 1

    print("VALIDATION_OK")
    print(f"Validated {len(REQUIRED_JSON_TEMPLATES)} JSON templates under {template_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
