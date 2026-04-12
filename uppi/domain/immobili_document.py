"""Loads the canonical single-client `immobili.yml` document."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from uppi.config.app_config import ImmobiliYamlSourceConfig
from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.domain.exceptions import YamlInputValidationError
from uppi.services.validation import validate_immobili_document_yaml

logger = logging.getLogger(__name__)

IMMOBILI_DIR = Path(__file__).resolve().parents[2] / "clients"
IMMOBILI_FILE = IMMOBILI_DIR / "immobili.yml"


def default_immobili_source_config() -> ImmobiliYamlSourceConfig:
    """Returns the canonical generation input source, driven by `UPPI_IMMOBILI_YAML`."""
    return ImmobiliYamlSourceConfig.from_env(
        repo_root=IMMOBILI_DIR.parent,
        default_immobili_file=IMMOBILI_FILE,
    )


def _parse_document(
    path: Path,
    *,
    source_config: ImmobiliYamlSourceConfig | None = None,
) -> ImmobiliDocumentConfig:
    """Reads, validates and normalizes one single-client immobili document."""
    resolved_source_config = source_config or default_immobili_source_config()
    resolved_path = Path(path or resolved_source_config.immobili_file)

    if not resolved_path.exists():
        raise FileNotFoundError(f"Immobili YAML file not found: {resolved_path}")

    try:
        with resolved_path.open("r", encoding="utf-8") as file_obj:
            raw_data = yaml.safe_load(file_obj) or {}
    except Exception as exc:
        raise YamlInputValidationError(
            f"Unable to read immobili YAML from {resolved_path}",
            details={"path": str(resolved_path)},
        ) from exc

    validation_result = validate_immobili_document_yaml(raw_data)
    if not validation_result.is_valid:
        raise YamlInputValidationError.from_validation_result(
            validation_result,
            fallback_message="Invalid immobili.yml document shape.",
        )

    document = ImmobiliDocumentConfig.from_raw(raw_data)
    logger.info("[IMMOBILI] Loaded %d immobili from %s", len(document.immobili), resolved_path)
    return document


def load_immobili_document(
    path: Path | None = None,
    *,
    source_config: ImmobiliYamlSourceConfig | None = None,
) -> ImmobiliDocumentConfig:
    """Loads the canonical generation input with explicit-path precedence."""
    resolved_source_config = source_config or default_immobili_source_config()
    resolved_path = Path(path) if path is not None else resolved_source_config.immobili_file
    return _parse_document(resolved_path, source_config=resolved_source_config)
