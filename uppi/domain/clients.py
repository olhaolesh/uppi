"""Читає clients.yml і підтримує explicit path, env override та default fallback."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml

from uppi.config.app_config import ClientsSourceConfig
from uppi.config.clients import ClientConfig
from uppi.domain.exceptions import YamlInputValidationError
from uppi.services.validation import (
    emit_validation_messages,
    validate_client_config,
    validate_client_yaml_record,
)

logger = logging.getLogger(__name__)

CLIENTS_DIR = Path(__file__).resolve().parents[2] / "clients"
CLIENTS_FILE = CLIENTS_DIR / "clients.yml"

DEFAULT_COMUNE = "PESCARA"
DEFAULT_TIPO_CATASTO = "F"
DEFAULT_UFFICIO = "PESCARA Territorio"


def default_clients_source_config() -> ClientsSourceConfig:
    """Повертає active clients-source з env override або canonical default path."""
    return ClientsSourceConfig.from_env(
        repo_root=CLIENTS_DIR.parent,
        default_clients_file=CLIENTS_FILE,
        default_comune=DEFAULT_COMUNE,
        default_tipo_catasto=DEFAULT_TIPO_CATASTO,
        default_ufficio_label=DEFAULT_UFFICIO,
    )


def _parse_yaml(path: Path, *, source_config: ClientsSourceConfig | None = None) -> List[Dict[str, Any]]:
    """Завантажує YAML-файл клієнтів і повертає список нормалізованих записів."""
    clients: List[Dict[str, Any]] = []
    resolved_source_config = source_config or default_clients_source_config()

    if not path.exists():
        logger.error("[CLIENTS] Файл clients.yml не знайдено: %s", path)
        return clients

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except Exception as e:
        logger.exception("[CLIENTS] Неможливо прочитати %s: %s", path, e)
        return clients

    if not isinstance(data, list):
        logger.error("[CLIENTS] Очікував список у %s, а отримав %r", path, type(data))
        return clients

    for raw in data:
        try:
            raw_validation = validate_client_yaml_record(raw)
            emit_validation_messages(
                logger,
                "[CLIENTS][VALIDATION]",
                raw_validation,
                emit_errors=False,
            )
            if not raw_validation.is_valid:
                raise YamlInputValidationError.from_validation_result(
                    raw_validation,
                    fallback_message="Некоректний YAML record.",
                )

            client_cfg = ClientConfig.from_raw(
                raw,
                default_comune=resolved_source_config.default_comune,
                default_tipo_catasto=resolved_source_config.default_tipo_catasto,
                default_ufficio_label=resolved_source_config.default_ufficio_label,
            )
        except ValueError as e:
            logger.error(
                "[CLIENTS] %s",
                YamlInputValidationError(str(e), details={"source": "ClientConfig.from_raw"}),
            )
            continue
        except YamlInputValidationError as e:
            logger.error("[CLIENTS] %s", e)
            continue
        except Exception as e:
            logger.exception("[CLIENTS] Неочікувана помилка при читанні YAML: %s", e)
            continue

        emit_validation_messages(
            logger,
            "[CLIENTS][VALIDATION]",
            validate_client_config(client_cfg),
        )

        client_dict = client_cfg.to_item_dict()

        client_dict.update(client_cfg.extra)

        client_dict["LOCATORE_CF"] = client_cfg.locatore_cf
        client_dict["COMUNE"] = client_cfg.comune
        client_dict["TIPO_CATASTO"] = client_cfg.tipo_catasto
        client_dict["UFFICIO_PROVINCIALE_LABEL"] = client_cfg.ufficio_label
        client_dict["FORCE_UPDATE_VISURA"] = bool(client_cfg.force_update_visura)

        client_dict["locatore_cf"] = client_cfg.locatore_cf
        client_dict["comune"] = client_cfg.comune
        client_dict["tipo_catasto"] = client_cfg.tipo_catasto
        client_dict["ufficio_label"] = client_cfg.ufficio_label
        client_dict["force_update_visura"] = bool(client_cfg.force_update_visura)

        clients.append(client_dict)

    logger.info("[CLIENTS] Завантажено %d клієнтів із %s", len(clients), path)
    return clients


def load_clients(
    path: Path | None = None,
    *,
    source_config: ClientsSourceConfig | None = None,
) -> List[Dict[str, Any]]:
    """
    Завантажує клієнтів із explicit path, active source_config або default fallback.

    Precedence order:
    1. explicit `path`
    2. `source_config.clients_file`
    3. current default source with `UPPI_CLIENTS_YAML` override support
    """
    resolved_source_config = source_config or default_clients_source_config()
    resolved_path = Path(path) if path is not None else resolved_source_config.clients_file
    return _parse_yaml(resolved_path, source_config=resolved_source_config)
