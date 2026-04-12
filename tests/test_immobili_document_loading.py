"""Focused tests for the canonical single-client `immobili.yml` loading surface."""

from __future__ import annotations

import importlib

import pytest
import yaml

import uppi.domain.immobili_document as immobili_document_module
from uppi.domain.exceptions import YamlInputValidationError


def test_load_immobili_document_reads_valid_single_client_yaml(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "immobili.yml"
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "LOCATORE_CF": "RSSMRA80A01H501Z",
                "COMUNE": "PESCARA",
                "TIPO_CATASTO": "F",
                "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
                "LOCATORE_VIA": "Via Roma",
                "immobili": [
                    {
                        "FOGLIO": "12",
                        "NUMERO": "345",
                        "SUB": "7",
                        "ENERGY_CLASS": "B",
                        "A1": "X",
                        "CUSTOM_FIELD": "kept-in-extra",
                    },
                    {
                        "enabled": False,
                        "FOGLIO": "13",
                        "NUMERO": "99",
                        "SUB": "1",
                    },
                ],
                "ROOT_EXTRA": "kept-too",
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    document = immobili_document_module.load_immobili_document(path=yaml_path)

    assert document.locatore_cf == "RSSMRA80A01H501Z"
    assert document.comune == "PESCARA"
    assert document.tipo_catasto == "F"
    assert document.ufficio_label == "PESCARA Territorio"
    assert document.locatore_via == "Via Roma"
    assert len(document.immobili) == 2
    assert document.immobili[0].foglio == "12"
    assert document.immobili[0].energy_class == "B"
    assert document.immobili[0].elements == {"a1": "X"}
    assert document.immobili[0].extra == {"CUSTOM_FIELD": "kept-in-extra"}
    assert document.immobili[1].enabled is False
    assert document.extra == {"ROOT_EXTRA": "kept-too"}


def test_load_immobili_document_rejects_legacy_flat_list_shape(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "immobili.yml"
    yaml_path.write_text(
        yaml.safe_dump(
            [{"LOCATORE_CF": "RSSMRA80A01H501Z", "FOGLIO": "12"}],
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(YamlInputValidationError) as exc_info:
        immobili_document_module.load_immobili_document(path=yaml_path)

    assert exc_info.value.details["error_codes"] == ["immobili_document_not_mapping"]


def test_uppi_immobili_yaml_env_override_controls_canonical_generation_source(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    override_path = tmp_path / "override-immobili.yml"
    monkeypatch.setenv("UPPI_IMMOBILI_YAML", str(override_path))

    reloaded = importlib.reload(immobili_document_module)
    try:
        source_config = reloaded.default_immobili_source_config()
        assert source_config.immobili_file == override_path
        assert source_config.immobili_dir == override_path.parent
    finally:
        monkeypatch.delenv("UPPI_IMMOBILI_YAML", raising=False)
        importlib.reload(reloaded)


def test_legacy_clients_env_override_does_not_become_canonical_immobili_source(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    legacy_override = tmp_path / "legacy-clients.yml"
    monkeypatch.delenv("UPPI_IMMOBILI_YAML", raising=False)
    monkeypatch.setenv("UPPI_CLIENTS_YAML", str(legacy_override))

    reloaded = importlib.reload(immobili_document_module)
    try:
        source_config = reloaded.default_immobili_source_config()
        assert source_config.immobili_file.name == "immobili.yml"
        assert source_config.immobili_file != legacy_override
    finally:
        monkeypatch.delenv("UPPI_CLIENTS_YAML", raising=False)
        importlib.reload(reloaded)
