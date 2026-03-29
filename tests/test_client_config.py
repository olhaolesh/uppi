"""Тести для current contract нормалізації ClientConfig."""

import pytest

from uppi.config.clients import ClientConfig


def test_client_config_from_raw_basic():
    """Перевіряє сценарій, описаний у назві тесту."""
    raw = {
        "LOCATORE_CF": "ABCDEF12G34H567I",
        "COMUNE": "PESCARA",
        "TIPO_CATASTO": "F",
        "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
        "FORCE_UPDATE_VISURA": "true",
        "IMMOBILE_VIA": "Via Roma",
        "A1": "X",
        "CUSTOM_FIELD": "value",
    }

    cfg = ClientConfig.from_raw(
        raw,
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    assert cfg.locatore_cf == "ABCDEF12G34H567I"
    assert cfg.force_update_visura is True
    assert cfg.immobile_via == "Via Roma"
    assert cfg.elements["a1"] == "X"
    assert cfg.extra["CUSTOM_FIELD"] == "value"

    item = cfg.to_item_dict()
    assert item["locatore_cf"] == "ABCDEF12G34H567I"
    assert item["immobile_via"] == "Via Roma"
    assert item["a1"] == "X"


def test_client_config_defaults_are_used_when_core_location_fields_are_missing():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = ClientConfig.from_raw(
        {"LOCATORE_CF": "ABCDEF12G34H567I"},
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    assert cfg.comune == "PESCARA"
    assert cfg.tipo_catasto == "F"
    assert cfg.ufficio_label == "PESCARA Territorio"
    assert cfg.force_update_visura is False


def test_client_config_known_current_behavior_uppercase_none_for_comune_ignores_lowercase_alias_and_uses_default():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = ClientConfig.from_raw(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "COMUNE": None,
            "comune": "CHIETI",
        },
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    assert cfg.comune == "PESCARA"


def test_client_config_known_current_behavior_whitespace_locatore_cf_blocks_lowercase_alias_and_raises():
    """Перевіряє сценарій, описаний у назві тесту."""
    with pytest.raises(ValueError, match="LOCATORE_CF is required"):
        ClientConfig.from_raw(
            {
                "LOCATORE_CF": "   ",
                "locatore_cf": "ABCDEF12G34H567I",
            },
            default_comune="PESCARA",
            default_tipo_catasto="F",
            default_ufficio_label="PESCARA Territorio",
        )


def test_client_config_known_current_behavior_whitespace_istat_raises_value_error():
    """Перевіряє сценарій, описаний у назві тесту."""
    with pytest.raises(ValueError):
        ClientConfig.from_raw(
            {
                "LOCATORE_CF": "ABCDEF12G34H567I",
                "ISTAT": "   ",
            },
            default_comune="PESCARA",
            default_tipo_catasto="F",
            default_ufficio_label="PESCARA Territorio",
        )


def test_client_config_known_current_behavior_element_key_with_surrounding_spaces_is_preserved_verbatim_in_elements():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = ClientConfig.from_raw(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            " A1 ": "X",
        },
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    assert cfg.elements == {" a1 ": "X"}
    assert cfg.to_item_dict()[" a1 "] == "X"


def test_client_config_to_item_dict_keeps_unknown_fields_nested_in_extra_only():
    """Перевіряє сценарій, описаний у назві тесту."""
    cfg = ClientConfig.from_raw(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "CUSTOM_FIELD": "value",
        },
        default_comune="PESCARA",
        default_tipo_catasto="F",
        default_ufficio_label="PESCARA Territorio",
    )

    item = cfg.to_item_dict()

    assert item["extra"] == {"CUSTOM_FIELD": "value"}
    assert "CUSTOM_FIELD" not in item
