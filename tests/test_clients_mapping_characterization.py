"""Characterization-тести для clients mapping і config defaults."""

from __future__ import annotations

import importlib
from pathlib import Path

import uppi.domain.clients as domain_clients
from uppi.utils.item_mapper import map_yaml_to_item


def test_default_clients_file_constant_points_to_repo_clients_yml():
    """Перевіряє сценарій, описаний у назві тесту."""
    expected = Path(domain_clients.__file__).resolve().parents[2] / "clients" / "clients.yml"

    assert domain_clients.CLIENTS_DIR == expected.parent
    assert domain_clients.CLIENTS_FILE == expected


def test_uppi_clients_yaml_env_override_takes_precedence_in_default_clients_source_config(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    override_path = tmp_path / "override-clients.yml"
    monkeypatch.setenv("UPPI_CLIENTS_YAML", str(override_path))
    reloaded = importlib.reload(domain_clients)
    try:
        source_config = reloaded.default_clients_source_config()
        assert source_config.clients_file == override_path
        assert source_config.clients_dir == override_path.parent
    finally:
        monkeypatch.delenv("UPPI_CLIENTS_YAML", raising=False)
        importlib.reload(reloaded)


def test_load_clients_reads_from_module_clients_file_constant_when_patched(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text("- LOCATORE_CF: ABCDEF12G34H567I\n", encoding="utf-8")

    monkeypatch.setenv("UPPI_CLIENTS_YAML", "")
    monkeypatch.setattr(domain_clients, "CLIENTS_FILE", yaml_path)

    rows = domain_clients.load_clients()

    assert len(rows) == 1
    assert rows[0]["LOCATORE_CF"] == "ABCDEF12G34H567I"
    assert rows[0]["locatore_cf"] == "ABCDEF12G34H567I"


def test_load_clients_explicit_path_takes_precedence_over_env_override(monkeypatch, tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    env_yaml = tmp_path / "env-clients.yml"
    explicit_yaml = tmp_path / "explicit-clients.yml"
    env_yaml.write_text("- LOCATORE_CF: ENVOVR12G34H567I\n", encoding="utf-8")
    explicit_yaml.write_text("- LOCATORE_CF: EXPLCT12G34H567I\n", encoding="utf-8")

    monkeypatch.setenv("UPPI_CLIENTS_YAML", str(env_yaml))
    reloaded = importlib.reload(domain_clients)
    try:
        rows = reloaded.load_clients(path=explicit_yaml)
        assert len(rows) == 1
        assert rows[0]["LOCATORE_CF"] == "EXPLCT12G34H567I"
    finally:
        monkeypatch.delenv("UPPI_CLIENTS_YAML", raising=False)
        importlib.reload(reloaded)


def test_parse_yaml_returns_empty_list_for_missing_file(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    rows = domain_clients._parse_yaml(tmp_path / "missing-clients.yml")

    assert rows == []


def test_parse_yaml_returns_empty_list_for_non_list_yaml(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text("LOCATORE_CF: ABCDEF12G34H567I\n", encoding="utf-8")

    rows = domain_clients._parse_yaml(yaml_path)

    assert rows == []


def test_parse_yaml_skips_invalid_entries_and_keeps_valid_ones(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text(
        """
- LOCATORE_CF: ABCDEF12G34H567I
  CUSTOM: ok
- LOCATORE_CF: ""
  CUSTOM: skipped
- just-a-string
""".lstrip(),
        encoding="utf-8",
    )

    rows = domain_clients._parse_yaml(yaml_path)

    assert len(rows) == 1
    assert rows[0]["LOCATORE_CF"] == "ABCDEF12G34H567I"
    assert rows[0]["CUSTOM"] == "ok"


def test_parse_yaml_applies_current_defaults_and_duplicates_uppercase_and_lowercase_keys(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text(
        """
- LOCATORE_CF: ABCDEF12G34H567I
  FORCE_UPDATE_VISURA: yes
""".lstrip(),
        encoding="utf-8",
    )

    rows = domain_clients._parse_yaml(yaml_path)
    row = rows[0]

    assert row["COMUNE"] == "PESCARA"
    assert row["TIPO_CATASTO"] == "F"
    assert row["UFFICIO_PROVINCIALE_LABEL"] == "PESCARA Territorio"
    assert row["FORCE_UPDATE_VISURA"] is True
    assert row["comune"] == "PESCARA"
    assert row["tipo_catasto"] == "F"
    assert row["ufficio_label"] == "PESCARA Territorio"
    assert row["force_update_visura"] is True


def test_parse_yaml_known_current_behavior_extra_fields_are_duplicated_in_top_level_and_extra(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text(
        """
- LOCATORE_CF: ABCDEF12G34H567I
  CUSTOM_FIELD: value
""".lstrip(),
        encoding="utf-8",
    )

    rows = domain_clients._parse_yaml(yaml_path)
    row = rows[0]

    assert row["extra"] == {"CUSTOM_FIELD": "value"}
    assert row["CUSTOM_FIELD"] == "value"


def test_parse_yaml_known_current_behavior_uppercase_none_blocks_lowercase_alias_and_defaults_are_used(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    yaml_path = tmp_path / "clients.yml"
    yaml_path.write_text(
        """
- LOCATORE_CF: ABCDEF12G34H567I
  COMUNE:
  comune: CHIETI
""".lstrip(),
        encoding="utf-8",
    )

    rows = domain_clients._parse_yaml(yaml_path)

    assert rows[0]["COMUNE"] == "PESCARA"
    assert rows[0]["comune"] == "PESCARA"


def test_item_mapper_applies_defaults_only_when_keys_are_missing():
    """Перевіряє сценарій, описаний у назві тесту."""
    mapped = map_yaml_to_item({"LOCATORE_CF": "ABCDEF12G34H567I"})

    assert mapped["locatore_cf"] == "ABCDEF12G34H567I"
    assert mapped["comune"] == "PESCARA"
    assert mapped["tipo_catasto"] == "F"
    assert mapped["ufficio_label"] == "PESCARA Territorio"


def test_item_mapper_known_current_behavior_explicit_empty_or_none_values_block_defaults():
    """Перевіряє сценарій, описаний у назві тесту."""
    mapped_empty = map_yaml_to_item({"COMUNE": ""})
    mapped_none = map_yaml_to_item({"COMUNE": None})

    assert mapped_empty["comune"] == ""
    assert mapped_none["comune"] is None
    assert mapped_empty["tipo_catasto"] == "F"
    assert mapped_none["tipo_catasto"] == "F"


def test_item_mapper_normalizes_known_keys_but_preserves_unknown_keys_in_extra_with_original_shape():
    """Перевіряє сценарій, описаний у назві тесту."""
    mapped = map_yaml_to_item(
        {
            " comune ": "CHIETI",
            "UFFICIO_LABEL": "CHIETI Territorio",
            "CUSTOM FIELD": "value",
        }
    )

    assert mapped["comune"] == "CHIETI"
    assert mapped["ufficio_label"] == "CHIETI Territorio"
    assert mapped["extra"] == {"CUSTOM FIELD": "value"}


def test_item_mapper_force_update_visura_uses_current_string_and_non_string_truthiness_rules():
    """Перевіряє сценарій, описаний у назві тесту."""
    mapped_yes = map_yaml_to_item({"FORCE_UPDATE_VISURA": "sì"})
    mapped_zero = map_yaml_to_item({"FORCE_UPDATE_VISURA": 0})
    mapped_two = map_yaml_to_item({"FORCE_UPDATE_VISURA": 2})

    assert mapped_yes["force_update_visura"] is True
    assert mapped_zero["force_update_visura"] is False
    assert mapped_two["force_update_visura"] is True


def test_item_mapper_known_current_behavior_unknown_field_is_ignored_from_top_level_and_stored_only_in_extra():
    """Перевіряє сценарій, описаний у назві тесту."""
    mapped = map_yaml_to_item({"LOCATORE_CF": "ABCDEF12G34H567I", "CUSTOM": "x"})

    assert mapped["locatore_cf"] == "ABCDEF12G34H567I"
    assert mapped["extra"] == {"CUSTOM": "x"}
    assert "CUSTOM" not in mapped
