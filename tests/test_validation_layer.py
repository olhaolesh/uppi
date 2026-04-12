"""Тести для окремого validation layer без зміни поточної runtime-логіки."""

from __future__ import annotations

from uppi.config.clients import ClientConfig
from uppi.domain.canone_models import CanoneInput, ContractKind
from uppi.services.validation import (
    validate_canone_input,
    validate_client_config,
    validate_client_yaml_record,
    validate_immobili_document_yaml,
    validate_parsed_visura_output,
)


def test_validate_client_yaml_record_accepts_valid_mapping():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_client_yaml_record({"LOCATORE_CF": "ABCDEF12G34H567I"})

    assert result.is_valid is True
    assert result.errors == []
    assert result.warnings == []


def test_validate_client_yaml_record_flags_missing_locatore_cf_as_structural_error():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_client_yaml_record({"COMUNE": "PESCARA"})

    assert result.is_valid is False
    assert [issue.code for issue in result.errors] == ["yaml_missing_locatore_cf"]


def test_validate_immobili_document_yaml_accepts_valid_single_client_shape():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_immobili_document_yaml(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "COMUNE": "PESCARA",
            "immobili": [
                {"FOGLIO": "12", "NUMERO": "345", "SUB": "7"},
            ],
        }
    )

    assert result.is_valid is True
    assert result.errors == []


def test_validate_immobili_document_yaml_flags_root_vs_immobile_shape_mistakes():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_immobili_document_yaml(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "FOGLIO": "12",
            "immobili": [
                {"LOCATORE_VIA": "Via Roma", "NUMERO": "345"},
                "not-a-mapping",
            ],
        }
    )

    assert result.is_valid is False
    assert {issue.code for issue in result.errors} == {
        "immobili_document_root_contains_immobile_fields",
        "immobili_document_item_contains_root_fields",
        "immobili_document_item_not_mapping",
    }


def test_validate_client_config_warning_first_for_questionable_but_tolerated_values():
    """Перевіряє сценарій, описаний у назві тесту."""
    client_cfg = ClientConfig(
        locatore_cf="ABCDEF12G34H567I",
        comune="PESCARA",
        tipo_catasto="F",
        ufficio_label="PESCARA Territorio",
        force_update_visura=False,
        contract_kind="SOMETHING_ELSE",
        foglio="12",
        durata_anni="abc",
        istat=-1.0,
        energy_class="Z",
        canone_contrattuale_mensile="not-a-number",
    )

    result = validate_client_config(client_cfg)

    assert result.is_valid is True
    assert {issue.code for issue in result.warnings} == {
        "yaml_unknown_contract_kind",
        "yaml_partial_cadastral_identity",
        "yaml_negative_istat",
        "yaml_non_integer_durata",
        "yaml_unexpected_energy_class",
        "yaml_non_numeric_canone",
    }


def test_validate_parsed_visura_output_accepts_current_happy_path_shape():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_parsed_visura_output(
        [
            {
                "foglio": "12",
                "numero": "345",
                "immobile_comune": "PESCARA",
                "via_name": "Via Roma",
            }
        ]
    )

    assert result.is_valid is True
    assert result.errors == []
    assert result.warnings == []


def test_validate_parsed_visura_output_hard_fails_non_list_structure():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_parsed_visura_output({"foglio": "12"})

    assert result.is_valid is False
    assert [issue.code for issue in result.errors] == ["parser_output_not_list"]


def test_validate_parsed_visura_output_warning_first_for_partial_item_shape():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_parsed_visura_output([{"immobile_comune": None, "via_name": None, "indirizzo_raw": None}])

    assert result.is_valid is True
    assert {issue.code for issue in result.warnings} == {
        "parser_missing_cadastral_identity",
        "parser_missing_immobile_comune",
        "parser_missing_address_text",
    }


def test_validate_canone_input_accepts_valid_current_shape():
    """Перевіряє сценарій, описаний у назві тесту."""
    canone_input = CanoneInput(
        superficie_catastale=85.0,
        micro_zona="5",
        foglio="12",
        categoria_catasto="A/2",
        classe_catasto="3",
        count_a=1,
        count_b=0,
        count_c=0,
        count_d=0,
        arredato=0.0,
        energy_class="B",
        contract_kind=ContractKind.CONCORDATO,
        durata_anni=3,
        istat=5.0,
        ignore_surcharges=False,
    )

    result = validate_canone_input(canone_input)

    assert result.is_valid is True
    assert result.warnings == []


def test_validate_canone_input_warning_first_for_obviously_suspicious_values():
    """Перевіряє сценарій, описаний у назві тесту."""
    canone_input = CanoneInput(
        superficie_catastale=0.0,
        micro_zona=None,
        foglio=None,
        categoria_catasto=None,
        classe_catasto=None,
        count_a=0,
        count_b=0,
        count_c=0,
        count_d=0,
        arredato=0.0,
        energy_class=None,
        contract_kind=ContractKind.CONCORDATO,
        durata_anni=0,
        istat=-2.0,
        ignore_surcharges=False,
    )

    result = validate_canone_input(canone_input)

    assert result.is_valid is True
    assert {issue.code for issue in result.warnings} == {
        "canone_non_positive_surface",
        "canone_missing_location_markers",
        "canone_missing_categoria",
        "canone_missing_classe",
        "canone_non_positive_durata",
        "canone_negative_istat",
    }


def test_validate_canone_input_hard_fails_wrong_object_type():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_canone_input({"superficie_catastale": 10})

    assert result.is_valid is False
    assert [issue.code for issue in result.errors] == ["canone_input_wrong_type"]
