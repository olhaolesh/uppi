"""Тести для окремого validation layer без зміни поточної runtime-логіки."""

from __future__ import annotations

import pytest

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
        "immobili_document_active_item_missing_identity",
        "immobili_document_item_contains_root_fields",
        "immobili_document_item_not_mapping",
    }


def test_validate_immobili_document_yaml_accepts_db_clearable_and_run_only_clear_markers():
    """Persistable clear markers and run-only clear markers must stay valid policy inputs."""
    result = validate_immobili_document_yaml(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "LOCATORE_COMUNE_RES": "-",
            "LOCATORE_VIA": "-",
            "LOCATORE_CIVICO": "-",
            "immobili": [
                {
                    "FOGLIO": "12",
                    "NUMERO": "345",
                    "SUB": "",
                    "IMMOBILE_COMUNE": "-",
                    "IMMOBILE_VIA": "-",
                    "IMMOBILE_CIVICO": "-",
                    "IMMOBILE_PIANO": "-",
                    "IMMOBILE_INTERNO": "-",
                    "ENERGY_CLASS": "-",
                    "ARREDATO": "-",
                    "ISTAT": "-",
                    "IGNORE_SURCHARGES": "-",
                    "A1": "-",
                    "CONDUTTORE_NOME": "-",
                    "CONDUTTORE_CF": "-",
                    "CONDUTTORE_COMUNE": "-",
                    "CONDUTTORE_VIA": "-",
                    "CONTRATTO_DATA": "-",
                    "DECORRENZA_DATA": "-",
                    "REGISTRAZIONE_DATA": "-",
                    "REGISTRAZIONE_NUM": "-",
                    "AGENZIA_ENTRATE_SEDE": "-",
                    "CANONE_CONTRATTUALE_MENSILE": "-",
                    "DURATA_ANNI": "-",
                    "CUSTOM_EXTRA": "-",
                }
            ],
        }
    )

    assert result.is_valid is True
    assert result.errors == []


@pytest.mark.parametrize(
    ("payload", "expected_code", "expected_field"),
    [
        (
            {"LOCATORE_CF": "-", "immobili": [{"FOGLIO": "12", "NUMERO": "345", "SUB": ""}]},
            "immobili_document_forbidden_clear_root_metadata_field",
            "LOCATORE_CF",
        ),
        (
            {"LOCATORE_CF": "ABCDEF12G34H567I", "COMUNE": "-", "immobili": [{"FOGLIO": "12", "NUMERO": "345", "SUB": ""}]},
            "immobili_document_forbidden_clear_root_metadata_field",
            "COMUNE",
        ),
        (
            {"LOCATORE_CF": "ABCDEF12G34H567I", "immobili": [{"FOGLIO": "-", "NUMERO": "345", "SUB": ""}]},
            "immobili_document_forbidden_clear_identity_field",
            "immobili[0].FOGLIO",
        ),
        (
            {"LOCATORE_CF": "ABCDEF12G34H567I", "immobili": [{"FOGLIO": "12", "NUMERO": "345", "SUB": "-",}]},
            "immobili_document_forbidden_clear_identity_field",
            "immobili[0].SUB",
        ),
        (
            {"LOCATORE_CF": "ABCDEF12G34H567I", "immobili": [{"FOGLIO": "12", "NUMERO": "345", "SUB": "", "VISURA_VIA": "-"}]},
            "immobili_document_forbidden_clear_display_field",
            "immobili[0].VISURA_VIA",
        ),
        (
            {"LOCATORE_CF": "ABCDEF12G34H567I", "immobili": [{"FOGLIO": "12", "NUMERO": "345", "SUB": "", "CONTRACT_KIND": "-"}]},
            "immobili_document_forbidden_clear_non_clearable_persistable_field",
            "immobili[0].CONTRACT_KIND",
        ),
    ],
)
def test_validate_immobili_document_yaml_rejects_forbidden_clear_targets(payload, expected_code, expected_field):
    """Forbidden `-` targets must fail early with precise operator-facing field paths."""
    result = validate_immobili_document_yaml(payload)

    assert result.is_valid is False
    assert any(issue.code == expected_code and issue.field == expected_field for issue in result.errors)


def test_validate_immobili_document_yaml_requires_complete_identity_for_active_records():
    """Active generation records must carry FOGLIO, NUMERO and explicit SUB."""
    result = validate_immobili_document_yaml(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "immobili": [
                {"enabled": True, "NUMERO": "345"},
                {"FOGLIO": "12", "SUB": ""},
                {"FOGLIO": "12", "NUMERO": "345"},
            ],
        }
    )

    assert result.is_valid is False
    assert [issue.code for issue in result.errors].count("immobili_document_active_item_missing_identity") == 4


def test_validate_immobili_document_yaml_disabled_records_do_not_require_identity():
    """Disabled records may stay in the document without strict-match identity fields."""
    result = validate_immobili_document_yaml(
        {
            "LOCATORE_CF": "ABCDEF12G34H567I",
            "immobili": [
                {"enabled": False, "CONDUTTORE_CF": "-"},
            ],
        }
    )

    assert result.is_valid is True
    assert result.errors == []


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
