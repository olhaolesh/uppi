"""Тести для typed domain exceptions і recoverable/non-recoverable класифікації."""

from __future__ import annotations

from uppi.domain.exceptions import (
    CanoneInputValidationError,
    NonRecoverableDomainError,
    ParsedVisuraValidationError,
    RecoverableDomainError,
    YamlInputValidationError,
)
from uppi.services.validation import (
    validate_canone_input,
    validate_client_yaml_record,
    validate_parsed_visura_output,
)


def test_yaml_input_validation_error_is_recoverable():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_client_yaml_record({"COMUNE": "PESCARA"})
    error = YamlInputValidationError.from_validation_result(
        result,
        fallback_message="Некоректний YAML record.",
    )

    assert isinstance(error, RecoverableDomainError)
    assert error.recoverable is True
    assert error.code == "yaml_input_validation_error"
    assert error.details["error_codes"] == ["yaml_missing_locatore_cf"]


def test_parsed_visura_validation_error_is_non_recoverable():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_parsed_visura_output({"foglio": "12"})
    error = ParsedVisuraValidationError.from_validation_result(
        result,
        fallback_message="Структурно некоректний parser output.",
    )

    assert isinstance(error, NonRecoverableDomainError)
    assert error.recoverable is False
    assert error.code == "parsed_visura_validation_error"
    assert error.details["error_codes"] == ["parser_output_not_list"]


def test_canone_input_validation_error_is_recoverable():
    """Перевіряє сценарій, описаний у назві тесту."""
    result = validate_canone_input({"superficie_catastale": 10})
    error = CanoneInputValidationError.from_validation_result(
        result,
        fallback_message="Структурно некоректний canone input.",
    )

    assert isinstance(error, RecoverableDomainError)
    assert error.recoverable is True
    assert error.code == "canone_input_validation_error"
    assert error.details["error_codes"] == ["canone_input_wrong_type"]


def test_warning_first_validation_cases_do_not_create_structural_errors():
    """Перевіряє сценарій, описаний у назві тесту."""
    parser_result = validate_parsed_visura_output([{"immobile_comune": None, "via_name": None, "indirizzo_raw": None}])
    canone_result = validate_canone_input("not-a-canone-input")

    assert parser_result.is_valid is True
    assert len(parser_result.warnings) == 3
    assert canone_result.is_valid is False
