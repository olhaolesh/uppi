"""Typed domain exceptions для non-browser service і validation surface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from uppi.services.validation.models import ValidationIssue, ValidationResult


class DomainError(Exception):
    """Базовий domain-рівень помилок із явною recoverable-класифікацією."""

    recoverable = False
    code = "domain_error"

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        issues: Sequence[ValidationIssue] | None = None,
    ) -> None:
        """Зберігає повідомлення, structured details і validation-issues для подальшого аналізу."""
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.issues = tuple(issues or ())


class RecoverableDomainError(DomainError):
    """Помилка, яку поточний pipeline може локально пережити або пропустити."""

    recoverable = True
    code = "recoverable_domain_error"


class NonRecoverableDomainError(DomainError):
    """Помилка, яка для поточного item/stage вважається фатальною."""

    recoverable = False
    code = "non_recoverable_domain_error"


class ValidationError(DomainError):
    """Базова validation-помилка для structural contract violations."""

    code = "validation_error"

    @classmethod
    def from_validation_result(
        cls,
        result: ValidationResult,
        *,
        fallback_message: str,
    ) -> "ValidationError":
        """Створює typed exception із current `ValidationResult`."""
        from uppi.services.validation.models import summarize_validation_errors

        return cls(
            summarize_validation_errors(result) or fallback_message,
            details={
                "error_codes": [issue.code for issue in result.errors],
                "warning_codes": [issue.code for issue in result.warnings],
            },
            issues=result.errors,
        )


class RecoverableValidationError(RecoverableDomainError, ValidationError):
    """Validation-помилка, яка у current flow вважається recoverable."""

    code = "recoverable_validation_error"


class NonRecoverableValidationError(NonRecoverableDomainError, ValidationError):
    """Validation-помилка, яка у current flow вважається non-recoverable."""

    code = "non_recoverable_validation_error"


class YamlInputValidationError(RecoverableValidationError):
    """Структурно невалідний YAML/input record, який можна пропустити."""

    code = "yaml_input_validation_error"


class ParsedVisuraValidationError(NonRecoverableValidationError):
    """Структурно зламаний parser output для поточного item."""

    code = "parsed_visura_validation_error"


class CanoneInputValidationError(RecoverableValidationError):
    """Структурно зламаний вхід для canone stage, який можна локально пропустити."""

    code = "canone_input_validation_error"


class ImmobiliDocumentNotFoundError(NonRecoverableDomainError):
    """Requested CF does not have enough DB state to build a generated immobili document."""

    code = "immobili_document_not_found"


class PrepareModeError(NonRecoverableDomainError):
    """Base application-level error for prepare-by-CF orchestration."""

    code = "prepare_mode_error"


class PrepareInputError(PrepareModeError):
    """Raised when `prepare-by-cf` receives a missing or malformed CF."""

    code = "prepare_input_error"


class PrepareImportFailedError(PrepareModeError):
    """Raised when the import-only runner cannot complete successfully."""

    code = "prepare_import_failed"


class PrepareNoDataError(PrepareModeError):
    """Raised when prepare cannot find DB-backed data to build `immobili.yml`."""

    code = "prepare_no_data"


class PrepareGenerationFailedError(PrepareModeError):
    """Raised when YAML generation fails after prepare completed its DB work."""

    code = "prepare_generation_failed"


class PrepareOutputWriteError(PrepareModeError):
    """Raised when prepare cannot persist the generated `immobili.yml`."""

    code = "prepare_output_write_error"


class ImportOnlyRunnerFailedError(NonRecoverableDomainError):
    """Raised when the reusable import-only runner cannot complete successfully."""

    code = "import_only_runner_failed"


class BulkImportModeError(NonRecoverableDomainError):
    """Base application-level error for bulk CSV import-only orchestration."""

    code = "bulk_import_mode_error"


class BulkImportCsvLoadError(BulkImportModeError):
    """Raised when bulk mode cannot load or parse the requested CSV file."""

    code = "bulk_import_csv_load_error"


class GenerationPrepareRequiredError(NonRecoverableDomainError):
    """Raised when generation cannot match a prepared immobile in DB state."""

    code = "generation_prepare_required"
