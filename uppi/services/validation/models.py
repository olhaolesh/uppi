"""Спільні типи й helper-и для soft/hard validation сигналів."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class ValidationIssue:
    """Описує одне validation-повідомлення."""

    code: str
    message: str
    field: str | None = None


@dataclass
class ValidationResult:
    """Агрегує warning/error результати однієї validation-перевірки."""

    warnings: List[ValidationIssue] = field(default_factory=list)
    errors: List[ValidationIssue] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Повертає `True`, якщо structural errors відсутні."""
        return not self.errors

    def add_warning(self, code: str, message: str, *, field: str | None = None) -> None:
        """Додає warning без зміни control flow."""
        self.warnings.append(ValidationIssue(code=code, message=message, field=field))

    def add_error(self, code: str, message: str, *, field: str | None = None) -> None:
        """Додає structural error для явно зламаного контракту."""
        self.errors.append(ValidationIssue(code=code, message=message, field=field))


def emit_validation_messages(
    logger: logging.Logger,
    prefix: str,
    result: ValidationResult,
    *,
    emit_warnings: bool = True,
    emit_errors: bool = True,
) -> None:
    """Пише warning/error результати у лог без зміни бізнес-логіки."""
    if emit_warnings:
        for issue in result.warnings:
            logger.warning("%s [%s] %s", prefix, issue.code, issue.message)
    if emit_errors:
        for issue in result.errors:
            logger.error("%s [%s] %s", prefix, issue.code, issue.message)


def summarize_validation_errors(result: ValidationResult) -> str:
    """Повертає короткий текстовий summary для hard-fail structural cases."""
    return "; ".join(issue.message for issue in result.errors)
