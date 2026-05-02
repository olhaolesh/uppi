"""Schemas for the Stage 5 bulk-import web API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

if TYPE_CHECKING:
    from uppi.web.services.bulk_import_adapter import BulkImportWebResult


class _StrictWebModel(BaseModel):
    """Base model for additive web DTOs that reject unknown fields."""

    model_config = ConfigDict(extra="forbid")


class ClientsBulkImportRequest(_StrictWebModel):
    """Protected request payload for `POST /clients/bulk-import`."""

    csv_content: str
    force_update_visura: bool = False
    fail_fast: bool = False

    @field_validator("csv_content", mode="before")
    @classmethod
    def _normalize_csv_content(cls, value) -> str:
        return str(value or "")

    @field_validator("csv_content")
    @classmethod
    def _validate_csv_content(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("csv_content must be a non-empty string")
        return value


class ClientsBulkImportInputResponse(BaseModel):
    """Web-facing record of the CSV input and applied flags."""

    clients_csv_path: str
    force_update_visura: bool
    fail_fast: bool


class ClientsBulkImportSummaryResponse(BaseModel):
    """Synchronous import-only counters returned to the web client."""

    total_rows: int
    valid_rows: int
    invalid_rows: int
    unique_clients: int
    imported_count: int
    failed_count: int
    skipped_count: int


class ClientsBulkImportRowResultResponse(BaseModel):
    """Per-row result for valid or deduped CSV rows."""

    row_number: int
    locatore_cf: str
    status: Literal["imported", "failed", "skipped_duplicate"]
    message: str


class ClientsBulkImportInvalidRowResponse(BaseModel):
    """Per-row invalid CSV diagnostics."""

    row_number: int
    code: str | None
    message: str


class ClientsBulkImportResponse(BaseModel):
    """Protected synchronous response shape for `POST /clients/bulk-import`."""

    status: Literal["completed", "aborted"]
    run_id: str
    input: ClientsBulkImportInputResponse
    summary: ClientsBulkImportSummaryResponse
    results: list[ClientsBulkImportRowResultResponse]
    invalid_rows: list[ClientsBulkImportInvalidRowResponse]
    messages: list[str]

    @classmethod
    def from_web_result(cls, result: "BulkImportWebResult") -> "ClientsBulkImportResponse":
        return cls(
            status=result.status,
            run_id=result.run_id,
            input=ClientsBulkImportInputResponse(
                clients_csv_path=result.clients_csv_path_relative,
                force_update_visura=result.force_update_visura,
                fail_fast=result.fail_fast,
            ),
            summary=ClientsBulkImportSummaryResponse(
                total_rows=result.total_rows,
                valid_rows=result.valid_rows,
                invalid_rows=result.invalid_rows_count,
                unique_clients=result.unique_clients,
                imported_count=result.imported_count,
                failed_count=result.failed_count,
                skipped_count=result.skipped_count,
            ),
            results=[
                ClientsBulkImportRowResultResponse(
                    row_number=row.row_number,
                    locatore_cf=row.locatore_cf,
                    status=row.status,
                    message=row.message,
                )
                for row in result.results
            ],
            invalid_rows=[
                ClientsBulkImportInvalidRowResponse(
                    row_number=row.row_number,
                    code=row.code,
                    message=row.message,
                )
                for row in result.invalid_rows
            ],
            messages=list(result.messages),
        )


__all__ = [
    "ClientsBulkImportRequest",
    "ClientsBulkImportResponse",
]
