"""Web-facing adapters and services for the additive shell."""

from .auth import authenticate_login, build_session_identity
from .bulk_import_adapter import (
    BulkImportAdapter,
    BulkImportCsvWriteError,
    BulkImportNoUsableRowsError,
    BulkImportWebResult,
)
from .generation_adapter import GeneratedRunResult, GenerationAdapter, GenerationRunFailedError
from .generation_yaml_builder import BuiltGenerationYaml, GenerationYamlBuilder
from .job_registry import JobRegistry, JobRegistryNotFoundError
from .prepare_adapter import PrepareSearchAdapter, PreparedSearchResult

__all__ = [
    "BuiltGenerationYaml",
    "BulkImportAdapter",
    "BulkImportCsvWriteError",
    "BulkImportNoUsableRowsError",
    "BulkImportWebResult",
    "GeneratedRunResult",
    "GenerationAdapter",
    "GenerationRunFailedError",
    "GenerationYamlBuilder",
    "JobRegistry",
    "JobRegistryNotFoundError",
    "PrepareSearchAdapter",
    "PreparedSearchResult",
    "authenticate_login",
    "build_session_identity",
]
