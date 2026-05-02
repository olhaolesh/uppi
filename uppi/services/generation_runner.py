"""Programmatic runner for the current generation-only pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Protocol
from uuid import uuid4

from itemadapter import ItemAdapter

from uppi.config.immobili import ImmobileConfig, ImmobiliDocumentConfig
from uppi.domain.failure_registry import FailureRecord
from uppi.domain.immobili_document import load_immobili_document
from uppi.items import UppiItem
from uppi.services.failure_registry import FailureRegistryRecorder
from uppi.utils.immobili_item_mapper import map_immobili_document_to_item

logger = logging.getLogger(__name__)


class GenerationProcessorProtocol(Protocol):
    """Minimal process surface required from the generation-only processor."""

    def process_generation_item(self, item, spider):
        """Processes one generation item without invoking browser/import logic."""


@dataclass(frozen=True)
class GenerationArtifactRef:
    """Safe runtime artifact reference returned after synchronous generation."""

    index: int
    foglio: str
    numero: str
    sub: str
    kind: str
    local_path: str | None
    bucket: str | None
    object_key: str | None


@dataclass(frozen=True)
class GenerationRunnerResult:
    """Structured synchronous result from the generation-only runner."""

    run_id: str
    locatore_cf: str
    requested_count: int
    generated_count: int
    failed_count: int
    artifacts: tuple[GenerationArtifactRef, ...]
    messages: tuple[str, ...] = ()
    failure_records: tuple[FailureRecord, ...] = ()


class _InMemoryFailureRegistryStorage:
    """Simple in-memory failure registry storage for one synchronous web run."""

    def __init__(self) -> None:
        self.records: list[FailureRecord] = []

    def append(self, record: FailureRecord) -> None:
        self.records.append(record)

    def list_records(
        self,
        *,
        run_id: str | None = None,
        client_cf: str | None = None,
    ) -> list[FailureRecord]:
        return [
            record
            for record in self.records
            if (run_id is None or record.run_id == run_id)
            and (client_cf is None or record.client_cf == client_cf)
        ]


class _RecordingDocumentStageService:
    """Wraps the current document stage to capture safe artifact references."""

    def __init__(
        self,
        inner,
        *,
        artifacts: list[GenerationArtifactRef],
        index_by_identity: dict[tuple[str, str, str], int],
    ) -> None:
        self.inner = inner
        self.artifacts = artifacts
        self.index_by_identity = index_by_identity

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def run(
        self,
        conn,
        spider,
        adapter: ItemAdapter,
        *,
        run_id: str,
        imm,
        contract_ctx: dict[str, Any],
        contract_id: int,
        immobile_id: int,
        locatore_cf: str,
        canone_result_snapshot: dict[str, Any] | None,
    ):
        output_path = self.inner.run(
            conn,
            spider,
            adapter,
            run_id=run_id,
            imm=imm,
            contract_ctx=contract_ctx,
            contract_id=contract_id,
            immobile_id=immobile_id,
            locatore_cf=locatore_cf,
            canone_result_snapshot=canone_result_snapshot,
        )
        if output_path is None:
            return None

        identity = _identity_key(
            adapter.get("foglio"),
            adapter.get("numero"),
            adapter.get("sub"),
        )
        storage = getattr(self.inner, "storage", None)
        bucket = getattr(getattr(storage, "cfg", None), "attestazioni_bucket", None)
        object_key = None
        if storage is not None and hasattr(storage, "attestazione_object_name"):
            object_key = storage.attestazione_object_name(locatore_cf, contract_id)

        self.artifacts.append(
            GenerationArtifactRef(
                index=self.index_by_identity.get(identity, 0),
                foglio=identity[0],
                numero=identity[1],
                sub=identity[2],
                kind="attestazione_docx",
                local_path=str(output_path),
                bucket=str(bucket) if bucket is not None else None,
                object_key=str(object_key) if object_key is not None else None,
            )
        )
        return output_path


class GenerationRunner:
    """Thin reusable seam over `load -> map -> UppiItem -> VisuraProcessor`."""

    def __init__(
        self,
        *,
        document_loader: Callable[[Path], ImmobiliDocumentConfig] = load_immobili_document,
        item_mapper: Callable[[ImmobiliDocumentConfig, ImmobileConfig], dict[str, Any]] = map_immobili_document_to_item,
        item_factory: Callable[..., Any] = UppiItem,
        processor_factory: Callable[[FailureRegistryRecorder], GenerationProcessorProtocol] | None = None,
    ) -> None:
        self.document_loader = document_loader
        self.item_mapper = item_mapper
        self.item_factory = item_factory
        self.processor_factory = processor_factory

    def run_yaml(
        self,
        yaml_path: str | Path,
        *,
        run_id: str | None = None,
    ) -> GenerationRunnerResult:
        """Loads one canonical single-client YAML and runs the generation-only path."""
        resolved_yaml_path = Path(yaml_path)
        document = self.document_loader(resolved_yaml_path)
        active_immobili = [
            immobile
            for immobile in document.immobili
            if immobile.enabled
        ]
        resolved_run_id = str(run_id or uuid4().hex)
        identity_to_index = {
            _identity_key(immobile.foglio, immobile.numero, immobile.sub): index
            for index, immobile in enumerate(document.immobili, start=1)
        }
        artifacts: list[GenerationArtifactRef] = []
        failure_storage = _InMemoryFailureRegistryStorage()
        failure_registry = FailureRegistryRecorder(storage=failure_storage)
        processor = self._build_processor(
            failure_registry=failure_registry,
            artifacts=artifacts,
            index_by_identity=identity_to_index,
        )
        spider = SimpleNamespace(
            logger=logger,
            run_id=resolved_run_id,
            crawl_run_id=resolved_run_id,
        )

        for immobile in active_immobili:
            mapped = dict(self.item_mapper(document, immobile))
            mapped["run_id"] = resolved_run_id
            item = self.item_factory(**mapped)
            processor.process_generation_item(item, spider)

        failure_records = tuple(
            failure_storage.list_records(
                run_id=resolved_run_id,
                client_cf=document.locatore_cf,
            )
        )
        artifacts_sorted = tuple(sorted(artifacts, key=lambda artifact: artifact.index))
        generated_count = len(artifacts_sorted)
        requested_count = len(active_immobili)
        failed_count = max(0, requested_count - generated_count)
        messages = ()
        if failed_count:
            messages = (f"{failed_count} generation item(s) failed.",)

        return GenerationRunnerResult(
            run_id=resolved_run_id,
            locatore_cf=document.locatore_cf,
            requested_count=requested_count,
            generated_count=generated_count,
            failed_count=failed_count,
            artifacts=artifacts_sorted,
            messages=messages,
            failure_records=failure_records,
        )

    def _build_processor(
        self,
        *,
        failure_registry: FailureRegistryRecorder,
        artifacts: list[GenerationArtifactRef],
        index_by_identity: dict[tuple[str, str, str], int],
    ) -> GenerationProcessorProtocol:
        if self.processor_factory is not None:
            processor = self.processor_factory(failure_registry)
        else:
            from uppi.services.visura_processor import VisuraProcessor

            processor = VisuraProcessor(failure_registry_recorder=failure_registry)

        base_document_stage = getattr(processor, "document_stage_service", None)
        if base_document_stage is not None:
            setattr(
                processor,
                "document_stage_service",
                _RecordingDocumentStageService(
                    base_document_stage,
                    artifacts=artifacts,
                    index_by_identity=index_by_identity,
                ),
            )
        return processor


def _identity_key(foglio: Any, numero: Any, sub: Any) -> tuple[str, str, str]:
    """Builds a stable identity key that tolerates blank/None cadastral sub values."""
    return (
        _stringify_identity_value(foglio),
        _stringify_identity_value(numero),
        _stringify_identity_value(sub),
    )


def _stringify_identity_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


__all__ = [
    "GenerationArtifactRef",
    "GenerationRunner",
    "GenerationRunnerResult",
]
