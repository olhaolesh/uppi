"""Append-only storage contract і recorder helpers для failure registry."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from uuid import uuid4
from typing import Protocol

from itemadapter import ItemAdapter

from uppi.domain.failure_registry import FailureRecord, FailureStage
from uppi.services.retry_policy import RetryPolicyMatrix

logger = logging.getLogger(__name__)


def default_failure_registry_path() -> Path:
    """Повертає safe local path для registry без workspace/path redesign."""
    return Path(__file__).resolve().parents[2] / "logs" / "failure_registry.jsonl"


class FailureRegistryStorage(Protocol):
    """Мінімальний storage contract для append-only failure registry."""

    def append(self, record: FailureRecord) -> None:
        """Додає один failure record без руйнування наявної історії."""

    def list_records(
        self,
        *,
        run_id: str | None = None,
        client_cf: str | None = None,
    ) -> list[FailureRecord]:
        """Повертає records, optionally filtered by run_id/client_cf."""


def is_failure_reported(error: BaseException) -> bool:
    """Перевіряє, чи виняток уже був записаний у failure registry."""
    return bool(getattr(error, "_uppi_failure_reported", False))


class FailureRegistryRecorder:
    """Тонкий recorder, який створює і безпечно зберігає failure records."""

    def __init__(
        self,
        storage: FailureRegistryStorage | None = None,
        *,
        retry_policy: RetryPolicyMatrix | None = None,
    ) -> None:
        """Створює recorder з explicit storage і retry-policy matrix."""
        self.storage = storage or JsonlFailureRegistryStorage()
        self.retry_policy = retry_policy or RetryPolicyMatrix()

    def resolve_run_id(self, *, adapter: ItemAdapter | None = None, spider=None) -> str:
        """Повертає stable run id для поточного item-processing scope."""
        if adapter is not None:
            explicit = adapter.get("run_id")
            if explicit:
                return str(explicit)

        for attr_name in ("run_id", "crawl_run_id"):
            candidate = getattr(spider, attr_name, None)
            if candidate:
                return str(candidate)

        return uuid4().hex

    def record_failure(
        self,
        *,
        run_id: str,
        client_cf: str,
        stage: FailureStage,
        error: Exception | str,
        retryable: bool | None = None,
        artifact_refs=(),
    ) -> FailureRecord | None:
        """Створює й append-ить failure record через explicit retry policy matrix."""
        if retryable is not None:
            resolved_retryable = bool(retryable)
        else:
            resolved_retryable = self.retry_policy.decide(
                stage=stage,
                error=error,
            ).retryable

        record = FailureRecord.from_error(
            run_id=run_id,
            client_cf=client_cf,
            stage=stage,
            error=error,
            retryable=resolved_retryable,
            artifact_refs=artifact_refs,
        )

        try:
            self.storage.append(record)
        except Exception as storage_exc:
            logger.warning(
                "[FAILURE_REGISTRY] append failed run_id=%s stage=%s error=%s",
                run_id,
                stage.value,
                storage_exc,
            )
            return None

        if isinstance(error, BaseException):
            setattr(error, "_uppi_failure_reported", True)

        return record


class JsonlFailureRegistryStorage:
    """Проста локальна JSONL-реалізація для майбутнього stage-level reporting."""

    def __init__(self, path: Path | None = None) -> None:
        """Створює storage adapter із explicit або canonical local path."""
        self.path = Path(path) if path is not None else default_failure_registry_path()

    def append(self, record: FailureRecord) -> None:
        """Append-only записує один failure record у JSONL-файл."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.to_dict(), ensure_ascii=False))
            fh.write("\n")

    def list_records(
        self,
        *,
        run_id: str | None = None,
        client_cf: str | None = None,
    ) -> list[FailureRecord]:
        """Читає JSONL history і фільтрує її за базовими ключами контракту."""
        if not self.path.exists():
            return []

        records: list[FailureRecord] = []
        with self.path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue

                record = FailureRecord.from_dict(json.loads(line))
                if run_id is not None and record.run_id != run_id:
                    continue
                if client_cf is not None and record.client_cf != client_cf:
                    continue
                records.append(record)

        return records
