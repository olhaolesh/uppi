"""Сервісний шар над object storage з retry-політикою для S3-помилок."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import logging
from typing import Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from minio.error import S3Error

from uppi.domain.object_storage import ObjectStorage


logger = logging.getLogger(__name__)


s3_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type((S3Error,)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


@dataclass(frozen=True)
class StorageUploadResult:
    """Містить bucket і key для успішно завантаженого артефакту."""
    bucket: str
    object_name: str


class StorageService:
    """Додає retry-політику поверх low-level storage adapter."""
    def __init__(self, storage: Optional[ObjectStorage] = None):
        """Створює сервіс із переданим або типовим storage-адаптером."""
        self.storage = storage or ObjectStorage()

    @s3_retry
    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Перевіряє наявність об’єкта з retry для transient S3-помилок."""
        return self.storage.object_exists(bucket, object_name)

    @s3_retry
    def upload_file(self, bucket: str, object_name: str, path: Path, content_type: str) -> StorageUploadResult:
        """Завантажує файл у сховище й повертає координати створеного артефакту."""
        self.storage.upload_file(bucket, object_name, path, content_type=content_type),
        return StorageUploadResult(bucket=bucket, object_name=object_name)
