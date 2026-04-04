"""Адаптер до S3-compatible storage з additive factory seams для DI foundation."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from decouple import config
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ObjectStorageConfig:
    """
    Конфіг для S3-compatible storage.

    S3_ENDPOINT:
      - для локального MinIO: "localhost:9000"
      - для Cloudflare R2: "<ACCOUNT_ID>.r2.cloudflarestorage.com"
        (без https://), а S3_SECURE=True
    """
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool

    visure_bucket: str
    attestazioni_bucket: str


def load_storage_config() -> ObjectStorageConfig:
    """Зчитує storage-конфіг з env у provider-neutral dataclass shape.

    Current runtime лишається env-backed. Future AWS/provider integration має
    гідрувати той самий `ObjectStorageConfig`, а не змінювати storage API.
    """
    return ObjectStorageConfig(
        endpoint=config("S3_ENDPOINT", default="localhost:9000"),
        access_key=config("S3_ACCESS_KEY", default="minioadmin"),
        secret_key=config("S3_SECRET_KEY", default="minioadmin"),
        secure=config("S3_SECURE", default="False").strip().lower() == "true",
        visure_bucket=config("VISURE_BUCKET", default=config("MINIO_BUCKET", default="uppi-bucket")),
        attestazioni_bucket=config("ATTESTAZIONI_BUCKET", default="attestazioni"),
    )


def create_object_storage(
    cfg: Optional[ObjectStorageConfig] = None,
    *,
    config_loader: Callable[[], ObjectStorageConfig] = load_storage_config,
    client_factory: Callable[..., Minio] = Minio,
) -> "ObjectStorage":
    """Створює storage-адаптер через explicit factory з current-compatible defaults."""
    return ObjectStorage(cfg=cfg, config_loader=config_loader, client_factory=client_factory)


class ObjectStorage:
    """
    Тонка обгортка над MinIO/S3 client.

    Boundary тут already відділяє caller-ів від конкретного backend-а
    (MinIO/R2/S3-compatible). Цей PR не додає AWS implementation, лише фіксує,
    що future provider/readiness work має лишатися в межах config/client seams.
    """
    def __init__(
        self,
        cfg: Optional[ObjectStorageConfig] = None,
        *,
        config_loader: Callable[[], ObjectStorageConfig] = load_storage_config,
        client_factory: Callable[..., Minio] = Minio,
    ):
        """Створює storage-адаптер з явним або canonical env-backed конфігом."""
        self.cfg = cfg or config_loader()
        self._client_factory = client_factory
        self._client: Optional[Minio] = None

    @property
    def client(self) -> Minio:
        """Ліниво створює й кешує клієнт MinIO/S3."""
        if self._client is None:
            self._client = self._client_factory(
                self.cfg.endpoint,
                access_key=self.cfg.access_key,
                secret_key=self.cfg.secret_key,
                secure=self.cfg.secure,
            )
        return self._client

    def ensure_bucket(self, bucket: str) -> None:
        """
        Для MinIO локально — створить bucket якщо нема.
        Для R2 часто створення bucket API може бути заборонене — тоді просто лог і йдемо далі.
        """
        try:
            exists = self.client.bucket_exists(bucket)
        except Exception as e:
            logger.warning("[S3] Cannot check bucket_exists(%s): %s", bucket, e)
            return

        if exists:
            return

        try:
            self.client.make_bucket(bucket)
            logger.info("[S3] Created bucket=%s", bucket)
        except S3Error as e:
            # На R2 типово: AccessDenied або MethodNotAllowed
            logger.warning("[S3] Cannot make_bucket(%s): %s", bucket, e)
        except Exception as e:
            logger.warning("[S3] Unexpected make_bucket(%s) error: %s", bucket, e)

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Перевіряє наявність об’єкта у bucket без кидання S3-помилки назовні."""
        try:
            self.client.stat_object(bucket, object_name)
            return True
        except S3Error:
            return False
        except Exception as e:
            logger.warning("[S3] object_exists error bucket=%s object=%s: %s", bucket, object_name, e)
            return False

    def upload_file(self, bucket: str, object_name: str, file_path: Path, content_type: str) -> None:
        """Завантажує локальний файл у bucket з указаним content type."""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.ensure_bucket(bucket)

        try:
            self.client.fput_object(bucket, object_name, str(file_path), content_type=content_type)
            logger.info("[S3] Uploaded %s -> %s/%s", file_path, bucket, object_name)
        except S3Error as e:
            logger.exception("[S3] Upload failed %s -> %s/%s: %s", file_path, bucket, object_name, e)
            raise
        except Exception as e:
            logger.exception("[S3] Unexpected upload error %s -> %s/%s: %s", file_path, bucket, object_name, e)
            raise

    # ---- Canonical object names (щоб не плодити різні формати) ----

    def visura_object_name(self, cf: str) -> str:
        """Формує canonical key для PDF-візури конкретного CF."""
        return f"visure/{cf}.pdf"

    def attestazione_object_name(self, cf: str, contract_id: str) -> str:
        """Формує canonical key для DOCX-атестації конкретного контракту."""
        return f"attestazioni/{cf}/{contract_id}.docx"
