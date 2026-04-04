"""Основний non-browser orchestrator, що зшиває parser, БД, storage і DOCX."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, List, Optional, Tuple
from dataclasses import replace

from decouple import config
from itemadapter import ItemAdapter

from uppi.config.app_config import VisuraProcessorRuntimeConfig
from uppi.domain.calculation_strategy import CalculationStrategy
from uppi.domain.db import get_pg_connection
from uppi.domain.failure_registry import FailureStage
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage
from uppi.domain.storage import get_client_dir, get_visura_path
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.services.db_repo import (
    db_load_immobili,
)
from uppi.services.failure_registry import FailureRegistryRecorder, is_failure_reported
from uppi.services.storage_minio import StorageService
from uppi.services.visura_stages import (
    AuditStageService,
    CanoneStageService,
    ContractSyncService,
    DocumentStageService,
    ImmobileSyncService,
    PersonSyncService,
    VisuraIngestService,
)
from uppi.utils.audit import safe_unlink
from uppi.utils.parse_utils import clean_str

logger = logging.getLogger(__name__)

# Налаштування з середовища
AE_USERNAME = config("AE_USERNAME", default="").strip()
TEMPLATE_VERSION = config("TEMPLATE_VERSION", default="pescara2018_v2").strip()
PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS = config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS",
                                              default="True").strip().lower() == "true"
DELETE_LOCAL_VISURA_AFTER_UPLOAD = config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False").strip().lower() == "true"
DEFAULT_TEMPLATE_PATH = Path(__file__).resolve().parents[2] / "attestazione_template" / "template_attestazione_pescara.docx"


def default_visura_processor_runtime_config(*, template_path: Path | None = None) -> VisuraProcessorRuntimeConfig:
    """Повертає runtime defaults процесора, сумісні з чинними module constants."""
    return VisuraProcessorRuntimeConfig(
        ae_username=AE_USERNAME,
        template_version=TEMPLATE_VERSION,
        template_path=Path(template_path) if template_path is not None else DEFAULT_TEMPLATE_PATH,
        prune_old_immobili_without_contracts=PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS,
        delete_local_visura_after_upload=DELETE_LOCAL_VISURA_AFTER_UPLOAD,
    )


def find_local_visura_pdf(cf: str, adapter: ItemAdapter) -> Optional[Path]:
    """Пошук файлу візури в локальній файловій системі."""
    p = clean_str(adapter.get("visura_download_path"))
    if p:
        path = Path(p)
        if path.exists():
            return path

    fallback = get_visura_path(cf)
    if fallback.exists():
        return fallback

    client_dir = get_client_dir(cf)
    # Шукаємо за префіксом DOC_ або просто найновіший PDF
    candidates = sorted(client_dir.glob("DOC_*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    any_pdf = sorted(client_dir.glob("*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if any_pdf:
        return any_pdf[0]

    return None


def filter_immobiles_by_yaml(immobiles: List[Tuple[int, Immobile]], adapter: ItemAdapter) -> List[Tuple[int, Immobile]]:
    """Фільтрація списку нерухомості з БД за параметрами, вказаними в YAML (item)."""
    foglio_f = clean_str(adapter.get("foglio"))
    numero_f = clean_str(adapter.get("numero"))
    sub_f = clean_str(adapter.get("sub"))

    out: List[Tuple[int, Immobile]] = []
    for imm_id, imm in immobiles:
        # Порівнюємо кадастрові ідентифікатори (основний спосіб матчингу)
        if foglio_f and str(getattr(imm, "foglio", "") or "") != foglio_f:
            continue
        if numero_f and str(getattr(imm, "numero", "") or "") != numero_f:
            continue
        if sub_f and str(getattr(imm, "sub", "") or "") != sub_f:
            continue

        out.append((imm_id, imm))
        logger.debug("[PIPELINE] Immobile ID=%s matched YAML criteria", imm_id)
    return out


class VisuraProcessor:
    """Зшиває parser, БД, storage і DOCX у один non-browser pipeline."""

    def __init__(
        self,
        storage: Optional[ObjectStorage] = None,
        template_path: Optional[Path] = None,
        *,
        storage_service: Optional[StorageService] = None,
        connection_factory: Callable = get_pg_connection,
        parser_factory: Callable[[], VisuraParser] = VisuraParser,
        runtime_config: Optional[VisuraProcessorRuntimeConfig] = None,
        calculation_strategy: CalculationStrategy | None = None,
        person_sync_service: PersonSyncService | None = None,
        visura_ingest_service: VisuraIngestService | None = None,
        immobile_sync_service: ImmobileSyncService | None = None,
        contract_sync_service: ContractSyncService | None = None,
        canone_stage_service: CanoneStageService | None = None,
        document_stage_service: DocumentStageService | None = None,
        audit_stage_service: AuditStageService | None = None,
        failure_registry_recorder: FailureRegistryRecorder | None = None,
    ):
        """
        Створює процесор із explicit dependency seams і current-compatible defaults.

        Цей конструктор не змінює orchestration order; він лише робить creation
        paths явними для подальших Sprint 2 змін.
        """
        resolved_runtime_config = runtime_config or default_visura_processor_runtime_config(template_path=template_path)
        if template_path is not None and runtime_config is not None:
            resolved_runtime_config = replace(resolved_runtime_config, template_path=Path(template_path))

        resolved_storage = storage or ObjectStorage()

        self.runtime_config = resolved_runtime_config
        self.storage = resolved_storage
        self.storage_service = storage_service or StorageService(resolved_storage)
        self.connection_factory = connection_factory
        self.parser_factory = parser_factory
        self.template_path = resolved_runtime_config.template_path
        self.failure_registry_recorder = failure_registry_recorder or FailureRegistryRecorder()
        self.person_sync_service = person_sync_service or PersonSyncService(
            failure_recorder=self.failure_registry_recorder,
        )
        self.visura_ingest_service = visura_ingest_service or VisuraIngestService(
            storage=resolved_storage,
            storage_service=self.storage_service,
            pdf_lookup=find_local_visura_pdf,
            failure_recorder=self.failure_registry_recorder,
        )
        self.immobile_sync_service = immobile_sync_service or ImmobileSyncService(
            parser_factory=self.parser_factory,
            prune_old_immobili_without_contracts=self.runtime_config.prune_old_immobili_without_contracts,
            failure_recorder=self.failure_registry_recorder,
        )
        self.contract_sync_service = contract_sync_service or ContractSyncService(
            failure_recorder=self.failure_registry_recorder,
        )
        self.canone_stage_service = canone_stage_service or CanoneStageService(
            calculation_strategy=calculation_strategy,
            failure_recorder=self.failure_registry_recorder,
        )
        self.audit_stage_service = audit_stage_service or AuditStageService(
            runtime_config=self.runtime_config,
            failure_recorder=self.failure_registry_recorder,
        )
        self.document_stage_service = document_stage_service or DocumentStageService(
            storage=resolved_storage,
            storage_service=self.storage_service,
            runtime_config=self.runtime_config,
            template_path=self.template_path,
            audit_stage=self.audit_stage_service,
            failure_recorder=self.failure_registry_recorder,
        )

    def process_item(self, item, spider):
        """Повністю обробляє один item після етапу browser download."""
        adapter = ItemAdapter(item)
        run_id = self.failure_registry_recorder.resolve_run_id(adapter=adapter, spider=spider)
        locatore_cf = clean_str(adapter.get("locatore_cf") or adapter.get("codice_fiscale"))

        if not locatore_cf:
            spider.logger.error("[PIPELINE] Missing locatore_cf for item: %r", item)
            return item

        cond_cf = clean_str(adapter.get("conduttore_cf"))
        conn = self.connection_factory()

        try:
            person_sync = self.person_sync_service.sync(
                conn,
                adapter,
                run_id=run_id,
                locatore_cf=locatore_cf,
                cond_cf=cond_cf,
            )

            visura_ingest = self.visura_ingest_service.ingest(
                conn,
                adapter,
                run_id=run_id,
                locatore_cf=locatore_cf,
            )

            self.immobile_sync_service.sync(
                conn,
                spider,
                adapter,
                run_id=run_id,
                locatore_cf=locatore_cf,
                loc_addr_id=person_sync.loc_addr_id,
                visura_ingest=visura_ingest,
            )

            # --- ЕТАП 4: ОПЕРАЦІЙНИЙ ЦИКЛ (КОНТРАКТИ ТА ГЕНЕРАЦІЯ) ---

            immobili_db = db_load_immobili(conn, locatore_cf)
            selected = filter_immobiles_by_yaml(immobili_db, adapter)

            for immobile_id, imm in selected:
                contract_sync = self.contract_sync_service.sync(
                    conn,
                    adapter,
                    run_id=run_id,
                    client_cf=locatore_cf,
                    immobile_id=immobile_id,
                )

                canone_stage = self.canone_stage_service.run(
                    conn,
                    spider,
                    adapter,
                    run_id=run_id,
                    locatore_cf=locatore_cf,
                    imm=imm,
                    contract_id=contract_sync.contract_id,
                    contract_ctx=contract_sync.contract_ctx,
                )

                self.document_stage_service.run(
                    conn,
                    spider,
                    adapter,
                    run_id=run_id,
                    imm=imm,
                    contract_ctx=canone_stage.contract_ctx,
                    contract_id=contract_sync.contract_id,
                    immobile_id=immobile_id,
                    locatore_cf=locatore_cf,
                    canone_result_snapshot=canone_stage.canone_result_snapshot,
                )

            conn.commit()

            # Очистка тимчасових файлів
            if self.runtime_config.delete_local_visura_after_upload and visura_ingest.pdf_to_delete:
                safe_unlink(visura_ingest.pdf_to_delete)

            return item

        except Exception as e:
            if not is_failure_reported(e):
                self.failure_registry_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.PIPELINE_FATAL,
                    error=e,
                )
            spider.logger.exception("[PIPELINE] Fatal error processing CF %s: %s", locatore_cf, e)
            if conn:
                conn.rollback()
            return item
        finally:
            if conn:
                conn.close()
