"""Тонкі stage services для non-browser pipeline `VisuraProcessor`."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from itemadapter import ItemAdapter

from uppi.config.app_config import VisuraProcessorRuntimeConfig
from uppi.domain.calculation_strategy import (
    CalculationStrategy,
    get_default_calculation_strategy,
)
from uppi.domain.canone_models import CanoneInput, ContractKind
from uppi.domain.failure_registry import FailureArtifactRef, FailureStage
from uppi.domain.exceptions import (
    CanoneInputValidationError,
    ParsedVisuraValidationError,
)
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage
from uppi.domain.storage import get_attestazione_path
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.services.attestazione_generator import build_template_params
from uppi.services.db_repo import (
    db_insert_attestazione_log,
    db_insert_canone_calc,
    db_load_contract_context,
    db_prune_old_immobili_without_contracts,
    db_update_immobile_real_address,
    db_upsert_address,
    db_upsert_contract,
    db_upsert_immobile,
    db_upsert_immobile_elements,
    db_upsert_person,
    db_upsert_visura,
    immobile_db_row,
    immobile_from_parsed_dict,
)
from uppi.services.attestazione_template_filler import fill_attestazione_template, underscored
from uppi.services.storage_minio import StorageService
from uppi.services.validation import (
    emit_validation_messages,
    validate_canone_input,
    validate_parsed_visura_output,
)
from uppi.services.failure_registry import FailureRegistryRecorder
from uppi.utils.audit import mask_username, sha256_file, sha256_text
from uppi.utils.parse_utils import clean_str, prepare_for_json, safe_float, split_full_name

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PersonSyncResult:
    """Результат синхронізації адрес і персон до подальших stage calls."""

    loc_addr_id: int | None
    cond_addr_id: int | None


@dataclass(frozen=True)
class VisuraIngestResult:
    """Результат ingest стадії для вже завантаженої локальної візури."""

    visura_db_id: int | None
    fetched_now: bool
    pdf_path: Path | None
    pdf_to_delete: Path | None


@dataclass(frozen=True)
class ImmobileSyncResult:
    """Результат синхронізації parsed immobile rows у master tables."""

    keep_ids: list[int]


@dataclass(frozen=True)
class ContractSyncResult:
    """Результат підготовки immobile/contract context для подальших stage calls."""

    contract_id: int
    contract_ctx: dict[str, Any]


@dataclass(frozen=True)
class CanoneStageResult:
    """Результат canone stage із можливим перезавантаженим contract context."""

    contract_ctx: dict[str, Any]
    canone_snapshot: dict[str, Any]
    canone_result_snapshot: dict[str, Any] | None


class PersonSyncService:
    """Синхронізує locatore/conduttore і їхні адреси без зміни stage order."""

    def __init__(self, *, failure_recorder: FailureRegistryRecorder | None = None) -> None:
        """Зберігає optional failure recorder без зміни success-flow semantics."""
        self.failure_recorder = failure_recorder

    def sync(
        self,
        conn,
        adapter: ItemAdapter,
        *,
        run_id: str,
        locatore_cf: str,
        cond_cf: str | None,
    ) -> PersonSyncResult:
        """Записує адреси й персони рівно в тому самому порядку, що й старий orchestrator."""
        try:
            loc_addr_id = None
            if adapter.get("locatore_comune_res") and adapter.get("locatore_via"):
                loc_addr_id = db_upsert_address(
                    conn,
                    {
                        "comune": adapter.get("locatore_comune_res"),
                        "via_full": adapter.get("locatore_via"),
                        "civico": adapter.get("locatore_civico"),
                    },
                )

            db_upsert_person(
                conn,
                locatore_cf,
                surname=clean_str(adapter.get("locatore_surname")),
                name=clean_str(adapter.get("locatore_name")),
                address_id=loc_addr_id,
            )

            cond_addr_id = None
            if cond_cf:
                if adapter.get("conduttore_comune"):
                    cond_addr_id = db_upsert_address(
                        conn,
                        {
                            "comune": adapter.get("conduttore_comune"),
                            "via_full": adapter.get("conduttore_via") or "",
                        },
                    )

                cond_full_name = clean_str(adapter.get("conduttore_nome"))
                c_surname, c_name = split_full_name(cond_full_name)

                db_upsert_person(
                    conn,
                    cond_cf,
                    surname=c_surname,
                    name=c_name,
                    address_id=cond_addr_id,
                )

            return PersonSyncResult(loc_addr_id=loc_addr_id, cond_addr_id=cond_addr_id)
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.PERSON_SYNC,
                    error=exc,
                )
            raise


class VisuraIngestService:
    """Реєструє й завантажує локальний PDF у storage без зміни lookup/upload contract."""

    def __init__(
        self,
        *,
        storage: ObjectStorage,
        storage_service: StorageService,
        pdf_lookup: Callable[[str, ItemAdapter], Optional[Path]],
        sha256_file_fn: Callable[[Path], str] = sha256_file,
        failure_recorder: FailureRegistryRecorder | None = None,
    ) -> None:
        """Фіксує залежності ingest стадії без зміни current defaults."""
        self.storage = storage
        self.storage_service = storage_service
        self.pdf_lookup = pdf_lookup
        self.sha256_file_fn = sha256_file_fn
        self.failure_recorder = failure_recorder

    def ingest(self, conn, adapter: ItemAdapter, *, run_id: str, locatore_cf: str) -> VisuraIngestResult:
        """Виконує current visura ingest path без зміни artifact lookup order."""
        visura_source = clean_str(adapter.get("visura_source"))
        visura_downloaded = bool(adapter.get("visura_downloaded"))
        pdf_path = None
        fetched_now = False
        visura_db_id = None
        pdf_to_delete: Path | None = None
        obj_name = self.storage.visura_object_name(locatore_cf)

        try:
            if visura_source == "sister" and visura_downloaded:
                pdf_path = self.pdf_lookup(locatore_cf, adapter)
                # Важливо: коли файл не знайдено, current behavior не робить fallback insert.
                if pdf_path:
                    checksum = self.sha256_file_fn(pdf_path)
                    bucket = self.storage.cfg.visure_bucket

                    self.storage_service.upload_file(bucket, obj_name, pdf_path, content_type="application/pdf")
                    fetched_now = True
                    visura_db_id = db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum, fetched_now=True)
                    pdf_to_delete = pdf_path
            else:
                visura_db_id = db_upsert_visura(
                    conn,
                    locatore_cf,
                    self.storage.cfg.visure_bucket,
                    obj_name,
                    None,
                    fetched_now=False,
                )

            return VisuraIngestResult(
                visura_db_id=visura_db_id,
                fetched_now=fetched_now,
                pdf_path=pdf_path,
                pdf_to_delete=pdf_to_delete,
            )
        except Exception as exc:
            if self.failure_recorder is not None:
                artifact_refs = [
                    FailureArtifactRef.create("storage_object", obj_name),
                ]
                if pdf_path is not None:
                    artifact_refs.append(FailureArtifactRef.create("local_visura_pdf", pdf_path))
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.VISURA_INGEST,
                    error=exc,
                    artifact_refs=artifact_refs,
                )
            raise


class ImmobileSyncService:
    """Парсить візуру й синхронізує immobile master data без зміни SQL contract."""

    def __init__(
        self,
        *,
        parser_factory: Callable[[], VisuraParser],
        prune_old_immobili_without_contracts: bool,
        failure_recorder: FailureRegistryRecorder | None = None,
    ) -> None:
        """Зберігає залежності стадії без зміни parser/repo semantics."""
        self.parser_factory = parser_factory
        self.prune_old_immobili_without_contracts = prune_old_immobili_without_contracts
        self.failure_recorder = failure_recorder

    def sync(
        self,
        conn,
        spider,
        adapter: ItemAdapter,
        *,
        run_id: str,
        locatore_cf: str,
        loc_addr_id: int | None,
        visura_ingest: VisuraIngestResult,
    ) -> ImmobileSyncResult:
        """Обробляє parsed immobile rows у тому самому порядку, що й старий код."""
        keep_ids: list[int] = []
        if not (visura_ingest.fetched_now and visura_ingest.pdf_path):
            return ImmobileSyncResult(keep_ids=keep_ids)

        try:
            parser = self.parser_factory()
            parsed_dicts = parser.parse(visura_ingest.pdf_path)
            parsed_validation = validate_parsed_visura_output(parsed_dicts)
            emit_validation_messages(
                spider.logger,
                "[PARSER][VALIDATION]",
                parsed_validation,
                emit_errors=False,
            )
            if not parsed_validation.is_valid:
                raise ParsedVisuraValidationError.from_validation_result(
                    parsed_validation,
                    fallback_message="Структурно некоректний parser output.",
                )

            if parsed_dicts:
                first_item = parsed_dicts[0]
                v_name = first_item.get("locatore_name")
                v_surname = first_item.get("locatore_surname")

                db_upsert_person(
                    conn,
                    locatore_cf,
                    surname=clean_str(adapter.get("locatore_surname")) or v_surname,
                    name=clean_str(adapter.get("locatore_name")) or v_name,
                    address_id=loc_addr_id,
                )

            for parsed_row in parsed_dicts:
                v_addr_id = db_upsert_address(
                    conn,
                    {
                        "comune": parsed_row.get("immobile_comune"),
                        "via_full": parsed_row.get("via_name") or parsed_row.get("indirizzo_raw"),
                        "civico": parsed_row.get("via_num"),
                        "piano": parsed_row.get("piano"),
                        "interno": parsed_row.get("interno"),
                        "scala": parsed_row.get("scala"),
                    },
                )

                imm_obj = immobile_from_parsed_dict(parsed_row)
                imm_id = db_upsert_immobile(
                    conn,
                    locatore_cf,
                    imm_obj,
                    visura_addr_id=v_addr_id,
                    source_visura_id=visura_ingest.visura_db_id,
                )
                keep_ids.append(imm_id)

            if keep_ids:
                db_prune_old_immobili_without_contracts(
                    conn,
                    locatore_cf,
                    keep_ids,
                    self.prune_old_immobili_without_contracts,
                )

            return ImmobileSyncResult(keep_ids=keep_ids)
        except Exception as exc:
            if self.failure_recorder is not None:
                artifact_refs = []
                if visura_ingest.pdf_path is not None:
                    artifact_refs.append(FailureArtifactRef.create("local_visura_pdf", visura_ingest.pdf_path))
                if visura_ingest.visura_db_id is not None:
                    artifact_refs.append(FailureArtifactRef.create("visura_id", str(visura_ingest.visura_db_id)))
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.IMMOBILE_SYNC,
                    error=exc,
                    artifact_refs=artifact_refs,
                )
            raise


class ContractSyncService:
    """Підготовлює contract context для одного immobile без зміни current joins."""

    def __init__(self, *, failure_recorder: FailureRegistryRecorder | None = None) -> None:
        """Зберігає optional recorder для stage-level failure reporting."""
        self.failure_recorder = failure_recorder

    def sync(
        self,
        conn,
        adapter: ItemAdapter,
        *,
        run_id: str,
        client_cf: str,
        immobile_id: int,
    ) -> ContractSyncResult:
        """Виконує current real-address -> elements -> contract -> context chain."""
        try:
            real_addr_id = None
            if adapter.get("immobile_comune") or adapter.get("immobile_via"):
                real_addr_id = db_upsert_address(
                    conn,
                    {
                        "comune": adapter.get("immobile_comune"),
                        "via_full": adapter.get("immobile_via"),
                        "civico": adapter.get("immobile_civico"),
                        "piano": adapter.get("immobile_piano"),
                        "interno": adapter.get("immobile_interno"),
                    },
                )

            db_update_immobile_real_address(
                conn,
                immobile_id,
                real_address_id=real_addr_id,
                energy_class=adapter.get("energy_class"),
            )
            db_upsert_immobile_elements(conn, immobile_id, adapter)

            contract_id = db_upsert_contract(conn, immobile_id, adapter)
            contract_ctx = db_load_contract_context(conn, contract_id)
            return ContractSyncResult(contract_id=contract_id, contract_ctx=contract_ctx)
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=client_cf,
                    stage=FailureStage.CONTRACT_SYNC,
                    error=exc,
                    artifact_refs=[FailureArtifactRef.create("immobile_id", str(immobile_id))],
                )
            raise


class CanoneStageService:
    """Готує `CanoneInput`, виконує current calculation path і зберігає snapshot."""

    def __init__(
        self,
        *,
        calculation_strategy: CalculationStrategy | None = None,
        failure_recorder: FailureRegistryRecorder | None = None,
    ) -> None:
        """Зберігає strategy seam і optional recorder без зміни current defaults."""
        self.calculation_strategy = calculation_strategy or get_default_calculation_strategy()
        self.failure_recorder = failure_recorder

    def run(
        self,
        conn,
        spider,
        adapter: ItemAdapter,
        *,
        run_id: str,
        locatore_cf: str,
        imm: Immobile,
        contract_id: int,
        contract_ctx: dict[str, Any],
    ) -> CanoneStageResult:
        """Виконує canone stage без зміни fallback rules, logging і persistence timing."""
        canone_snapshot: dict[str, Any] = {}
        canone_result_snapshot: dict[str, Any] | None = None

        try:
            elements = contract_ctx.get("elements") or {}

            def cnt(keys: list[str]) -> int:
                """Рахує кількість непорожніх element-flags у current contract context."""
                return sum(1 for key in keys if str(elements.get(key, "") or "").strip() != "")

            kind_raw = clean_str(adapter.get("contract_kind"))
            kind_str = (kind_raw or "CONCORDATO").upper()

            kind_enum = ContractKind.CONCORDATO
            try:
                kind_enum = ContractKind[kind_str]
            except KeyError:
                # Unknown kind intentionally падає в current default `CONCORDATO`,
                # бо це вже зафіксована runtime semantics і baseline-covered quirk.
                logger.warning("[CANONE] Unknown contract kind '%s', defaulting to CONCORDATO", kind_str)
                kind_enum = ContractKind.CONCORDATO

            yaml_energy = clean_str(adapter.get("energy_class"))
            db_energy = contract_ctx.get("immobile", {}).get("energy_class")
            if yaml_energy == "-":
                final_energy = None
            elif yaml_energy:
                final_energy = yaml_energy.upper()
            else:
                final_energy = db_energy

            yaml_istat = adapter.get("istat")
            db_istat = contract_ctx.get("contract", {}).get("istat_rate")
            final_istat = None
            if str(yaml_istat).strip() == "-":
                final_istat = None
            elif yaml_istat is not None and str(yaml_istat).strip() != "":
                final_istat = safe_float(yaml_istat)
            elif db_istat is not None:
                final_istat = float(db_istat)

            yaml_durata = adapter.get("durata_anni")
            db_durata = contract_ctx.get("contract", {}).get("durata_anni")
            final_durata = 3
            if str(yaml_durata).strip() == "-":
                final_durata = 3
            elif yaml_durata is not None and str(yaml_durata).strip() != "":
                final_durata = int(yaml_durata)
            elif db_durata is not None:
                final_durata = int(db_durata)

            yaml_arredato = adapter.get("arredato")
            db_arredato = contract_ctx.get("contract", {}).get("arredato_pct")
            final_arredato = 0.0
            if str(yaml_arredato).strip() == "-":
                final_arredato = 0.0
            elif yaml_arredato is not None and str(yaml_arredato).strip() != "":
                final_arredato = safe_float(yaml_arredato) or 0.0
            elif db_arredato is not None:
                final_arredato = float(db_arredato)

            yaml_ignore = adapter.get("ignore_surcharges")
            db_ignore = contract_ctx.get("contract", {}).get("ignore_surcharges")
            final_ignore = False
            if str(yaml_ignore).strip() == "-":
                final_ignore = False
            elif yaml_ignore is not None and str(yaml_ignore).strip() != "":
                final_ignore = str(yaml_ignore).lower() in ("true", "1", "yes", "y")
            elif db_ignore is not None:
                final_ignore = bool(db_ignore)

            can_in = CanoneInput(
                superficie_catastale=float(imm.superficie_totale or adapter.get("superficie_totale") or 0),
                micro_zona=clean_str(imm.micro_zona),
                foglio=clean_str(imm.foglio),
                categoria_catasto=clean_str(imm.categoria),
                classe_catasto=clean_str(imm.classe),
                count_a=cnt(["a1", "a2"]),
                count_b=cnt([f"b{i}" for i in range(1, 6)]),
                count_c=cnt([f"c{i}" for i in range(1, 8)]),
                count_d=cnt([f"d{i}" for i in range(1, 14)]),
                arredato=final_arredato,
                energy_class=final_energy,
                contract_kind=kind_enum,
                durata_anni=final_durata,
                istat=final_istat,
                ignore_surcharges=final_ignore,
            )

            canone_validation = validate_canone_input(can_in)
            emit_validation_messages(
                spider.logger,
                "[CANONE][VALIDATION]",
                canone_validation,
                emit_errors=False,
            )
            if not canone_validation.is_valid:
                raise CanoneInputValidationError.from_validation_result(
                    canone_validation,
                    fallback_message="Структурно некоректний canone input.",
                )

            logger.debug(
                "[CALC_INPUT] K=%s En=%s Arr=%s Dur=%s Ign=%s",
                kind_str,
                final_energy,
                final_arredato,
                final_durata,
                final_ignore,
            )

            can_res = self.calculation_strategy.calculate(can_in)
            canone_snapshot = prepare_for_json(can_in.__dict__)
            canone_result_snapshot = prepare_for_json(can_res.__dict__) if can_res else {}

            db_insert_canone_calc(
                conn,
                contract_id,
                self.calculation_strategy.code,
                inputs={"canone_input": canone_snapshot, "result": canone_result_snapshot},
                result_mensile=safe_float(getattr(can_res, "canone_finale_mensile", None)),
            )

            contract_ctx = db_load_contract_context(conn, contract_id)
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.CANONE_STAGE,
                    error=exc,
                    artifact_refs=[FailureArtifactRef.create("contract_id", str(contract_id))],
                )
            spider.logger.warning("[CANONE] Calculation skipped or failed for contract %s: %s", contract_id, exc)

        return CanoneStageResult(
            contract_ctx=contract_ctx,
            canone_snapshot=canone_snapshot,
            canone_result_snapshot=canone_result_snapshot,
        )


class AuditStageService:
    """Пише audit-лог для DOCX stage без зміни існуючого timing."""

    def __init__(
        self,
        *,
        runtime_config: VisuraProcessorRuntimeConfig,
        failure_recorder: FailureRegistryRecorder | None = None,
    ) -> None:
        """Зберігає runtime metadata, яка вже входить до current audit contract."""
        self.runtime_config = runtime_config
        self.failure_recorder = failure_recorder

    def log_generated(
        self,
        conn,
        contract_id: int,
        out_bucket: str,
        out_obj: str,
        params_snapshot: dict[str, Any],
        *,
        run_id: str,
        client_cf: str,
    ) -> None:
        """Пише current success-аудит після успішного generation/upload."""
        try:
            db_insert_attestazione_log(
                conn,
                contract_id,
                "generated",
                out_bucket,
                out_obj,
                params_snapshot=prepare_for_json(params_snapshot),
                error=None,
                author_login_masked=mask_username(self.runtime_config.ae_username),
                author_login_sha256=sha256_text(self.runtime_config.ae_username),
                template_version=self.runtime_config.template_version,
            )
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=client_cf,
                    stage=FailureStage.AUDIT_STAGE,
                    error=exc,
                    artifact_refs=[
                        FailureArtifactRef.create("contract_id", str(contract_id)),
                        FailureArtifactRef.create("audit_status", "generated"),
                        FailureArtifactRef.create("storage_object", out_obj),
                    ],
                )
            raise

    def log_failed(
        self,
        conn,
        contract_id: int,
        error: Exception | str,
        *,
        run_id: str,
        client_cf: str,
    ) -> None:
        """Пише current failed-аудит для generation/upload stage."""
        try:
            db_insert_attestazione_log(
                conn,
                contract_id,
                "failed",
                "",
                "",
                {"error_stage": "generation_or_upload"},
                error=str(error),
                author_login_masked=mask_username(self.runtime_config.ae_username),
                author_login_sha256=sha256_text(self.runtime_config.ae_username),
                template_version=self.runtime_config.template_version,
            )
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=client_cf,
                    stage=FailureStage.AUDIT_STAGE,
                    error=exc,
                    artifact_refs=[
                        FailureArtifactRef.create("contract_id", str(contract_id)),
                        FailureArtifactRef.create("audit_status", "failed"),
                    ],
                )
            raise


class DocumentStageService:
    """Генерує DOCX, завантажує його й тригерить audit у current order."""

    def __init__(
        self,
        *,
        storage: ObjectStorage,
        storage_service: StorageService,
        runtime_config: VisuraProcessorRuntimeConfig,
        template_path: Path,
        audit_stage: AuditStageService,
        failure_recorder: FailureRegistryRecorder | None = None,
    ) -> None:
        """Фіксує залежності generation/upload stage без зміни naming contract."""
        self.storage = storage
        self.storage_service = storage_service
        self.runtime_config = runtime_config
        self.template_path = template_path
        self.audit_stage = audit_stage
        self.failure_recorder = failure_recorder

    def run(
        self,
        conn,
        spider,
        adapter: ItemAdapter,
        *,
        run_id: str,
        imm: Immobile,
        contract_ctx: dict[str, Any],
        contract_id: int,
        immobile_id: int,
        locatore_cf: str,
        canone_result_snapshot: dict[str, Any] | None,
    ) -> Path | None:
        """Виконує current DOCX stage без зміни generation/upload/audit sequence."""
        params = build_template_params(adapter, imm, contract_ctx)
        output_path = get_attestazione_path(locatore_cf, contract_id, imm)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        out_bucket = self.storage.cfg.attestazioni_bucket
        out_obj = self.storage.attestazione_object_name(locatore_cf, contract_id)

        try:
            logger.debug("[DEBUG_ADDR] Contract CTX: %s", contract_ctx.get("immobile"))
            fill_attestazione_template(
                template_path=str(self.template_path),
                output_folder=str(output_path.parent),
                filename=output_path.name,
                params=params,
                underscored=underscored,
            )

            self.storage_service.upload_file(
                out_bucket,
                out_obj,
                output_path,
                content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

            params_snapshot = {
                "locatore_cf": locatore_cf,
                "immobile_id": immobile_id,
                "contract_id": contract_id,
                "yaml_item": dict(adapter.asdict()),
                "immobile_master_data": immobile_db_row(imm),
                "contract_ctx": contract_ctx,
                "template_version": self.runtime_config.template_version,
                "canone_result": canone_result_snapshot,
                "output": {"bucket": out_bucket, "object": out_obj},
            }

            self.audit_stage.log_generated(
                conn,
                contract_id,
                out_bucket,
                out_obj,
                params_snapshot,
                run_id=run_id,
                client_cf=locatore_cf,
            )
            return output_path
        except Exception as exc:
            if self.failure_recorder is not None:
                self.failure_recorder.record_failure(
                    run_id=run_id,
                    client_cf=locatore_cf,
                    stage=FailureStage.DOCUMENT_STAGE,
                    error=exc,
                    artifact_refs=[
                        FailureArtifactRef.create("contract_id", str(contract_id)),
                        FailureArtifactRef.create("local_output_path", output_path),
                        FailureArtifactRef.create("storage_object", out_obj),
                    ],
                )
            spider.logger.exception("[DOCX] Failed for contract %s", contract_id)
            self.audit_stage.log_failed(
                conn,
                contract_id,
                exc,
                run_id=run_id,
                client_cf=locatore_cf,
            )
            return None
