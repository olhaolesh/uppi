"""Builds one canonical web-run generation YAML from prepared search output."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

import yaml

from uppi.config.app_config import project_root
from uppi.config.immobili import ELEMENT_KEYS, ImmobileConfig, ImmobiliDocumentConfig

if TYPE_CHECKING:
    from uppi.web.schemas.attestazioni import AttestazioniGenerateRequest


ROOT_CLIENT_UPDATE_FIELDS = (
    "locatore_comune_res",
    "locatore_via",
    "locatore_civico",
)
IMMOBILE_EDITABLE_FIELDS = (
    "immobile_comune",
    "immobile_via",
    "immobile_civico",
    "immobile_piano",
    "immobile_interno",
    "energy_class",
    "arredato",
    "istat",
    "ignore_surcharges",
    "contract_kind",
)
RUN_ONLY_FIELDS = (
    "conduttore_nome",
    "conduttore_cf",
    "conduttore_comune",
    "conduttore_via",
    "contratto_data",
    "decorrenza_data",
    "registrazione_data",
    "registrazione_num",
    "agenzia_entrate_sede",
    "canone_contrattuale_mensile",
    "durata_anni",
)
ELEMENT_KEY_SEQUENCE = (
    [f"A{i}" for i in range(1, 3)]
    + [f"B{i}" for i in range(1, 6)]
    + [f"C{i}" for i in range(1, 8)]
    + [f"D{i}" for i in range(1, 14)]
)


class UnsafePreparedYamlPathError(ValueError):
    """Raised when the requested prepared YAML path escapes the allowed subtree."""


class NoSelectedImmobilesError(ValueError):
    """Raised when the web payload does not enable any immobile for generation."""


class PreparedDocumentClientMismatchError(ValueError):
    """Raised when the prepared YAML owner does not match the requested CF."""


class PreparedImmobileIndexNotFoundError(ValueError):
    """Raised when the web payload references an immobile index not in prepared YAML."""


class PreparedImmobileIdentityMismatchError(ValueError):
    """Raised when payload identity differs from the prepared YAML identity."""


@dataclass(frozen=True)
class BuiltGenerationYaml:
    """Web-run generation input plus safe local artifact paths."""

    run_id: str
    locatore_cf: str
    prepared_output_path: Path
    generation_output_path: Path
    requested_count: int
    document: ImmobiliDocumentConfig


class GenerationYamlBuilder:
    """Transforms prepared search output into one canonical generation-only YAML."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        document_loader: Callable[[Path], ImmobiliDocumentConfig] | None = None,
    ) -> None:
        self.repo_root = Path(repo_root) if repo_root is not None else project_root()
        self.document_loader = document_loader or _default_document_loader
        self.allowed_prepared_root = self.repo_root / "clients" / "web_prepare"
        self.allowed_generation_root = self.repo_root / "clients" / "web_generation"

    def build(self, payload: "AttestazioniGenerateRequest") -> BuiltGenerationYaml:
        """Validates the prepared source path, applies allowed edits, and writes a web-run YAML."""
        locatore_cf = payload.locatore_cf
        prepared_output_path = self._resolve_prepared_output_path(
            locatore_cf=locatore_cf,
            prepared_immobili_yaml_path=payload.prepared_immobili_yaml_path,
        )
        prepared_document = self.document_loader(prepared_output_path)
        if prepared_document.locatore_cf.strip().upper() != locatore_cf:
            raise PreparedDocumentClientMismatchError(
                "Prepared YAML owner does not match the requested locatore CF."
            )

        requested_updates = {entry.index: entry for entry in payload.immobili}
        unknown_indexes = sorted(
            index
            for index in requested_updates
            if index < 1 or index > len(prepared_document.immobili)
        )
        if unknown_indexes:
            raise PreparedImmobileIndexNotFoundError(
                f"Prepared immobile index not found: {unknown_indexes[0]}."
            )

        next_immobili: list[ImmobileConfig] = []
        selected_count = 0
        for index, prepared_immobile in enumerate(prepared_document.immobili, start=1):
            requested_entry = requested_updates.get(index)
            if requested_entry is None:
                next_immobili.append(replace(prepared_immobile, enabled=False))
                continue

            self._assert_identity_matches(
                prepared_immobile=prepared_immobile,
                payload_identity=requested_entry.identity,
                index=index,
            )
            if requested_entry.enabled:
                selected_count += 1
                next_immobili.append(
                    self._apply_selected_immobile_updates(
                        prepared_immobile=prepared_immobile,
                        payload_entry=requested_entry,
                    )
                )
            else:
                next_immobili.append(replace(prepared_immobile, enabled=False))

        if selected_count <= 0:
            raise NoSelectedImmobilesError(
                "At least one immobile must stay enabled for generation."
            )

        next_document = replace(
            prepared_document,
            locatore_comune_res=self._resolve_root_field(
                prepared_document.locatore_comune_res,
                payload.client_updates,
                "locatore_comune_res",
            ),
            locatore_via=self._resolve_root_field(
                prepared_document.locatore_via,
                payload.client_updates,
                "locatore_via",
            ),
            locatore_civico=self._resolve_root_field(
                prepared_document.locatore_civico,
                payload.client_updates,
                "locatore_civico",
            ),
            immobili=tuple(next_immobili),
        )

        run_id = uuid4().hex
        generation_output_path = (
            self.allowed_generation_root / locatore_cf / run_id / "immobili.yml"
        )
        self._write_document(next_document, generation_output_path)
        validated_document = self.document_loader(generation_output_path)
        return BuiltGenerationYaml(
            run_id=run_id,
            locatore_cf=locatore_cf,
            prepared_output_path=prepared_output_path,
            generation_output_path=generation_output_path,
            requested_count=selected_count,
            document=validated_document,
        )

    def _resolve_prepared_output_path(
        self,
        *,
        locatore_cf: str,
        prepared_immobili_yaml_path: str | None,
    ) -> Path:
        raw_path = (prepared_immobili_yaml_path or "").strip()
        if not raw_path:
            candidate = self.allowed_prepared_root / locatore_cf / "immobili.yml"
        else:
            parsed = Path(raw_path)
            if parsed.is_absolute():
                raise UnsafePreparedYamlPathError(
                    "Absolute prepared YAML paths are not allowed."
                )
            candidate = (self.repo_root / parsed).resolve()

        allowed_root = self.allowed_prepared_root.resolve()
        if not candidate.is_relative_to(allowed_root):
            raise UnsafePreparedYamlPathError(
                "Prepared YAML path must stay under clients/web_prepare/."
            )
        return candidate

    def _assert_identity_matches(
        self,
        *,
        prepared_immobile: ImmobileConfig,
        payload_identity,
        index: int,
    ) -> None:
        prepared_identity = (
            _identity_value(prepared_immobile.foglio),
            _identity_value(prepared_immobile.numero),
            _identity_value(prepared_immobile.sub),
        )
        requested_identity = (
            _identity_value(payload_identity.foglio),
            _identity_value(payload_identity.numero),
            _identity_value(payload_identity.sub),
        )
        if prepared_identity != requested_identity:
            raise PreparedImmobileIdentityMismatchError(
                f"Prepared immobile identity mismatch at index {index}."
            )

    def _apply_selected_immobile_updates(
        self,
        *,
        prepared_immobile: ImmobileConfig,
        payload_entry,
    ) -> ImmobileConfig:
        editable_updates = _model_updates(payload_entry.editable)
        run_only_updates = _model_updates(payload_entry.run_only)
        next_elements = dict(prepared_immobile.elements)
        next_elements.update(payload_entry.elements)
        return replace(
            prepared_immobile,
            enabled=True,
            immobile_comune=editable_updates.get(
                "immobile_comune",
                prepared_immobile.immobile_comune,
            ),
            immobile_via=editable_updates.get(
                "immobile_via",
                prepared_immobile.immobile_via,
            ),
            immobile_civico=editable_updates.get(
                "immobile_civico",
                prepared_immobile.immobile_civico,
            ),
            immobile_piano=editable_updates.get(
                "immobile_piano",
                prepared_immobile.immobile_piano,
            ),
            immobile_interno=editable_updates.get(
                "immobile_interno",
                prepared_immobile.immobile_interno,
            ),
            energy_class=editable_updates.get(
                "energy_class",
                prepared_immobile.energy_class,
            ),
            arredato=editable_updates.get(
                "arredato",
                prepared_immobile.arredato,
            ),
            istat=editable_updates.get(
                "istat",
                prepared_immobile.istat,
            ),
            ignore_surcharges=editable_updates.get(
                "ignore_surcharges",
                prepared_immobile.ignore_surcharges,
            ),
            contract_kind=editable_updates.get(
                "contract_kind",
                prepared_immobile.contract_kind,
            ),
            conduttore_nome=run_only_updates.get(
                "conduttore_nome",
                prepared_immobile.conduttore_nome,
            ),
            conduttore_cf=run_only_updates.get(
                "conduttore_cf",
                prepared_immobile.conduttore_cf,
            ),
            conduttore_comune=run_only_updates.get(
                "conduttore_comune",
                prepared_immobile.conduttore_comune,
            ),
            conduttore_via=run_only_updates.get(
                "conduttore_via",
                prepared_immobile.conduttore_via,
            ),
            contratto_data=run_only_updates.get(
                "contratto_data",
                prepared_immobile.contratto_data,
            ),
            decorrenza_data=run_only_updates.get(
                "decorrenza_data",
                prepared_immobile.decorrenza_data,
            ),
            registrazione_data=run_only_updates.get(
                "registrazione_data",
                prepared_immobile.registrazione_data,
            ),
            registrazione_num=run_only_updates.get(
                "registrazione_num",
                prepared_immobile.registrazione_num,
            ),
            agenzia_entrate_sede=run_only_updates.get(
                "agenzia_entrate_sede",
                prepared_immobile.agenzia_entrate_sede,
            ),
            canone_contrattuale_mensile=run_only_updates.get(
                "canone_contrattuale_mensile",
                prepared_immobile.canone_contrattuale_mensile,
            ),
            durata_anni=run_only_updates.get(
                "durata_anni",
                prepared_immobile.durata_anni,
            ),
            elements=next_elements,
        )

    def _resolve_root_field(self, current_value: Any, updates_model, field_name: str) -> Any:
        updates = _model_updates(updates_model)
        return updates.get(field_name, current_value)

    def _write_document(self, document: ImmobiliDocumentConfig, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            yaml.safe_dump(
                _serialize_document(document),
                sort_keys=False,
                allow_unicode=True,
            ),
            encoding="utf-8",
        )


def _default_document_loader(path: Path) -> ImmobiliDocumentConfig:
    """Loads one canonical YAML document via the existing domain surface."""
    from uppi.domain.immobili_document import load_immobili_document

    return load_immobili_document(path=path)


def _identity_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _model_updates(model: Any) -> dict[str, Any]:
    raw = model.model_dump(exclude_unset=True)
    return {
        key: _normalize_payload_value(value)
        for key, value in raw.items()
        if value is not None
    }


def _normalize_payload_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def _serialize_document(document: ImmobiliDocumentConfig) -> dict[str, Any]:
    root = {
        "LOCATORE_CF": _serialize_scalar(document.locatore_cf),
        "COMUNE": _serialize_scalar(document.comune),
        "TIPO_CATASTO": _serialize_scalar(document.tipo_catasto),
        "UFFICIO_PROVINCIALE_LABEL": _serialize_scalar(document.ufficio_label),
        "LOCATORE_COMUNE_RES": _serialize_scalar(document.locatore_comune_res),
        "LOCATORE_VIA": _serialize_scalar(document.locatore_via),
        "LOCATORE_CIVICO": _serialize_scalar(document.locatore_civico),
        "immobili": [_serialize_immobile(immobile) for immobile in document.immobili],
    }
    if document.extra:
        root.update(document.extra)
    return root


def _serialize_immobile(immobile: ImmobileConfig) -> dict[str, Any]:
    payload = {
        "enabled": bool(immobile.enabled),
        "FOGLIO": _serialize_scalar(immobile.foglio),
        "NUMERO": _serialize_scalar(immobile.numero),
        "SUB": _serialize_scalar(immobile.sub),
        "RENDITA": _serialize_scalar(immobile.rendita),
        "SUPERFICIE_TOTALE": _serialize_scalar(immobile.superficie_totale),
        "CATEGORIA": _serialize_scalar(immobile.categoria),
        "VISURA_COMUNE": _serialize_scalar(immobile.visura_comune),
        "VISURA_VIA": _serialize_scalar(immobile.visura_via),
        "VISURA_CIVICO": _serialize_scalar(immobile.visura_civico),
        "IMMOBILE_COMUNE": _serialize_scalar(immobile.immobile_comune),
        "IMMOBILE_VIA": _serialize_scalar(immobile.immobile_via),
        "IMMOBILE_CIVICO": _serialize_scalar(immobile.immobile_civico),
        "IMMOBILE_PIANO": _serialize_scalar(immobile.immobile_piano),
        "IMMOBILE_INTERNO": _serialize_scalar(immobile.immobile_interno),
        "ENERGY_CLASS": _serialize_scalar(immobile.energy_class),
        "ARREDATO": _serialize_scalar(immobile.arredato),
        "ISTAT": _serialize_scalar(immobile.istat),
        "IGNORE_SURCHARGES": _serialize_scalar(immobile.ignore_surcharges),
        "CONTRACT_KIND": _serialize_scalar(immobile.contract_kind),
        "CONDUTTORE_NOME": _serialize_scalar(immobile.conduttore_nome),
        "CONDUTTORE_CF": _serialize_scalar(immobile.conduttore_cf),
        "CONDUTTORE_COMUNE": _serialize_scalar(immobile.conduttore_comune),
        "CONDUTTORE_VIA": _serialize_scalar(immobile.conduttore_via),
        "CONTRATTO_DATA": _serialize_scalar(immobile.contratto_data),
        "DECORRENZA_DATA": _serialize_scalar(immobile.decorrenza_data),
        "REGISTRAZIONE_DATA": _serialize_scalar(immobile.registrazione_data),
        "REGISTRAZIONE_NUM": _serialize_scalar(immobile.registrazione_num),
        "AGENZIA_ENTRATE_SEDE": _serialize_scalar(immobile.agenzia_entrate_sede),
        "CANONE_CONTRATTUALE_MENSILE": _serialize_scalar(immobile.canone_contrattuale_mensile),
        "DURATA_ANNI": _serialize_scalar(immobile.durata_anni),
    }
    for key in ELEMENT_KEY_SEQUENCE:
        payload[key] = _serialize_scalar(immobile.elements.get(key.lower()))
    if immobile.extra:
        payload.update(immobile.extra)
    return payload


def _serialize_scalar(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return float(value)
    return value


__all__ = [
    "BuiltGenerationYaml",
    "GenerationYamlBuilder",
    "NoSelectedImmobilesError",
    "PreparedDocumentClientMismatchError",
    "PreparedImmobileIdentityMismatchError",
    "PreparedImmobileIndexNotFoundError",
    "UnsafePreparedYamlPathError",
]
