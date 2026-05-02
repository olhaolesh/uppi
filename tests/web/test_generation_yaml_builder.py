"""Tests for Stage 4 web-run generation YAML building."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from uppi.domain.exceptions import YamlInputValidationError
from uppi.web.schemas.attestazioni import AttestazioniGenerateRequest
from uppi.web.services.generation_yaml_builder import (
    GenerationYamlBuilder,
    PreparedImmobileIdentityMismatchError,
    PreparedImmobileIndexNotFoundError,
    UnsafePreparedYamlPathError,
)


OWNER_CF = "RSSMRA80A01H501Z"


def _write_prepared_yaml(repo_root: Path) -> Path:
    prepared_path = repo_root / "clients" / "web_prepare" / OWNER_CF / "immobili.yml"
    prepared_path.parent.mkdir(parents=True, exist_ok=True)
    prepared_path.write_text(
        yaml.safe_dump(
            {
                "LOCATORE_CF": OWNER_CF,
                "COMUNE": "PESCARA",
                "TIPO_CATASTO": "F",
                "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
                "LOCATORE_COMUNE_RES": "",
                "LOCATORE_VIA": "",
                "LOCATORE_CIVICO": "",
                "immobili": [
                    {
                        "enabled": True,
                        "FOGLIO": "12",
                        "NUMERO": "345",
                        "SUB": "7",
                        "RENDITA": "EUR 123.45",
                        "SUPERFICIE_TOTALE": 98.7,
                        "CATEGORIA": "A/2",
                        "VISURA_COMUNE": "PESCARA",
                        "VISURA_VIA": "VIA ROMA",
                        "VISURA_CIVICO": "10",
                        "IMMOBILE_COMUNE": "",
                        "IMMOBILE_VIA": "",
                        "IMMOBILE_CIVICO": "",
                        "IMMOBILE_PIANO": "",
                        "IMMOBILE_INTERNO": "",
                        "ENERGY_CLASS": "",
                        "ARREDATO": "",
                        "ISTAT": "",
                        "IGNORE_SURCHARGES": False,
                        "CONTRACT_KIND": "ordinario",
                        "CONDUTTORE_NOME": "",
                        "CONDUTTORE_CF": "",
                        "CONDUTTORE_COMUNE": "",
                        "CONDUTTORE_VIA": "",
                        "CONTRATTO_DATA": "",
                        "DECORRENZA_DATA": "",
                        "REGISTRAZIONE_DATA": "",
                        "REGISTRAZIONE_NUM": "",
                        "AGENZIA_ENTRATE_SEDE": "",
                        "CANONE_CONTRATTUALE_MENSILE": "",
                        "DURATA_ANNI": "",
                        "A1": "",
                        "B1": "",
                        "C1": "",
                        "D1": "",
                    },
                    {
                        "enabled": True,
                        "FOGLIO": "13",
                        "NUMERO": "99",
                        "SUB": "1",
                        "RENDITA": "EUR 99.00",
                        "SUPERFICIE_TOTALE": 55.0,
                        "CATEGORIA": "A/3",
                        "VISURA_COMUNE": "PESCARA",
                        "VISURA_VIA": "VIA MAZZINI",
                        "VISURA_CIVICO": "22",
                        "IMMOBILE_COMUNE": "PESCARA",
                        "IMMOBILE_VIA": "VIA MAZZINI",
                        "IMMOBILE_CIVICO": "22",
                        "IMMOBILE_PIANO": "",
                        "IMMOBILE_INTERNO": "",
                        "ENERGY_CLASS": "",
                        "ARREDATO": "",
                        "ISTAT": "",
                        "IGNORE_SURCHARGES": False,
                        "CONTRACT_KIND": "ordinario",
                        "CONDUTTORE_NOME": "",
                        "CONDUTTORE_CF": "",
                        "CONDUTTORE_COMUNE": "",
                        "CONDUTTORE_VIA": "",
                        "CONTRATTO_DATA": "",
                        "DECORRENZA_DATA": "",
                        "REGISTRAZIONE_DATA": "",
                        "REGISTRAZIONE_NUM": "",
                        "AGENZIA_ENTRATE_SEDE": "",
                        "CANONE_CONTRATTUALE_MENSILE": "",
                        "DURATA_ANNI": "",
                        "A1": "",
                        "B1": "",
                        "C1": "",
                        "D1": "",
                    },
                ],
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return prepared_path


def _make_payload(**overrides) -> AttestazioniGenerateRequest:
    payload = {
        "locatore_cf": OWNER_CF,
        "prepared_immobili_yaml_path": f"clients/web_prepare/{OWNER_CF}/immobili.yml",
        "client_updates": {
            "locatore_comune_res": "PESCARA",
            "locatore_via": "VIA ROMA",
            "locatore_civico": "10",
        },
        "immobili": [
            {
                "index": 1,
                "enabled": True,
                "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                "editable": {
                    "immobile_comune": "PESCARA",
                    "immobile_via": "VIA ROMA",
                    "immobile_civico": "10",
                    "immobile_piano": "1",
                    "immobile_interno": "2",
                    "energy_class": "G",
                    "arredato": "SI",
                    "istat": "",
                    "ignore_surcharges": False,
                    "contract_kind": "ordinario",
                },
                "run_only": {
                    "conduttore_nome": "Mario Rossi",
                    "conduttore_cf": "RSSMRA80A01H501Z",
                    "conduttore_comune": "PESCARA",
                    "conduttore_via": "VIA VERDI 3",
                    "contratto_data": "2026-05-02",
                    "decorrenza_data": "2026-06-01",
                    "registrazione_data": "2026-05-10",
                    "registrazione_num": "12345",
                    "agenzia_entrate_sede": "PESCARA",
                    "canone_contrattuale_mensile": "500",
                    "durata_anni": "4",
                },
                "elements": {"a1": "X", "b1": "", "c1": "", "d1": ""},
            }
        ],
    }
    payload.update(overrides)
    return AttestazioniGenerateRequest.model_validate(payload)


def test_generation_yaml_builder_writes_canonical_web_run_yaml_and_preserves_protected_fields(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    prepared_path = _write_prepared_yaml(tmp_path)
    prepared_before = prepared_path.read_text(encoding="utf-8")
    builder = GenerationYamlBuilder(repo_root=tmp_path)

    built = builder.build(_make_payload())

    assert built.prepared_output_path == prepared_path
    assert built.generation_output_path.relative_to(tmp_path).as_posix().startswith(
        f"clients/web_generation/{OWNER_CF}/"
    )
    assert prepared_path.read_text(encoding="utf-8") == prepared_before

    document = built.document
    assert document.locatore_cf == OWNER_CF
    assert document.comune == "PESCARA"
    assert document.tipo_catasto == "F"
    assert document.ufficio_label == "PESCARA Territorio"
    assert document.locatore_comune_res == "PESCARA"
    assert document.locatore_via == "VIA ROMA"
    assert document.locatore_civico == "10"

    first = document.immobili[0]
    second = document.immobili[1]

    assert first.enabled is True
    assert first.foglio == "12"
    assert first.numero == "345"
    assert first.sub == "7"
    assert first.visura_via == "VIA ROMA"
    assert first.immobile_piano == "1"
    assert first.immobile_interno == "2"
    assert first.energy_class == "G"
    assert first.arredato == "SI"
    assert first.ignore_surcharges is False
    assert first.conduttore_nome == "Mario Rossi"
    assert first.conduttore_via == "VIA VERDI 3"
    assert first.canone_contrattuale_mensile == "500"
    assert first.elements["a1"] == "X"

    assert second.enabled is False
    assert second.foglio == "13"
    assert second.numero == "99"
    assert second.sub == "1"
    assert second.visura_via == "VIA MAZZINI"


def test_generation_yaml_builder_rejects_unsafe_path_index_mismatch_and_identity_drift(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    _write_prepared_yaml(tmp_path)
    builder = GenerationYamlBuilder(repo_root=tmp_path)

    with pytest.raises(UnsafePreparedYamlPathError):
        builder.build(
            _make_payload(prepared_immobili_yaml_path="../outside/immobili.yml")
        )

    with pytest.raises(PreparedImmobileIndexNotFoundError):
        builder.build(
            _make_payload(
                immobili=[
                    {
                        "index": 99,
                        "enabled": True,
                        "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                    }
                ]
            )
        )

    with pytest.raises(PreparedImmobileIdentityMismatchError):
        builder.build(
            _make_payload(
                immobili=[
                    {
                        "index": 1,
                        "enabled": True,
                        "identity": {"foglio": "12", "numero": "999", "sub": "7"},
                    }
                ]
            )
        )


def test_generation_yaml_builder_reuses_existing_yaml_validation_for_contract_kind_clear_rejection(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    _write_prepared_yaml(tmp_path)
    builder = GenerationYamlBuilder(repo_root=tmp_path)

    with pytest.raises(YamlInputValidationError):
        builder.build(
            _make_payload(
                immobili=[
                    {
                        "index": 1,
                        "enabled": True,
                        "identity": {"foglio": "12", "numero": "345", "sub": "7"},
                        "editable": {"contract_kind": "-"},
                    }
                ]
            )
        )
