"""Tests for the Stage 3 prepare/search adapter seam."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.web.services.prepare_adapter import PrepareSearchAdapter


def _raw_document(locatore_cf: str = "RSSMRA80A01H501Z") -> dict:
    """Returns a minimal canonical single-client document fixture."""
    return {
        "LOCATORE_CF": locatore_cf,
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
                "IGNORE_SURCHARGES": "",
                "CONTRACT_KIND": "",
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
            }
        ],
    }


def test_prepare_search_adapter_uses_deterministic_web_output_and_default_force_flag(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    calls = {}
    document = ImmobiliDocumentConfig.from_raw(_raw_document())

    class FakePrepareService:
        def prepare(self, locatore_cf, *, force_update_visura=False, output_path=None):
            calls["locatore_cf"] = locatore_cf
            calls["force_update_visura"] = force_update_visura
            calls["output_path"] = Path(output_path)
            return SimpleNamespace(
                locatore_cf=locatore_cf,
                output_path=Path(output_path),
                decision="db_hit_no_force",
                db_hit_before_import=True,
                import_performed=False,
            )

    loaded_paths: list[Path] = []

    adapter = PrepareSearchAdapter(
        repo_root=tmp_path,
        prepare_service_factory=lambda: FakePrepareService(),
        document_loader=lambda path: loaded_paths.append(Path(path)) or document,
    )

    result = adapter.prepare_search(" rssmra80a01h501z ")

    expected_path = tmp_path / "clients" / "web_prepare" / "RSSMRA80A01H501Z" / "immobili.yml"
    assert calls == {
        "locatore_cf": "RSSMRA80A01H501Z",
        "force_update_visura": False,
        "output_path": expected_path,
    }
    assert loaded_paths == [expected_path]
    assert result.output_path == expected_path
    assert result.output_path_relative == "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml"
    assert result.source == "db"
    assert result.document == document


def test_prepare_search_adapter_maps_imported_result_to_sister_source(tmp_path):
    """Перевіряє сценарій, описаний у назві тесту."""
    document = ImmobiliDocumentConfig.from_raw(_raw_document())

    class FakePrepareService:
        def prepare(self, locatore_cf, *, force_update_visura=False, output_path=None):
            return SimpleNamespace(
                locatore_cf=locatore_cf,
                output_path=Path(output_path),
                decision="db_miss_imported",
                db_hit_before_import=False,
                import_performed=True,
            )

    adapter = PrepareSearchAdapter(
        repo_root=tmp_path,
        prepare_service_factory=lambda: FakePrepareService(),
        document_loader=lambda path: document,
    )

    result = adapter.prepare_search("RSSMRA80A01H501Z", force_update_visura=True)

    assert result.source == "sister"
    assert result.import_performed is True
    assert result.db_hit_before_import is False
