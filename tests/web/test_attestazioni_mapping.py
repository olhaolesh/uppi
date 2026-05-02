"""Tests for Stage 3 DTO mapping from prepared `immobili.yml` data."""

from __future__ import annotations

from pathlib import Path

from uppi.config.immobili import ImmobiliDocumentConfig
from uppi.web.schemas.attestazioni import AttestazioniSearchResponse
from uppi.web.services.prepare_adapter import PreparedSearchResult


def test_attestazioni_search_response_separates_identity_visura_editable_run_only_and_elements():
    """Перевіряє сценарій, описаний у назві тесту."""
    document = ImmobiliDocumentConfig.from_raw(
        {
            "LOCATORE_CF": "RSSMRA80A01H501Z",
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
                    "IMMOBILE_COMUNE": "PESCARA",
                    "IMMOBILE_VIA": "VIA ROMA",
                    "IMMOBILE_CIVICO": "10",
                    "IMMOBILE_PIANO": "2",
                    "IMMOBILE_INTERNO": "A",
                    "ENERGY_CLASS": "A1",
                    "ARREDATO": "SI",
                    "ISTAT": "",
                    "IGNORE_SURCHARGES": False,
                    "CONTRACT_KIND": "4+4",
                    "CONDUTTORE_NOME": "Mario Rossi",
                    "CONDUTTORE_CF": "RSSMRA80A01H501Z",
                    "CONDUTTORE_COMUNE": "PESCARA",
                    "CONDUTTORE_VIA": "VIA TEST",
                    "CONTRATTO_DATA": "2024-01-01",
                    "DECORRENZA_DATA": "2024-02-01",
                    "REGISTRAZIONE_DATA": "2024-02-10",
                    "REGISTRAZIONE_NUM": "12345",
                    "AGENZIA_ENTRATE_SEDE": "PESCARA",
                    "CANONE_CONTRATTUALE_MENSILE": "1000",
                    "DURATA_ANNI": "4",
                    "A1": "alpha",
                    "B1": "beta",
                    "C1": "gamma",
                    "D1": "delta",
                }
            ],
        }
    )
    result = PreparedSearchResult(
        locatore_cf="RSSMRA80A01H501Z",
        output_path=Path("/tmp/immobili.yml"),
        output_path_relative="clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
        decision="db_hit_no_force",
        db_hit_before_import=True,
        import_performed=False,
        source="db",
        document=document,
    )

    response = AttestazioniSearchResponse.from_prepared_result(result)
    payload = response.model_dump()

    assert payload["status"] == "prepared"
    assert payload["source"] == "db"
    assert payload["client"] == {
        "locatore_cf": "RSSMRA80A01H501Z",
        "comune": "PESCARA",
        "tipo_catasto": "F",
        "ufficio_label": "PESCARA Territorio",
        "locatore_comune_res": "",
        "locatore_via": "",
        "locatore_civico": "",
    }
    assert payload["document"] == {
        "immobili_yaml_path": "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
        "immobili_count": 1,
        "active_count": 1,
    }
    assert payload["immobili"][0]["identity"] == {
        "foglio": "12",
        "numero": "345",
        "sub": "7",
    }
    assert payload["immobili"][0]["visura"] == {
        "rendita": "EUR 123.45",
        "superficie_totale": 98.7,
        "categoria": "A/2",
        "visura_comune": "PESCARA",
        "visura_via": "VIA ROMA",
        "visura_civico": "10",
    }
    assert payload["immobili"][0]["editable"] == {
        "immobile_comune": "PESCARA",
        "immobile_via": "VIA ROMA",
        "immobile_civico": "10",
        "immobile_piano": "2",
        "immobile_interno": "A",
        "energy_class": "A1",
        "arredato": "SI",
        "istat": "",
        "ignore_surcharges": False,
        "contract_kind": "4+4",
    }
    assert payload["immobili"][0]["run_only"] == {
        "conduttore_nome": "Mario Rossi",
        "conduttore_cf": "RSSMRA80A01H501Z",
        "conduttore_comune": "PESCARA",
        "conduttore_via": "VIA TEST",
        "contratto_data": "2024-01-01",
        "decorrenza_data": "2024-02-01",
        "registrazione_data": "2024-02-10",
        "registrazione_num": "12345",
        "agenzia_entrate_sede": "PESCARA",
        "canone_contrattuale_mensile": "1000",
        "durata_anni": "4",
    }
    assert payload["immobili"][0]["elements"]["a1"] == "alpha"
    assert payload["immobili"][0]["elements"]["b1"] == "beta"
    assert payload["immobili"][0]["elements"]["c1"] == "gamma"
    assert payload["immobili"][0]["elements"]["d1"] == "delta"
