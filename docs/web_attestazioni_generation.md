# Web Attestazioni Generation

Цей документ описує Stage 4 adapter endpoint:

- `POST /attestazioni/generate`

Canonical references:

- [./web_migration_baseline.md](./web_migration_baseline.md)
- [./web_attestazioni_search_prepare.md](./web_attestazioni_search_prepare.md)
- [./document_generation.md](./document_generation.md)

## Що робить endpoint

Endpoint:

- є protected і потребує active web session;
- працює тільки після `POST /attestazioni/search` і prepared
  `clients/web_prepare/<LOCATORE_CF>/immobili.yml`;
- приймає operator edits і будує окремий web-run generation input YAML;
- делегує виконання в current generation-only path;
- повертає synchronous MVP response з `run_id`, counters, messages і safe
  artifact refs;
- не викликає prepare;
- не реалізує bulk import API;
- не створює job/status model.

## Request

```json
{
  "locatore_cf": "RSSMRA80A01H501Z",
  "prepared_immobili_yaml_path": "clients/web_prepare/RSSMRA80A01H501Z/immobili.yml",
  "client_updates": {
    "locatore_comune_res": "PESCARA",
    "locatore_via": "VIA ROMA",
    "locatore_civico": "10"
  },
  "immobili": [
    {
      "index": 1,
      "enabled": true,
      "identity": {
        "foglio": "12",
        "numero": "345",
        "sub": "7"
      },
      "editable": {
        "immobile_comune": "PESCARA",
        "immobile_via": "VIA ROMA",
        "immobile_civico": "10",
        "immobile_piano": "1",
        "immobile_interno": "2",
        "energy_class": "G",
        "arredato": "SI",
        "istat": "",
        "ignore_surcharges": false,
        "contract_kind": "ordinario"
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
        "durata_anni": "4"
      },
      "elements": {
        "a1": "X",
        "b1": "",
        "c1": "",
        "d1": ""
      }
    }
  ]
}
```

## Response

Успішний response містить:

- `status`
- `run_id`
- `locatore_cf`
- `input.prepared_immobili_yaml_path`
- `input.generation_immobili_yaml_path`
- `summary.requested_count`
- `summary.generated_count`
- `summary.failed_count`
- `artifacts`
- `messages`

MVP artifact refs:

- `local_path` повертається тільки якщо current generation path реально створив
  DOCX локально;
- `bucket` і `object_key` повертаються тільки якщо їх можна безпечно отримати з
  current storage naming contract;
- `download_url` поки `null`;
- окремий download endpoint у цьому slice не додається.

## Важливі межі цього slice

- endpoint не ходить у SISTER;
- endpoint не викликає import/browser path;
- endpoint не запускає prepare;
- web layer не керує `state.json` lifecycle;
- document output naming contract не змінюється;
- document pipeline order не змінюється;
- validation і `"-"` semantics не змінюються;
- AWS/SSM/ECS/IaC у цьому slice не реалізуються.

## Поточні config assumptions

- Stage 2 auth/session лишається єдиним web auth boundary;
- prepared YAML має лежати під `clients/web_prepare/`;
- web-run generation YAML пишеться під
  `clients/web_generation/<LOCATORE_CF>/<RUN_ID>/immobili.yml`;
- current generation-only path лишається synchronous MVP execution seam.
