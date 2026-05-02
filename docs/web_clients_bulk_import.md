# Web Clients Bulk Import

Цей документ описує Stage 5 adapter endpoint:

- `POST /clients/bulk-import`

Canonical references:

- [./web_migration_baseline.md](./web_migration_baseline.md)
- [./runtime_flow.md](./runtime_flow.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)

## Що робить endpoint

Endpoint:

- є protected і потребує active web session;
- приймає CSV як JSON `csv_content`, без multipart upload;
- записує web-run `clients.csv` у repo-local path;
- делегує виконання в current bulk import owner path;
- повертає synchronous MVP summary з row results і invalid rows;
- лишається import-only boundary.

## Request

```json
{
  "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
  "force_update_visura": false,
  "fail_fast": false
}
```

## Response

Успішний response містить:

- `status`
- `run_id`
- `input.clients_csv_path`
- `input.force_update_visura`
- `input.fail_fast`
- `summary`
- `results`
- `invalid_rows`
- `messages`

`status` дорівнює:

- `completed`, якщо current bulk import service завершив run without abort;
- `aborted`, якщо `fail_fast=true` і current import-only owner path зупинився на
  першій помилці.

## Важливі межі цього slice

- endpoint записує `clients/web_bulk_import/<RUN_ID>/clients.csv`;
- canonical `clients/clients.csv` не змінюється;
- web layer не викликає `prepare-by-CF`;
- endpoint не створює `immobili.yml`;
- endpoint не запускає generation;
- bulk import service може internally reuse-ити protected browser/import flow, але
  web layer ним напряму не керує;
- multipart upload поки не реалізований;
- job/status model ще не реалізований;
- AWS/SSM/ECS/IaC у цьому slice не реалізуються;
- `state.json` lifecycle не змінюється.
