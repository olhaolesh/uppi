# Frontend Bulk Import Integration

Цей документ описує Stage 8 інтеграцію екрана
`Додавання клієнтів в БД` у `frontend/`.

Canonical references:

- [./frontend_skeleton.md](./frontend_skeleton.md)
- [./web_clients_bulk_import.md](./web_clients_bulk_import.md)
- [./web_auth_session.md](./web_auth_session.md)

## Що інтегровано

Екран `/clients/bulk-import` тепер викликає:

- `POST /clients/bulk-import`

Flow:

1. оператор вставляє CSV у textarea або читає локальний `.csv` файл у браузері;
2. frontend формує JSON payload з `csv_content`;
3. оператор задає `force_update_visura` і `fail_fast`;
4. frontend викликає protected backend endpoint;
5. UI показує synchronous result: `status`, `run_id`, input info, summary, row results,
   invalid rows і messages.

## Auth і session

- для цього екрана потрібна активна web session;
- frontend використовує `credentials: "include"`;
- CSV content не зберігається у `localStorage` або `sessionStorage`.

## Payload

Frontend передає JSON, а не multipart upload:

```json
{
  "csv_content": "LOCATORE_CF\nRSSMRA80A01H501Z\nBNCLGU85C01G482K\n",
  "force_update_visura": false,
  "fail_fast": false
}
```

## Межі цього slice

- frontend не створює `immobili.yml`;
- endpoint не запускає generation;
- endpoint не викликає `prepare-by-CF`;
- frontend не керує browser/import flow напряму;
- current bulk import owner service може reuse-ити import/browser path internally,
  але це лишається backend responsibility;
- jobs/logs/artifacts model лишається окремим Stage 9;
- Docker/AWS/IaC у цьому slice не додаються.

## Локальний запуск

```bash
cd frontend
pnpm install
pnpm dev
```

Frontend читає API base URL з:

- `VITE_UPPI_API_BASE_URL`

За замовчуванням використовується локальний backend shell.
