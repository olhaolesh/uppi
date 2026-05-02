# Web Attestazioni Search / Prepare

Цей документ описує Stage 3 adapter endpoint:

- `POST /attestazioni/search`

## Що робить endpoint

Endpoint:

- є protected і потребує active web session;
- приймає `locatore_cf` і optional `force_update_visura`;
- делегує в current `prepare-by-CF` owner path;
- повертає frontend-friendly DTO з client data, prepared `immobili.yml` path і
  grouped immobile fields;
- не запускає generation;
- не створює DOCX;
- не реалізує bulk import API.

## Request

```json
{
  "locatore_cf": "RSSMRA80A01H501Z",
  "force_update_visura": false
}
```

## Response

Успішний response містить:

- `status`
- `source`
- `client`
- `document`
- `immobili`
- `messages`

`source` повертається як:

- `db`, якщо prepare не виконував import refresh
- `sister`, якщо prepare виконав import refresh
- `unknown`, якщо current prepare result не дає безпечного висновку

## Важливі межі цього slice

- web layer не вирішує DB hit / DB miss / force refresh
- web layer не керує browser flow напряму
- current prepare service за потреби може reuse-ити protected import/browser path
- `state.json` lifecycle не змінюється
- AE/SISTER/browser-critical flow не змінюється
- AWS/SSM/ECS не реалізуються в цьому slice

## Поточні config assumptions

- endpoint працює поверх уже створеного web shell
- endpoint покладається на current Stage 2 auth/session model
- endpoint не додає SSM client або Secrets Manager client
- current prepare output для web API записується в deterministic repo-local path
  під `clients/web_prepare/<LOCATORE_CF>/immobili.yml`
