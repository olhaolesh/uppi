# Frontend Skeleton

Цей документ описує Stage 6 frontend skeleton у `frontend/`.

Canonical references:

- [./web_migration_baseline.md](./web_migration_baseline.md)
- [./architecture_decisions/web_service_foundation.md](./architecture_decisions/web_service_foundation.md)
- [./web_backend_shell.md](./web_backend_shell.md)
- [./web_auth_session.md](./web_auth_session.md)

## Що створено

- окремий frontend app на Vite + React + TypeScript
- login/session-aware shell
- три protected screens:
  - `/attestazioni/generate`
  - `/clients/bulk-import`
  - `/jobs`
- typed API client layer
- локальний dev proxy до backend shell без змін browser-critical runtime

## Локальний запуск

Потрібен Node.js та сумісний package manager.

```bash
cd frontend
pnpm install
pnpm dev
```

Додаткові команди:

```bash
pnpm build
pnpm test
```

## Env

- `VITE_UPPI_API_BASE_URL`

Default:

- `http://localhost:8000`

У dev-режимі Vite proxy використовує цей target, щоб login/session міг працювати без
окремого backend CORS slice.

## Що вже інтегровано

- `POST /auth/login`
- `GET /auth/me`
- `POST /auth/logout`
- реальний Stage 7 flow для екрана `Згенерувати Attestazione`
  через `POST /attestazioni/search` і `POST /attestazioni/generate`
- реальний Stage 8 flow для екрана `Додавання клієнтів в БД`
  через `POST /clients/bulk-import`
- реальний Stage 9 flow для екрана `Статус / Логи / Артефакти`
  через `GET /jobs` і `GET /jobs/{run_id}`

Frontend використовує `credentials: "include"` і не зберігає password/pin у
`localStorage` або `sessionStorage`.

Stage 7 detail:

- [./frontend_attestazione_integration.md](./frontend_attestazione_integration.md)

Stage 8 detail:

- [./frontend_bulk_import_integration.md](./frontend_bulk_import_integration.md)

Stage 9 detail:

- [./web_jobs_status_artifacts.md](./web_jobs_status_artifacts.md)

## Що поки не реалізовано

- async queue / background jobs
- polling, WebSocket або SSE live logs
- artifact download endpoint
- signed URLs або storage delivery layer

## Чого цей slice не робить

- не додає Docker/AWS/IaC
- не додає SSM або Secrets Manager clients
- не змінює backend business/runtime logic
- не змінює AE/SISTER/browser flow
- не змінює `state.json` lifecycle
