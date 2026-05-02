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

Frontend використовує `credentials: "include"` і не зберігає password/pin у
`localStorage` або `sessionStorage`.

## Що поки skeleton/mock-only

- екран `Згенерувати Attestazione` не викликає real `POST /attestazioni/search`
- екран `Згенерувати Attestazione` не викликає real `POST /attestazioni/generate`
- екран `Додавання клієнтів в БД` не викликає real `POST /clients/bulk-import`
- екран `Статус / Логи / Артефакти` не читає real jobs/logs/artifacts data

План інтеграції:

- реальна інтеграція екрана Attestazione: Stage 7
- реальна інтеграція bulk import екрана: Stage 8
- job/status/logs/artifacts model: Stage 9

## Чого цей slice не робить

- не додає Docker/AWS/IaC
- не додає SSM або Secrets Manager clients
- не змінює backend business/runtime logic
- не змінює AE/SISTER/browser flow
- не змінює `state.json` lifecycle
