# Frontend Attestazione Integration

Цей документ описує Stage 7 інтеграцію екрана
`/attestazioni/generate`.

Canonical references:

- [./frontend_skeleton.md](./frontend_skeleton.md)
- [./web_attestazioni_search_prepare.md](./web_attestazioni_search_prepare.md)
- [./web_attestazioni_generation.md](./web_attestazioni_generation.md)
- [./web_auth_session.md](./web_auth_session.md)

## Що інтегровано

Екран `Згенерувати Attestazione` тепер виконує реальний flow:

1. `search` через `POST /attestazioni/search`
2. operator edits / immobile selection у React state
3. `generate` через `POST /attestazioni/generate`
4. synchronous result view з `run_id`, summary, messages і artifact refs

## Які backend endpoints використовуються

- `POST /auth/login`
- `GET /auth/me`
- `POST /auth/logout`
- `POST /attestazioni/search`
- `POST /attestazioni/generate`

Екран потребує active web session.

## Важливі межі

- frontend не зберігає response data у `localStorage` або `sessionStorage`
- frontend не ходить у SISTER напряму
- frontend не керує browser/import flow напряму
- backend runtime/business logic не переписується
- artifact download link з’являється тільки якщо backend реально повернув `download_url`
- якщо `download_url` дорівнює `null`, UI показує лише технічні refs

## Що ще не інтегровано

- bulk import screen лишається Stage 8
- jobs/logs/artifacts backend model лишається Stage 9
- download endpoint окремо не додається в цьому slice
- Docker/AWS/IaC не реалізуються в цьому slice

## Локальний запуск

```bash
cd frontend
pnpm install
pnpm dev
```

Потрібний backend shell:

```bash
uvicorn uppi.web.app:app --reload
```

Env:

- `VITE_UPPI_API_BASE_URL`

Default:

- `http://localhost:8000`
