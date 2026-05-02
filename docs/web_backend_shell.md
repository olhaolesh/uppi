# Web Backend Shell

Цей документ описує тільки мінімальний FastAPI backend shell для Етапу 1
web migration plan.

Canonical references:

- [./web_migration_baseline.md](./web_migration_baseline.md)
- [./architecture_decisions/web_service_foundation.md](./architecture_decisions/web_service_foundation.md)
- [./2_Uppi_Aws_Implementation_Plan.md](./2_Uppi_Aws_Implementation_Plan.md)

## Що це таке

Поточний shell:

- додає ізольований FastAPI app factory;
- публікує `GET /health/live` і `GET /health/ready`;
- читає web-specific config з env-first dataclass;
- має базовий cookie-based auth/session layer;
- не імпортує і не запускає prepare/import/generation business flows.

## Локальний запуск

Після встановлення залежностей:

```bash
uvicorn uppi.web.app:app --reload
```

Доступні endpoints:

- `GET /health/live`
- `GET /health/ready`

Auth/session detail:

- [./web_auth_session.md](./web_auth_session.md)

## Чого цей shell ще не робить

- немає search/prepare API
- немає generation API
- немає bulk import API
- немає AWS integration
- немає Docker/deployment foundation
- не чіпає AE/SISTER/browser flow
