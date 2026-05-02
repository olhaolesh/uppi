# Web Backend Shell

Цей документ описує ізольований FastAPI backend shell, який став базою для
Stage 1-4 additive web/API slices.

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
- додає protected web adapters для search/prepare і generation;
- не дублює prepare/import/generation orchestration logic.

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

Attestazioni detail:

- [./web_attestazioni_search_prepare.md](./web_attestazioni_search_prepare.md)
- [./web_attestazioni_generation.md](./web_attestazioni_generation.md)

Bulk import detail:

- [./web_clients_bulk_import.md](./web_clients_bulk_import.md)

## Чого цей shell ще не робить

- немає AWS integration
- немає Docker/deployment foundation
- не чіпає AE/SISTER/browser flow
