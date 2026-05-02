# Web Auth And Session

Цей документ описує Stage 2 auth/session layer для additive FastAPI backend
shell.

Canonical references:

- [./web_migration_baseline.md](./web_migration_baseline.md)
- [./architecture_decisions/web_service_foundation.md](./architecture_decisions/web_service_foundation.md)
- [./web_backend_shell.md](./web_backend_shell.md)
- [./2_Uppi_Aws_Implementation_Plan.md](./2_Uppi_Aws_Implementation_Plan.md)

## Доступні endpoints

- `POST /auth/login`
- `GET /auth/me`
- `POST /auth/logout`

Health endpoints залишаються public:

- `GET /health/live`
- `GET /health/ready`

## Потрібні env змінні

- `UPPI_WEB_APP_NAME`
- `UPPI_WEB_APP_VERSION`
- `UPPI_WEB_ENV`
- `UPPI_WEB_DEBUG`
- `UPPI_WEB_AUTH_USERNAME`
- `UPPI_WEB_AUTH_PASSWORD`
- `UPPI_WEB_AUTH_PIN`
- `UPPI_WEB_SESSION_SECRET`
- `UPPI_WEB_SESSION_COOKIE_NAME`
- `UPPI_WEB_SESSION_COOKIE_SECURE`
- `UPPI_WEB_SESSION_MAX_AGE_SECONDS`

Для `local` / `test` / `dev` shell допускає safe development defaults для auth
credentials і session secret. Для production-like environment session secret і
auth credentials мають бути задані явно.

## Що важливо для MVP

- credentials не зберігаються у frontend
- Stage 2 не додає AWS SDK
- Stage 2 не читає SSM Parameter Store напряму
- майбутній production/MVP config має приходити через AWS Systems Manager
  Parameter Store в окремому deployment/config-provider slice
- AWS Secrets Manager лишається optional future path
- web logout не є AE/SISTER logout
- web logout не змінює `state.json` lifecycle

## Чого ще немає

- немає search/prepare API
- немає generation API
- немає bulk import API
- немає Docker/deployment foundation
- немає AWS integration clients
