# Backend Containerization Foundation

Цей документ фіксує Stage 10A backend containerization foundation для UPPI.
Це лише packaging/runtime slice. Він не додає AWS resources, IaC, ECS task
definition або secret-provider integration.

## Що додано

- `Dockerfile.backend`
- `.dockerignore`
- `scripts/docker/start_backend.sh`
- `scripts/docker/smoke_backend_container.sh`
- `.env.backend.example`

## Base image

Використовується:

- `mcr.microsoft.com/playwright/python:v1.55.0-noble`

Причина:

- у `requirements.txt` зафіксовано `playwright==1.55.0`
- current runtime already depends on Playwright/browser system dependencies
- для Stage 10A важливо отримати production-oriented browser-compatible base
  image без крихкого ручного `apt install`

## Що контейнер запускає

Container запускає web backend:

```bash
uvicorn uppi.web.app:app --host 0.0.0.0 --port 8000
```

Це не змінює runtime semantics існуючих CLI або web endpoints.

## Writable runtime dirs

Container foundation створює або очікує writable local filesystem для:

- `downloads/`
- `captcha_images/`
- `logs/`
- `clients/web_prepare/`
- `clients/web_generation/`
- `clients/web_bulk_import/`
- `clients/web_jobs/`

Важливо:

- current runtime ще не є повністю stateless
- ці директорії мають лишатися task-local / ephemeral
- `state.json` не переноситься у shared volume policy
- `state.json` не можна шарити між tasks, sessions або паралельними runs

## Env contract

### Web / auth / session

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

### Database

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSL_MODE`

### Object storage / S3-compatible storage

- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_SECURE`
- `VISURE_BUCKET`
- `ATTESTAZIONI_BUCKET`

Legacy fallback still exists in code:

- `MINIO_BUCKET`

Але primary current contract для container docs — `VISURE_BUCKET`.

### AE / SISTER / 2Captcha runtime

- `AE_LOGIN_URL`
- `AE_URL_SERVIZI`
- `SISTER_VISURE_CATASTALI_URL`
- `SISTER_LOGOUT_URL`
- `AE_USERNAME`
- `AE_PASSWORD`
- `AE_PIN`
- `TWO_CAPTCHA_API_KEY`

### Workspace / logging

- `UPPI_WORKSPACE_ROOT`
- `UPPI_LOG_DIR`
- `UPPI_LOG_FILE`
- `UPPI_LOG_LEVEL`

### Current runtime override envs that still matter

- `UPPI_IMMOBILI_YAML`
- `UPPI_CLIENTS_CSV`
- `UPPI_CLIENTS_YAML`
- `TEMPLATE_VERSION`
- `PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS`
- `DELETE_LOCAL_VISURA_AFTER_UPLOAD`

## Local build

```bash
docker build -f Dockerfile.backend -t uppi-backend:local .
```

## Local run for web-shell smoke

Цей запуск не потребує live DB/S3/AE/SISTER, бо health endpoints не перевіряють
external dependencies:

```bash
docker run --rm -p 8000:8000 \
  -e UPPI_WEB_ENV=local \
  -e UPPI_WEB_SESSION_SECRET=local-dev-session-secret \
  -e UPPI_WEB_AUTH_USERNAME=operator \
  -e UPPI_WEB_AUTH_PASSWORD=operator-password \
  -e UPPI_WEB_AUTH_PIN=123456 \
  uppi-backend:local
```

Після старту перевіряти:

```bash
curl http://127.0.0.1:8000/health/live
curl http://127.0.0.1:8000/health/ready
```

## Optional smoke script

Є helper:

```bash
bash scripts/docker/smoke_backend_container.sh
```

Він:

1. збирає image
2. піднімає container з local placeholder auth/session env
3. перевіряє `/health/live`
4. перевіряє `/health/ready`
5. перевіряє, що container logs не містять smoke secret placeholders

## Secrets / config policy

- secrets не входять у image
- `.env`, `state.json`, `downloads/`, `captcha_images/`, `logs/` і web-run
  artifact dirs не копіюються в build context
- `.env.backend.example` містить тільки placeholders
- для MVP AWS deployment secrets/config мають приходити через
  Systems Manager Parameter Store у Stage 11 / deploy setup, а не з
  committed files

## Non-goals цього slice

У Stage 10A свідомо не робиться:

- Terraform / CDK / CloudFormation
- ECS task definition
- ECR push automation
- ALB / RDS / S3 / CloudFront / Route53 / ACM resources
- SSM client
- Secrets Manager client
- frontend static hosting
- live AE/SISTER smoke

## Related docs

- [./aws_readiness_runtime_boundaries.md](./aws_readiness_runtime_boundaries.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- [./web_backend_shell.md](./web_backend_shell.md)
