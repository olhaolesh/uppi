# Parameter Store Contract

Цей документ фіксує Stage 10B Parameter Store path convention для AWS test
environment. Він не створює parameters і не додає SSM client у runtime.

Stage 11 має створити ці parameters вручну або окремим provisioning step.

## Rules

- root path for test environment: `/uppi/test`
- secrets go to `SecureString`
- non-sensitive coordination values may use `String`
- values never commit-яться в repo
- current app runtime все ще env-driven; ECS task definition template мапить
  values або secrets у container env

## SecureString parameters

| Path | Env var | Type | Notes |
| --- | --- | --- | --- |
| `/uppi/test/web/auth/username` | `UPPI_WEB_AUTH_USERNAME` | `SecureString` | Web login username |
| `/uppi/test/web/auth/password` | `UPPI_WEB_AUTH_PASSWORD` | `SecureString` | Web login password |
| `/uppi/test/web/auth/pin` | `UPPI_WEB_AUTH_PIN` | `SecureString` | Web login pin |
| `/uppi/test/web/session/secret` | `UPPI_WEB_SESSION_SECRET` | `SecureString` | Cookie signing secret |
| `/uppi/test/db/host` | `DB_HOST` | `SecureString` | Conservatively hidden host endpoint |
| `/uppi/test/db/user` | `DB_USER` | `SecureString` | DB username |
| `/uppi/test/db/password` | `DB_PASSWORD` | `SecureString` | DB password |
| `/uppi/test/s3/access_key` | `S3_ACCESS_KEY` | `SecureString` | S3-compatible access key |
| `/uppi/test/s3/secret_key` | `S3_SECRET_KEY` | `SecureString` | S3-compatible secret key |
| `/uppi/test/ae/username` | `AE_USERNAME` | `SecureString` | AE/SISTER username |
| `/uppi/test/ae/password` | `AE_PASSWORD` | `SecureString` | AE/SISTER password |
| `/uppi/test/ae/pin` | `AE_PIN` | `SecureString` | AE/SISTER pin |
| `/uppi/test/2captcha/api_key` | `TWO_CAPTCHA_API_KEY` | `SecureString` | 2Captcha API key |

## String parameters

Ці values можуть лишатися plain env у ECS task definition, але для Stage 11
зручно також зафіксувати їх у Parameter Store як deployment bookkeeping:

| Path | Related env var | Type | Notes |
| --- | --- | --- | --- |
| `/uppi/test/web/app/name` | `UPPI_WEB_APP_NAME` | `String` | Default app label |
| `/uppi/test/web/app/version` | `UPPI_WEB_APP_VERSION` | `String` | Deployment-visible version |
| `/uppi/test/web/env` | `UPPI_WEB_ENV` | `String` | Typically `test` |
| `/uppi/test/web/debug` | `UPPI_WEB_DEBUG` | `String` | `False` for test env |
| `/uppi/test/web/session/cookie_name` | `UPPI_WEB_SESSION_COOKIE_NAME` | `String` | Cookie name |
| `/uppi/test/web/session/cookie_secure` | `UPPI_WEB_SESSION_COOKIE_SECURE` | `String` | `True` behind HTTPS |
| `/uppi/test/web/session/max_age_seconds` | `UPPI_WEB_SESSION_MAX_AGE_SECONDS` | `String` | Session TTL |
| `/uppi/test/db/port` | `DB_PORT` | `String` | Usually `5432` |
| `/uppi/test/db/name` | `DB_NAME` | `String` | DB name |
| `/uppi/test/db/ssl_mode` | `DB_SSL_MODE` | `String` | Example: `require` |
| `/uppi/test/aws/region` | deployment only | `String` | Manual provisioning reference |
| `/uppi/test/s3/endpoint` | `S3_ENDPOINT` | `String` | S3-compatible endpoint |
| `/uppi/test/s3/secure` | `S3_SECURE` | `String` | `True` or `False` |
| `/uppi/test/s3/visure_bucket` | `VISURE_BUCKET` | `String` | Remote visure bucket |
| `/uppi/test/s3/attestazioni_bucket` | `ATTESTAZIONI_BUCKET` | `String` | Remote attestazioni bucket |
| `/uppi/test/ae/login_url` | `AE_LOGIN_URL` | `String` | Non-secret runtime URL |
| `/uppi/test/ae/servizi_url` | `AE_URL_SERVIZI` | `String` | Non-secret runtime URL |
| `/uppi/test/ae/visure_catastali_url` | `SISTER_VISURE_CATASTALI_URL` | `String` | Non-secret runtime URL |
| `/uppi/test/ae/logout_url` | `SISTER_LOGOUT_URL` | `String` | Non-secret runtime URL |
| `/uppi/test/runtime/template_version` | `TEMPLATE_VERSION` | `String` | Current document template version |

## Mapping to ECS task definition

Current Stage 10B template uses:

- `environment[]` for non-sensitive values
- `secrets[]` for `SecureString` values

Canonical template:

- [./ecs_task_definition.backend.template.json](./ecs_task_definition.backend.template.json)

## Security notes

- do not commit real values
- do not expose `SecureString` values in rendered files that will be committed
- do not reuse `state.json` as a secret transport or shared artifact
- actual parameter creation belongs to Stage 11 provisioning
