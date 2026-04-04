# AWS Readiness Runtime Boundaries

Цей документ є canonical AWS-readiness artifact для current codebase.
Він не запускає AWS migration і не змінює runtime semantics. Його мета —
зафіксувати concrete boundaries, які вже існують, і чітко показати, що ще лишається
локальним або deferred.

## Scope і non-goals

У scope цього artifact:

- config-provider readiness
- object storage boundary
- DB connection factory notes
- workspace/local-artifact implications для cloud runtime
- practical recommendation для ECS/Fargate-first deployment shape

Свідомо не робиться:

- actual AWS integration
- Secrets Manager / SSM clients
- ECS migration implementation
- transaction-boundary redesign
- workspace/path redesign
- browser flow changes

## Current Runtime Boundaries

### Config / provider boundary

Canonical code paths:

- [uppi/config/app_config.py](../uppi/config/app_config.py)
- [uppi/domain/db.py](../uppi/domain/db.py)
- [uppi/domain/object_storage.py](../uppi/domain/object_storage.py)
- [uppi/config/workspace.py](../uppi/config/workspace.py)

Current state:

- runtime today is env-first via `decouple.config(...)`
- callers вже спираються на dataclass/factory seams:
  - `DatabaseConfig`
  - `ClientsSourceConfig`
  - `VisuraProcessorRuntimeConfig`
  - `ObjectStorageConfig`
  - `WorkspaceConfig`

Що це означає для AWS readiness:

- actual source of secrets/config ще можна змінити later без переписування
  orchestration code
- future provider layer має гідрувати existing dataclasses, а не обходити їх
  прямими SDK-викликами в business code

### Object storage boundary

Canonical code paths:

- [uppi/domain/object_storage.py](../uppi/domain/object_storage.py)
- [uppi/services/storage_minio.py](../uppi/services/storage_minio.py)
- [uppi/services/visura_stages.py](../uppi/services/visura_stages.py)

Current state:

- `ObjectStorage` already encapsulates backend client creation
- `StorageService` adds retry around storage operations
- visura/docx stages already talk to storage via this boundary
- persisted remote artifact identity is bucket + object key

Що вже good seam для AWS:

- caller-и не залежать від concrete MinIO client construction
- storage object naming already централізоване
- upload/existence checks уже ізольовані від orchestration

Що ще лишається локальним:

- local PDF/DOCX files існують до upload
- CAPTCHA screenshots теж local-only
- current flow не робить streaming upload directly from browser/download stream
- compensating delete logic for uploaded artifacts відсутня

### DB connection boundary

Canonical code paths:

- [uppi/domain/db.py](../uppi/domain/db.py)
- [uppi/config/app_config.py](../uppi/config/app_config.py)

Current state:

- connection creation already централізоване в `get_pg_connection(...)`
- kwargs builder already відділений у `build_pg_connection_kwargs(...)`
- connection ownership уже зафіксований outer orchestrator-ом і окремо
  задокументований у [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)

Що вже good seam для ECS/Fargate:

- env-backed secrets легко inject-ити в task definition today
- future SSM/Secrets Manager provider може заповнювати той самий
  `DatabaseConfig`
- connection factory seam already exists for tests/runtime packaging

Що deferred:

- pooling
- IAM auth
- DSN/provider refresh
- transaction redesign

## Config Provider Readiness For SSM / Secrets Manager

Поточна рекомендація:

- лишити application code provider-agnostic
- future provider layer має резолвити secrets/config before building:
  - `AppConfig`
  - `DatabaseConfig`
  - `ObjectStorageConfig`

Рекомендований shape для future implementation:

1. bootstrap provider читає env/SSM/Secrets Manager
2. provider materializes existing dataclasses
3. runtime code continues using current factories/services

Що не треба робити later:

- читати SSM/Secrets Manager прямо з repo/service/stage code
- змішувати secret retrieval з browser/stage orchestration
- обходити current config dataclasses side-channel логікою

## Object Storage Boundary Notes

Для AWS-ready runtime важливо розуміти current contract:

- object storage is the remote persistence boundary for visura/docx artifacts
- local filesystem is still part of current processing path before upload
- runtime must therefore support writable ephemeral storage, even if S3 is the
  final remote target

Практичні implications:

- S3 bucket/object naming contract already exists
- local artifact cleanup is still governed by current workspace policy
- stateful local artifacts must stay task-local and must not be shared between runs

Пов'язані документи:

- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

## Runtime Recommendations: ECS / Fargate First

Current recommended first target:

- ECS/Fargate task runtime, not Lambda

Причина:

- browser automation + Playwright + local downloads/screenshots + DOCX/PDF
  generation already assume writable local filesystem and a fuller process model
- current flow also benefits from long-lived process scope per run

Recommended runtime shape:

- one worker/task per pipeline run or conservative low-concurrency worker
- writable ephemeral filesystem for:
  - `downloads/`
  - `captcha_images/`
  - generated DOCX/PDF artifacts
  - local logs/failure registry if still enabled locally
- env-injected config today; provider-backed config later
- reachable PostgreSQL endpoint
- S3-compatible object storage reachable from task
- container image with Playwright-compatible browser/runtime dependencies

Operational cautions:

- current code still has browser-critical invariants and `state.json` semantics
- do not share `state.json` across tasks or between fresh sessions
- local artifact lifecycle is not yet redesigned for fully stateless runtime
- current document/upload flow still has known partial-failure windows

## What Is Ready vs Deferred

### Already ready enough for boundary work

- provider-neutral config dataclasses
- DB connection factory seam
- object storage boundary
- workspace policy with unchanged default paths
- failure registry / retry classification / transaction review docs

### Still local or deferred

- actual SSM / Secrets Manager integration
- ECS task definition / image packaging
- remote-first artifact pipeline
- transaction/resource redesign between DB and S3
- browser runtime packaging hardening
- compatibility-shim migration for production code in `uppi/docs/`

## Concrete Next-Step Recommendations

Після цього PR, якщо AWS-readiness work продовжується:

1. add provider-backed bootstrap layer that hydrates existing config dataclasses
2. prepare ECS/Fargate packaging around current Playwright/browser assumptions
3. keep local artifacts task-local and ephemeral
4. only after that discuss remote-first artifact or transaction redesign

## Related Documents

- [./current_architecture.md](./current_architecture.md)
- [./runtime_flow.md](./runtime_flow.md)
- [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
