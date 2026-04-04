# Failure Registry Contract

Цей документ фіксує current Sprint 3 contract для failure registry.
Мета цієї зони: мати єдину model/storage surface і stage-level reporting
без зміни current pipeline behavior і без blind browser retry.

## Scope цього кроку

На поточний момент введено:

- code-level failure registry model
- stage-aligned contract fields
- append-only storage contract
- проста локальна JSONL implementation slice
- stage-level reporting integration у processor/stage services
- explicit retry policy matrix для `retryable`
- явний no-blind-retry contract для browser-derived failures

На поточний момент НЕ введено:

- full retry engine / requeue scheduler
- transaction-boundary redesign
- failure-handling rewrite у `VisuraProcessor`
- browser-adjacent changes

## Canonical Model

Code-level model живе в:

- [../uppi/domain/failure_registry.py](../uppi/domain/failure_registry.py)

Основні поля `FailureRecord`:

- `run_id`
- `client_cf`
- `stage`
- `error_type`
- `retryable`
- `message_redacted`
- `artifact_refs`
- `recorded_at_utc`

## Stage Vocabulary

`stage` узгоджений із стабілізованими Sprint 2 boundaries:

- `PersonSync`
- `VisuraIngest`
- `ImmobileSync`
- `ContractSync`
- `CanoneStage`
- `DocumentStage`
- `AuditStage`
- `PipelineFatal`

Останній value потрібен як safe fallback для outer orchestrator failure surface.

## Redaction Rules

### `message_redacted`

- зберігає лише sanitized message surface
- не повинен містити raw secrets, tokens, session-bearing data, raw CAPTCHA content
- сумісний із already existing logging/redaction rules

### `artifact_refs`

- зберігаються як безпечні посилання на артефакти
- не повинні містити raw payload
- можуть посилатися на object key, stage marker, local artifact path або інший safe reference

### `client_cf`

- лишається contract identifier для зв'язку failure record з client-level processing
- не повинен випадково логуватися сирим поза redacted logging path

## Retry Policy Matrix

Retry policy code-level surface живе в:

- [../uppi/services/retry_policy.py](../uppi/services/retry_policy.py)

`retryable` тепер не є просто raw field або прямим віддзеркаленням `error.recoverable`.
Воно рахується через explicit matrix:

- stage boundary
- failure kind
- conservative default

### Failure Kinds

Matrix використовує такі класи:

- `browser_derived`
- `infra_transient`
- `storage_transient`
- `validation_contract`
- `data_contract`
- `local_artifact`
- `unknown`

### Stage-Level Policy

- `PersonSync`: retry тільки для `infra_transient`
- `VisuraIngest`: retry для `infra_transient`, `storage_transient`
- `ImmobileSync`: retry тільки для `infra_transient`
- `ContractSync`: retry тільки для `infra_transient`
- `CanoneStage`: retry тільки для `infra_transient`
- `DocumentStage`: retry для `infra_transient`, `storage_transient`
- `AuditStage`: retry тільки для `infra_transient`
- `PipelineFatal`: retry для `infra_transient`, `storage_transient`

### Explicit No-Retry Surface

Blind retry заборонений для всіх stages, якщо failure класифіковано як:

- `browser_derived`
- `validation_contract`
- `data_contract`
- `local_artifact`

Окремо це покриває browser-derived cases на кшталт:

- Playwright/selector/locator failures
- CAPTCHA/auth/logout/state-related failures
- `state.json` / storage-state misuse surface
- SISTER transition / browser navigation derived failures

### Recoverable vs Retryable

`recoverable` і `retryable` тепер не тотожні:

- `recoverable` означає, що current pipeline може локально пережити або пропустити failure
- `retryable` означає лише те, що matrix допускає безпечний повтор без blind browser replay

Тому warning-first або locally-recoverable validation failures можуть лишатися
`retryable = false`.

## Storage Contract

Storage surface живе в:

- [../uppi/services/failure_registry.py](../uppi/services/failure_registry.py)

Мінімальний contract:

- `append(record)`
- `list_records(run_id=None, client_cf=None)`

Принципи:

- append-only за духом
- без destructive update logic
- без складної persistence architecture
- достатньо простий для подальшого stage-level reporting

## Initial Implementation Slice

Поточна reference implementation:

- `JsonlFailureRegistryStorage`

Характеристики:

- локальний JSONL-файл
- safe default path у `logs/failure_registry.jsonl`
- читає й повертає `FailureRecord`
- підтримує простий read-side filter за `run_id` і `client_cf`

Це не “повна failure platform”, а лише базова contract-compatible реалізація.

## Confirmed Non-Goals

У цьому кроці свідомо не робиться:

- redesign transaction ownership
- mutation existing success flow
- browser-critical behavior changes
- stage-order changes
- AWS/workspace work
- strategy abstraction

## Current Integration Status

Stage-level reporting already covers:

- `PersonSync`
- `VisuraIngest`
- `ImmobileSync`
- `ContractSync`
- `CanoneStage`
- `DocumentStage`
- `AuditStage`
- outer fallback `PipelineFatal`

Failure reporting використовує retry matrix прямо в recorder layer,
без orchestration redesign і без зміни success path.

## Next Step After This PR

Наступним окремим PR можна робити transaction/resource-safety review:

- не змішувати це з retry engine redesign
- не змішувати це з browser-adjacent changes
