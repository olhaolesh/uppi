# Sprint 2 Architecture State

> Historical / archival snapshot.
> Документ корисний як проміжна точка після Sprint 2, але не є головним
> current architecture guide. Для актуального опису системи читайте
> [README.md](../../README.md) і [Поточну архітектуру](../current_architecture.md).

Цей документ фіксує поточний технічний стан після основних Sprint 2 змін.
Він не замінює execution-plan, а описує фактичні boundaries, на які тепер
спирається кодова база.

Пов'язані документи:

- current architecture guide: [../current_architecture.md](../current_architecture.md)
- protected invariants: [../refactor_protected_invariants.md](../refactor_protected_invariants.md)
- canonical live smoke checklist: [../live_smoke_strategy_ae_sister.md](../live_smoke_strategy_ae_sister.md)
- historical merge-readiness checklist: [./sprint_2_merge_readiness_checklist.md](./sprint_2_merge_readiness_checklist.md)

## Що змінилося в Sprint 2

Sprint 2 змінив non-browser service/repository boundaries, але не змінював
browser-critical semantics.

Зроблено:

- Config / DI foundation
- `UPPI_CLIENTS_YAML` support і config normalization
- temp-Postgres repo integration tests
- repository split на thin submodules
- patch policy extraction у pure policy units
- validation layer
- typed domain exceptions
- extraction-first decomposition `VisuraProcessor` у thin orchestrator + stage services

## Поточна структура шарів

### Config / DI

Canonical config і creation seams:

- [uppi/config/app_config.py](../../uppi/config/app_config.py)
- [uppi/domain/db.py](../../uppi/domain/db.py)
- [uppi/domain/object_storage.py](../../uppi/domain/object_storage.py)
- [uppi/domain/clients.py](../../uppi/domain/clients.py)

Призначення:

- зробити env/default resolution явним;
- дати injectable seams для DB/storage/clients source;
- не змінювати current default fallback behavior.

### Repository Layer

Thin repository submodules живуть у:

- [uppi/services/repositories/address_repo.py](../../uppi/services/repositories/address_repo.py)
- [uppi/services/repositories/person_repo.py](../../uppi/services/repositories/person_repo.py)
- [uppi/services/repositories/visura_repo.py](../../uppi/services/repositories/visura_repo.py)
- [uppi/services/repositories/immobile_repo.py](../../uppi/services/repositories/immobile_repo.py)
- [uppi/services/repositories/contract_repo.py](../../uppi/services/repositories/contract_repo.py)
- [uppi/services/repositories/audit_repo.py](../../uppi/services/repositories/audit_repo.py)
- shared helpers: [uppi/services/repositories/common.py](../../uppi/services/repositories/common.py)

Compatibility facade лишається в:

- [uppi/services/db_repo.py](../../uppi/services/db_repo.py)

Принцип:

- repo layer відповідає за SQL/read-write contract;
- old import surface через `uppi.services.db_repo` лишається валідним;
- transaction ownership у Sprint 2 не змінювався.

### Policy Layer

Patch/update semantics винесені в:

- [uppi/services/policies/patch_policy.py](../../uppi/services/policies/patch_policy.py)
- [uppi/services/policies/contract_patch_policy.py](../../uppi/services/policies/contract_patch_policy.py)
- [uppi/services/policies/immobile_patch_policy.py](../../uppi/services/policies/immobile_patch_policy.py)

Принцип:

- policy layer містить pure-function business rules;
- repo layer викликає policy functions, але не втрачає current SQL semantics;
- known current quirks свідомо не “виправлялися” в межах Sprint 2.

### Validation Layer

Validation surface живе в:

- [uppi/services/validation/models.py](../../uppi/services/validation/models.py)
- [uppi/services/validation/yaml_validation.py](../../uppi/services/validation/yaml_validation.py)
- [uppi/services/validation/parser_validation.py](../../uppi/services/validation/parser_validation.py)
- [uppi/services/validation/canone_validation.py](../../uppi/services/validation/canone_validation.py)

Принцип:

- warning-first для questionable або historically tolerated cases;
- hard-fail лише для clearly invalid structural contracts;
- validation layer не є validation crackdown і не переписує tolerated current behavior.

### Typed Domain Exceptions

Exception hierarchy живе в:

- [uppi/domain/exceptions.py](../../uppi/domain/exceptions.py)

Базовий поділ:

- `RecoverableDomainError`
- `NonRecoverableDomainError`
- validation-specific typed exceptions для YAML input, parser output і canone input

Принцип:

- error surface став явнішим;
- exception layer не супроводжувався orchestration redesign;
- browser-adjacent exception flow не змінювався.

### Orchestration Layer

Current orchestrator:

- [uppi/services/visura_processor.py](../../uppi/services/visura_processor.py)

Extracted stage services:

- [uppi/services/visura_stages.py](../../uppi/services/visura_stages.py)

Поточна роль `VisuraProcessor`:

- лишається orchestrator;
- створює connection;
- координує stage order;
- володіє outer commit / rollback / close;
- не робить transaction redesign.

## Current Stage Order

Поточний порядок stage calls у `VisuraProcessor`:

1. `PersonSyncService`
2. `VisuraIngestService`
3. `ImmobileSyncService`
4. `db_load_immobili()` + YAML-based selection
5. `ContractSyncService`
6. `CanoneStageService`
7. `DocumentStageService`
8. `AuditStageService` як частина current document stage contract
9. outer `commit()`
10. optional local PDF cleanup після commit

Цей порядок у Sprint 2 не redesign-ився; decomposition була лише extraction-first.

## Що Sprint 2 свідомо НЕ робив

- не змінював browser-critical flow
- не змінював `state.json` semantics
- не змінював selector order / wait/click/fill sequence
- не змінював logout semantics
- не змінював direct SISTER contract
- не робив transaction-boundary redesign
- не вводив failure registry
- не вводив retry matrix
- не вводив calculation strategy abstraction
- не вводив workspace/path abstraction

## Відомі current behaviors, які Sprint 2 свідомо зберіг

- `contract_kind` reset semantics
- `durata_anni="-" -> 3`
- mismatch для `d12` code shape
- current literal-preservation semantics у patch/business rules
- current tolerant parser/validation behavior
- current ingest behavior, коли `visura_source == "sister"` і локальний PDF не знайдено

Ці кейси є частиною current contract і не “лагодилися” в Sprint 2.

## Що це означає для Sprint 3

Sprint 3 має стартувати вже з цих boundaries, а не повертатися до монолітного
`db_repo.py` або старого `VisuraProcessor`.

Тобто далі можна планувати:

- failure registry
- transaction/resource-safety review
- strategy boundaries
- workspace/local-artifact policy

Canonical review note для цієї зони:

- [docs/transaction_resource_safety_review.md](../transaction_resource_safety_review.md)
- [docs/aws_readiness_runtime_boundaries.md](../aws_readiness_runtime_boundaries.md)

Але без перегляду browser-critical invariants і без неявного redesign
`state.json` lifecycle.
