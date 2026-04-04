# Поточна архітектура

Це головний current architecture guide для проєкту.
Якщо вам треба зрозуміти, як код влаштований сьогодні, читайте цей документ,
а не historical sprint-plan файли.

## Для чого цей проєкт

UPPI автоматизує повний ланцюжок:

- читання вхідних даних про клієнтів
- отримання `visura` через AE/SISTER
- парсинг PDF
- запис нормалізованих даних у PostgreSQL
- розрахунок canone
- генерацію DOCX `Attestazione`
- upload артефактів у object storage
- audit і failure reporting

## Високорівневий поділ системи

### 1. Browser-critical зона

Код:

- [../uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)
- [../uppi/ae/auth.py](../uppi/ae/auth.py)
- [../uppi/ae/sister_navigation.py](../uppi/ae/sister_navigation.py)
- [../uppi/ae/captcha.py](../uppi/ae/captcha.py)
- [../uppi/ae/download.py](../uppi/ae/download.py)
- [../uppi/settings.py](../uppi/settings.py)

Відповідальність:

- fresh session start
- login у AE
- direct SISTER transition
- CAPTCHA path
- visura download
- explicit logout
- protected `state.json` lifecycle

Цю зону не можна змінювати без high-risk review.

Reference:
- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

### 2. Orchestration layer

Код:

- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../uppi/services/visura_stages.py)

Відповідальність:

- взяти item після browser/download phase
- відкрити outer DB connection
- пройти non-browser stage order
- виконати outer `commit` / `rollback` / `close`

`VisuraProcessor` сьогодні є thin orchestrator.
Основна робота винесена в stage services.

### 3. Repository layer

Код:

- [../uppi/services/repositories/address_repo.py](../uppi/services/repositories/address_repo.py)
- [../uppi/services/repositories/person_repo.py](../uppi/services/repositories/person_repo.py)
- [../uppi/services/repositories/visura_repo.py](../uppi/services/repositories/visura_repo.py)
- [../uppi/services/repositories/immobile_repo.py](../uppi/services/repositories/immobile_repo.py)
- [../uppi/services/repositories/contract_repo.py](../uppi/services/repositories/contract_repo.py)
- [../uppi/services/repositories/audit_repo.py](../uppi/services/repositories/audit_repo.py)
- compatibility facade: [../uppi/services/db_repo.py](../uppi/services/db_repo.py)

Відповідальність:

- SQL
- read/write shape
- joined context loading
- persistence contract

Repository layer не повинен містити розмазану business semantics.

### 4. Policy layer

Код:

- [../uppi/services/policies/patch_policy.py](../uppi/services/policies/patch_policy.py)
- [../uppi/services/policies/contract_patch_policy.py](../uppi/services/policies/contract_patch_policy.py)
- [../uppi/services/policies/immobile_patch_policy.py](../uppi/services/policies/immobile_patch_policy.py)

Відповідальність:

- smart patch logic
- delete-on-`"-"` semantics
- fallback/default business rules

Це pure-function layer без SQL.

### 5. Validation та exceptions

Код:

- [../uppi/services/validation/](../uppi/services/validation/)
- [../uppi/domain/exceptions.py](../uppi/domain/exceptions.py)

Відповідальність:

- warning-first validation
- hard-fail лише для clearly invalid structural contracts
- typed domain exceptions для non-browser service surface

### 6. Failure handling / retry classification

Код:

- [../uppi/domain/failure_registry.py](../uppi/domain/failure_registry.py)
- [../uppi/services/failure_registry.py](../uppi/services/failure_registry.py)
- [../uppi/services/retry_policy.py](../uppi/services/retry_policy.py)

Відповідальність:

- standardized failure records
- stage-level failure reporting
- retry classification matrix

Немає full retry engine. Є тільки registry + policy surface.

### 7. Config / DB / storage seams

Код:

- [../uppi/config/app_config.py](../uppi/config/app_config.py)
- [../uppi/config/workspace.py](../uppi/config/workspace.py)
- [../uppi/domain/db.py](../uppi/domain/db.py)
- [../uppi/domain/object_storage.py](../uppi/domain/object_storage.py)
- [../uppi/services/storage_minio.py](../uppi/services/storage_minio.py)

Відповідальність:

- env/default resolution
- DB connection factory
- object storage boundary
- workspace/local artifact policy

### 8. Document generation

Код:

- [../uppi/services/attestazione_generator.py](../uppi/services/attestazione_generator.py)
- [../uppi/services/attestazione_template_filler.py](../uppi/services/attestazione_template_filler.py)
- compatibility shim:
  [../uppi/docs/attestazione_template_filler.py](../uppi/docs/attestazione_template_filler.py)

Відповідальність:

- побудова placeholder params
- заповнення DOCX template
- upload generated DOCX
- audit log around generation result

## Поточний non-browser stage order

Для одного item порядок зараз такий:

1. `PersonSyncService`
2. `VisuraIngestService`
3. `ImmobileSyncService`
4. `db_load_immobili()` + YAML-driven selection
5. `ContractSyncService`
6. `CanoneStageService`
7. `DocumentStageService`
8. `AuditStageService`
9. outer `commit()`
10. optional local PDF cleanup after commit

Порядок важливий. Це не місце для casual “optimization”.

## Де шукати інформацію по задачах

- Треба зрозуміти runtime order:
  [./runtime_flow.md](./runtime_flow.md)
- Треба щось міняти у browser flow:
  [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- Треба зрозуміти `state.json`:
  [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- Треба зрозуміти local artifacts:
  [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- Треба зрозуміти transaction/partial failures:
  [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)
- Треба зрозуміти document generation:
  [./document_generation.md](./document_generation.md)
- Треба зрозуміти failure reporting і retry:
  [./failure_registry_contract.md](./failure_registry_contract.md)
- Треба зрозуміти AWS-readiness seams:
  [./aws_readiness_runtime_boundaries.md](./aws_readiness_runtime_boundaries.md)

## Що вважати historical

Sprint/refactor execution plans і merge-closeout notes — це historical/planning
artifacts. Вони корисні для історії змін, але не є головним описом поточної
архітектури.
