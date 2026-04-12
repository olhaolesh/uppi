# Основний runtime flow

Цей документ пояснює, що саме відбувається під час одного run, у якій
послідовності це виконується і які модулі беруть участь на кожному етапі.

Legacy note для rollout:

- цей файл описує поточну implemented mixed-flow модель;
- він не є source of truth для нового rollout contract;
- для single-client `immobili.yml`, трьох цільових режимів і правила
  `prepare owns fetch/update logic` див.
  [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md).

## 1. Що є input у поточній реалізації

Основний input у поточній реалізації — `clients.yml`.

Code path:

- [../uppi/domain/clients.py](../uppi/domain/clients.py)
- [../uppi/config/app_config.py](../uppi/config/app_config.py)

Що тут відбувається:

- YAML читається
- current mapping і defaults нормалізуються
- validation layer перевіряє базовий structural contract

## 2. Spider start: fresh session і cache decision

Code path:

- [../uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)

На старті spider:

1. чистить старий `state.json`
2. чистить `captcha_images/`
3. читає clients input
4. для кожного клієнта вирішує:
   - чи можна пропустити browser path і використати current DB/storage state
   - чи треба реально йти в SISTER

Тут важливо:

- `state.json` не reusable cache
- cleanup старого state — частина protected lifecycle

## 3. Browser phase: AE -> SISTER -> download

Code paths:

- [../uppi/ae/auth.py](../uppi/ae/auth.py)
- [../uppi/ae/sister_navigation.py](../uppi/ae/sister_navigation.py)
- [../uppi/ae/captcha.py](../uppi/ae/captcha.py)
- [../uppi/ae/download.py](../uppi/ae/download.py)

Послідовність:

1. login у AE
2. open SISTER in new tab
3. save `state.json`
4. navigate to `Visure catastali`
5. solve CAPTCHA if present
6. trigger PDF download
7. explicit logout

Що тут не можна змінювати casually:

- selector order
- wait/click/fill sequence
- direct SISTER contract
- logout semantics
- `state.json` lifecycle

Reference:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

## 4. Item entering the non-browser pipeline

Після browser phase spider yield-ить `UppiItem`.

Далі item іде в pipeline:

- [../uppi/pipelines.py](../uppi/pipelines.py)
- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)

Pipeline вже не керує браузером. Тут починається non-browser data/document flow.

## 5. Outer orchestrator

Current orchestrator:

- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)

Що він робить:

- створює DB connection
- координує stage order
- володіє outer `commit()`, `rollback()` і `close()`
- не виконує сам весь business logic вручну, а делегує stage services

## 6. Stage services у поточному порядку

### `PersonSyncService`

Що робить:

- синхронізує locatore/conduttore
- синхронізує їхні адреси

### `VisuraIngestService`

Що робить:

- шукає локальний PDF
- рахує checksum
- upload-ить PDF у storage
- реєструє visura в БД

### `ImmobileSyncService`

Що робить:

- запускає parser
- валідує parser output
- записує immobili і пов'язані visura-address дані

### `ContractSyncService`

Що робить:

- синхронізує real address
- синхронізує immobile elements
- upsert-ить contract
- будує `contract_ctx`

### `CanoneStageService`

Що робить:

- будує `CanoneInput`
- запускає current calculation strategy
- записує snapshot розрахунку в БД

### `DocumentStageService`

Що робить:

- будує template params
- генерує DOCX
- upload-ить DOCX у storage
- викликає audit

### `AuditStageService`

Що робить:

- пише success/failed audit rows

## 7. Де що відбувається

### Читання input

- [../uppi/domain/clients.py](../uppi/domain/clients.py)

### Парсинг PDF

- [../uppi/parsers/visura_pdf_parser.py](../uppi/parsers/visura_pdf_parser.py)

### Робота з БД

- [../uppi/services/repositories/](../uppi/services/repositories/)
- compatibility facade:
  [../uppi/services/db_repo.py](../uppi/services/db_repo.py)

### Генерація документів

- [../uppi/services/attestazione_generator.py](../uppi/services/attestazione_generator.py)
- [../uppi/services/attestazione_template_filler.py](../uppi/services/attestazione_template_filler.py)

### Upload / storage

- [../uppi/domain/object_storage.py](../uppi/domain/object_storage.py)
- [../uppi/services/storage_minio.py](../uppi/services/storage_minio.py)

### Audit / failure reporting

- [../uppi/services/repositories/audit_repo.py](../uppi/services/repositories/audit_repo.py)
- [../uppi/services/failure_registry.py](../uppi/services/failure_registry.py)

## 8. Failure handling в current code

Що вже є:

- typed domain exceptions
- validation layer
- failure registry
- stage-level failure reporting
- retry classification matrix

Що ще не зроблено:

- full retry engine
- transaction-boundary redesign
- compensating delete logic for artifacts

Reference:

- [./failure_registry_contract.md](./failure_registry_contract.md)
- [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)

## 9. Outputs і artifacts

Current local artifacts:

- `downloads/`
- `captcha_images/`
- local DOCX/PDF files
- protected `state.json`

Current remote artifacts:

- visura PDF object
- attestazione DOCX object

Reference:

- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)

## 10. Що не можна змінювати без high-risk review

- browser flow
- `state.json` lifecycle semantics
- selector / wait / logout semantics
- direct SISTER transition
- visura download flow

Canonical source:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
