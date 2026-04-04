# UPPI

UPPI автоматизує отримання кадастрових виписок (`visura`) через AE/SISTER,
обробляє їх у non-browser pipeline, записує дані в PostgreSQL, завантажує
артефакти в object storage і генерує DOCX-документи `Attestazione`.

Цей `README.md` є головним entry point документації. Якщо треба зрозуміти
проєкт з нуля, починайте звідси, а не зі sprint/refactor-plan файлів.

## Що робить проєкт

Проєкт має дві великі частини:

- browser-critical flow:
  логін у AE, direct SISTER transition, CAPTCHA, завантаження PDF, explicit logout
- non-browser pipeline:
  парсинг PDF, синхронізація даних у БД, розрахунок canone, генерація DOCX,
  upload у storage, audit і failure reporting

Важливо:

- browser-critical semantics захищені окремими invariants
- `state.json` має protected lifecycle contract
- structural refactor уже зроблено, але browser flow навмисно не redesign-ився

## Як виглядає runtime flow

Коротко один run проходить так:

1. Spider стартує fresh session, чистить старий `state.json` і `captcha_images/`.
2. Завантажуються клієнти з `clients.yml`.
3. Для кожного клієнта вирішується, чи треба реально йти в SISTER, чи можна
   використати current DB/storage state.
4. Якщо потрібен browser path:
   - логін у AE
   - відкриття SISTER
   - збереження `state.json`
   - перехід до `Visure catastali`
   - CAPTCHA path, якщо з’являється
   - download PDF
   - explicit logout
5. Далі item потрапляє в non-browser pipeline:
   - `PersonSync`
   - `VisuraIngest`
   - `ImmobileSync`
   - `ContractSync`
   - `CanoneStage`
   - `DocumentStage`
   - `AuditStage`
6. Наприкінці outer orchestrator робить `commit()` і optional cleanup локального PDF.

Повний розбір:
- [docs/runtime_flow.md](docs/runtime_flow.md)

## Архітектура коротко

Ключові шари:

- `uppi/spiders/`, `uppi/ae/`:
  browser-critical orchestration і Playwright flow
- `uppi/services/visura_processor.py`, `uppi/services/visura_stages.py`:
  thin orchestrator і stage services
- `uppi/services/repositories/`:
  thin repository layer
- `uppi/services/policies/`:
  patch/business rules як pure functions
- `uppi/services/validation/`:
  warning-first validation layer
- `uppi/domain/`:
  models, DB/storage/config seams, calculation strategy, typed exceptions
- `uppi/services/storage_minio.py`, `uppi/domain/object_storage.py`:
  object storage boundary
- `uppi/services/attestazione_generator.py`,
  `uppi/services/attestazione_template_filler.py`:
  document generation path

Детально:
- [docs/current_architecture.md](docs/current_architecture.md)

## З чого почати junior-розробнику

Recommended reading order:

1. [README.md](README.md)
2. [Поточна архітектура](docs/current_architecture.md)
3. [Основний runtime flow](docs/runtime_flow.md)
4. [Protected invariants](docs/refactor_protected_invariants.md)
5. [Локальний запуск і тести](docs/local_development_and_testing.md)
6. [Document generation](docs/document_generation.md)
7. Reference docs за потреби:
   - [Failure registry](docs/failure_registry_contract.md)
   - [Transaction / resource safety](docs/transaction_resource_safety_review.md)
   - [Workspace / local artifacts](docs/workspace_local_artifacts_policy.md)
   - [state.json lifecycle](docs/state_json_lifecycle_contract.md)
   - [AWS readiness](docs/aws_readiness_runtime_boundaries.md)

## Карта документації

### Current operational docs

- [docs/current_architecture.md](docs/current_architecture.md)
  Загальна карта шарів, boundaries і модулів.
- [docs/runtime_flow.md](docs/runtime_flow.md)
  Послідовність виконання одного run і одного client/item.
- [docs/local_development_and_testing.md](docs/local_development_and_testing.md)
  Встановлення, `.env`, локальний запуск, тести, verification gates.
- [docs/document_generation.md](docs/document_generation.md)
  Як працює DOCX generation і де лежить canonical template-filler.
- [docs/refactor_protected_invariants.md](docs/refactor_protected_invariants.md)
  Що не можна міняти без high-risk review.
- [docs/live_smoke_strategy_ae_sister.md](docs/live_smoke_strategy_ae_sister.md)
  Canonical manual smoke checklist.

### Reference architecture docs

- [docs/failure_registry_contract.md](docs/failure_registry_contract.md)
- [docs/transaction_resource_safety_review.md](docs/transaction_resource_safety_review.md)
- [docs/workspace_local_artifacts_policy.md](docs/workspace_local_artifacts_policy.md)
- [docs/state_json_lifecycle_contract.md](docs/state_json_lifecycle_contract.md)
- [docs/aws_readiness_runtime_boundaries.md](docs/aws_readiness_runtime_boundaries.md)
- [docs/logging_foundation.md](docs/logging_foundation.md)
- [docs/compatibility_shim_migration_uppi_docs.md](docs/compatibility_shim_migration_uppi_docs.md)

### Domain/source materials

Це не operational docs, а reference materials:

- [uppi/docs/accordo_pescara.md](uppi/docs/accordo_pescara.md)
- [uppi/docs/accordo_pescara.pdf](uppi/docs/accordo_pescara.pdf)
- [uppi/docs/accordo_pescara_ocr.pdf](uppi/docs/accordo_pescara_ocr.pdf)
- [uppi/docs/pescara2018_summary.md](uppi/docs/pescara2018_summary.md)

### Historical / archival docs

Ці файли корисні для історії рефакторингу, але не є головними current guides:

- `docs/refactor_execution_plan_*`
- `docs/refactor_execution_plan_overview.md`
- `docs/refactor_risk_register.md`
- `docs/sprint_2_architecture_state.md`
- `docs/sprint_2_merge_readiness_checklist.md`
- `docs/sprint_2_closeout_note.md`

## Локальний запуск

Швидкий старт:

1. Створити `.env`
2. Ініціалізувати БД:
   `python uppi/utils/db_utils/init_db.py`
3. Запустити spider:
   `scrapy crawl uppi`

Повна інструкція:
- [docs/local_development_and_testing.md](docs/local_development_and_testing.md)

## Тестування і verification

Базова команда:

```bash
venv/bin/python -m pytest -q
```

Ключові suites:

- parser baseline
- DOCX baseline
- golden-path pipeline integration
- repo integration tests on temp Postgres
- stage service tests
- validation / failure reporting / retry policy tests

Коли потрібен live smoke:

- будь-які зміни навколо browser-critical flow
- будь-які зміни навколо `state.json`
- будь-які зміни навколо end-to-end artifact paths/cleanup

Checklist:
- [docs/live_smoke_strategy_ae_sister.md](docs/live_smoke_strategy_ae_sister.md)

## Troubleshooting: куди дивитися

- Проблема з логіном, direct SISTER, logout, CAPTCHA, `state.json`:
  [docs/refactor_protected_invariants.md](docs/refactor_protected_invariants.md),
  [docs/state_json_lifecycle_contract.md](docs/state_json_lifecycle_contract.md)
- Проблема з runtime flow:
  [docs/runtime_flow.md](docs/runtime_flow.md)
- Проблема з DB / storage / partial failures:
  [docs/transaction_resource_safety_review.md](docs/transaction_resource_safety_review.md)
- Проблема з DOCX generation:
  [docs/document_generation.md](docs/document_generation.md)
- Проблема з локальними артефактами:
  [docs/workspace_local_artifacts_policy.md](docs/workspace_local_artifacts_policy.md)
- Проблема з failure reporting / retry classification:
  [docs/failure_registry_contract.md](docs/failure_registry_contract.md)

## Важливі заборони

Без окремого high-risk review не можна:

- міняти AE/SISTER flow
- міняти `state.json` lifecycle semantics
- міняти selector order / wait / click / logout semantics
- робити blind browser retry
- робити transaction-boundary redesign під виглядом локального cleanup

Canonical source:
- [docs/refactor_protected_invariants.md](docs/refactor_protected_invariants.md)
