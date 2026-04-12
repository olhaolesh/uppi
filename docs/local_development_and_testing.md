# Локальний запуск і тестування

Цей документ пояснює, як підняти проєкт локально, які команди запускати і як
перевіряти зміни без випадкового порушення protected flow.

Legacy note для rollout:

- цей файл описує поточну implemented command/config surface;
- він не є source of truth для нового contract split між `prepare`, bulk import
  і generation;
- для rollout-target режимів і нового `immobili.yml` див.
  [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md).

## 1. Що потрібно локально

- Python 3.11
- virtualenv `venv/`
- PostgreSQL
- S3-compatible storage (локальний MinIO достатній)
- Playwright browser dependencies
- `.env` з AE/SISTER/DB/S3 налаштуваннями

## 2. Мінімальна підготовка середовища

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install
```

## 3. База даних

Ініціалізація схеми:

```bash
python uppi/utils/db_utils/init_db.py
```

Current schema file:

- [../uppi/utils/db_utils/uppi_schema.sql](../uppi/utils/db_utils/uppi_schema.sql)

## 4. Конфіг і `.env`

Основні групи налаштувань:

- AE/SISTER credentials і URLs
- 2Captcha key
- PostgreSQL
- S3-compatible storage
- `UPPI_CLIENTS_YAML` у поточній legacy реалізації
- runtime flags для pipeline

Current config surface:

- [../uppi/config/app_config.py](../uppi/config/app_config.py)
- [../uppi/config/workspace.py](../uppi/config/workspace.py)

## 5. Локальний запуск поточної реалізації

Основний запуск:

```bash
scrapy crawl uppi
```

Що робити перед run:

- перевірити `.env`
- переконатися, що DB і storage доступні
- переконатися, що `clients.yml` вказує на потрібний input у поточній
  реалізації

## 6. Основна тестова команда

```bash
venv/bin/python -m pytest -q
```

## 7. Найважливіші test suites

### Базові regression suites

- [../tests/test_pipeline_golden_path_integration.py](../tests/test_pipeline_golden_path_integration.py)
- [../tests/test_visura_pdf_parser_baseline.py](../tests/test_visura_pdf_parser_baseline.py)
- [../tests/test_attestazione_generator_baseline.py](../tests/test_attestazione_generator_baseline.py)

### Repo / SQL contracts

- [../tests/test_db_repo_patch_characterization.py](../tests/test_db_repo_patch_characterization.py)
- [../tests/test_db_repo_postgres_integration.py](../tests/test_db_repo_postgres_integration.py)

### Service boundaries

- [../tests/test_visura_stage_services.py](../tests/test_visura_stage_services.py)
- [../tests/test_validation_layer.py](../tests/test_validation_layer.py)
- [../tests/test_domain_exceptions.py](../tests/test_domain_exceptions.py)
- [../tests/test_failure_reporting_integration.py](../tests/test_failure_reporting_integration.py)
- [../tests/test_retry_policy.py](../tests/test_retry_policy.py)

### Workspace / state / safety contracts

- [../tests/test_workspace_policy.py](../tests/test_workspace_policy.py)
- [../tests/test_state_json_lifecycle_contract.py](../tests/test_state_json_lifecycle_contract.py)

## 8. Коли потрібен manual live smoke

Live smoke потрібен, якщо PR торкається:

- browser-critical flow
- `state.json`
- cleanup/path handling around logout/state artifacts
- end-to-end artifact path resolution

Canonical checklist:

- [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

## 9. Troubleshooting map

### Не працює AE/SISTER/login/logout

Читати:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

### Ламається parser / DB / stage pipeline

Читати:

- [./runtime_flow.md](./runtime_flow.md)
- [./current_architecture.md](./current_architecture.md)

### Проблема з partial failures або artifact consistency

Читати:

- [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)

### Проблема з DOCX generation

Читати:

- [./document_generation.md](./document_generation.md)

### Проблема з failure reporting / retry flags

Читати:

- [./failure_registry_contract.md](./failure_registry_contract.md)

## 10. Що не треба робити “по дорозі”

- не міняти browser flow під час звичайного docs/test cleanup
- не оптимізувати `state.json` lifecycle
- не змішувати behavior fix з великим documentation PR
- не трактувати historical sprint docs як current runbook
