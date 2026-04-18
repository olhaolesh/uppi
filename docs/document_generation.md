# Document Generation

Цей документ пояснює current DOCX-generation path і те, де саме лежить код,
який відповідає за `Attestazione`.

## 1. Що генерується

Проєкт генерує DOCX `Attestazione` після того, як:

- є contract context
- є canone snapshot
- є вибраний `Immobile`

Це робиться в `DocumentStageService`.

Code path:

- [../uppi/services/visura_stages.py](../uppi/services/visura_stages.py)

## 2. Які модулі беруть участь

### Placeholder params builder

- [../uppi/services/attestazione_generator.py](../uppi/services/attestazione_generator.py)

Відповідальність:

- взяти `adapter`, `immobile`, `contract_ctx`
- зібрати словник `{{PLACEHOLDER}} -> value`
- зберегти current precedence rules між YAML, DB context і overrides

### DOCX template filler

Canonical home:

- [../uppi/services/attestazione_template_filler.py](../uppi/services/attestazione_template_filler.py)

Compatibility shim:

- [../uppi/docs/attestazione_template_filler.py](../uppi/docs/attestazione_template_filler.py)

Відповідальність:

- скопіювати DOCX template
- пройти по paragraph/table runs
- замінити placeholders
- зберегти current underline semantics і current blank behavior

### Template file

- `attestazione_template/template_attestazione_pescara.docx`

## 3. Де формується output path

Code path:

- [../uppi/domain/storage.py](../uppi/domain/storage.py)

Current local naming:

- `downloads/{CF}/ATTESTAZIONE_{CF}_{contract_id}_{slug}.docx`

Current remote naming:

- `attestazioni/{CF}/{contract_id}.docx`

## 4. Current execution order

У `DocumentStageService` порядок зараз такий:

1. `build_template_params(...)`
2. `get_attestazione_path(...)`
3. `fill_attestazione_template(...)`
4. `storage_service.upload_file(...)`
5. `audit_stage.log_generated(...)`

На failure path:

1. failure record
2. logger exception
3. `audit_stage.log_failed(...)`
4. `return None`

Порядок тут already part of current contract.

## 5. Compatibility-shim migration note

Історично template filler лежав у `uppi/docs/`.
Тепер canonical implementation живе в `uppi/services/`, але старий import path
поки лишається working через thin shim.

Migration note:

- [./archive/compatibility_shim_migration_uppi_docs.md](./archive/compatibility_shim_migration_uppi_docs.md)

## 6. Baseline tests

Основні guardrails:

- [../tests/test_attestazione_generator_baseline.py](../tests/test_attestazione_generator_baseline.py)
- [../tests/test_pipeline_golden_path_integration.py](../tests/test_pipeline_golden_path_integration.py)
- [../tests/test_visura_stage_services.py](../tests/test_visura_stage_services.py)

## 7. Що не вважати safe change

Без окремого review не треба:

- міняти placeholder precedence
- міняти underline / blank-filler behavior
- міняти output naming contract
- змішувати migration slice з formatting fixes
- прибирати old import shim в тому самому PR
