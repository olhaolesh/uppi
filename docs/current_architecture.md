# Current Architecture

Це головний current architecture guide для кодової бази після rollout split.

Behavioral contract:
[./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)

Runtime sequencing:
[./runtime_flow.md](./runtime_flow.md)

## 1. High-Level Split

### 1. Generation-only production path

Code:

- [../uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)
- [../uppi/domain/immobili_document.py](../uppi/domain/immobili_document.py)
- [../uppi/utils/immobili_item_mapper.py](../uppi/utils/immobili_item_mapper.py)
- [../uppi/pipelines.py](../uppi/pipelines.py)
- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../uppi/services/visura_stages.py)

Responsibility:

- load prepared single-client `immobili.yml`
- validate it
- process only active immobili
- perform strict DB match
- run calculation / document / audit stages

It does not:

- login to AE/SISTER
- decide whether refresh is needed
- call the browser/import path

### 2. Import-only browser path

Code:

- [../uppi/spiders/uppi_browser_spider.py](../uppi/spiders/uppi_browser_spider.py)
- [../uppi/spiders/uppi_import_spider.py](../uppi/spiders/uppi_import_spider.py)
- [../uppi/services/import_only_runner.py](../uppi/services/import_only_runner.py)
- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)

Responsibility:

- protected AE/SISTER login and navigation
- CAPTCHA handling
- visura download
- import-only non-browser boundary up to `ImmobileSync`

It is reused by:

- `prepare-by-CF`
- bulk CSV import-only mode

### 3. Prepare orchestration

Code:

- [../uppi/cli/prepare_by_cf.py](../uppi/cli/prepare_by_cf.py)
- [../uppi/services/prepare_by_cf.py](../uppi/services/prepare_by_cf.py)
- [../uppi/services/immobili_yaml_generator.py](../uppi/services/immobili_yaml_generator.py)

Responsibility:

- own fetch/update decision logic
- decide DB hit vs import refresh
- generate one canonical `immobili.yml`

### 4. Bulk CSV orchestration

Code:

- [../uppi/cli/bulk_import_clients_csv.py](../uppi/cli/bulk_import_clients_csv.py)
- [../uppi/services/bulk_import_clients_csv.py](../uppi/services/bulk_import_clients_csv.py)
- [../uppi/domain/clients_csv.py](../uppi/domain/clients_csv.py)

Responsibility:

- load CSV
- normalize and dedupe CF
- reuse import-only runner
- report results

### 5. DB-driven YAML generator

Code:

- [../uppi/services/immobili_yaml_generator.py](../uppi/services/immobili_yaml_generator.py)
- [../uppi/services/repositories/prepare_document_repo.py](../uppi/services/repositories/prepare_document_repo.py)

Responsibility:

- read one client from DB
- build a deterministic single-client document
- serialize canonical `immobili.yml`

### 6. Validation and policy layer

Code:

- [../uppi/services/validation/](../uppi/services/validation/)
- [../uppi/services/policies/immobili_generation_policy.py](../uppi/services/policies/immobili_generation_policy.py)
- [../uppi/services/policies/contract_patch_policy.py](../uppi/services/policies/contract_patch_policy.py)
- [../uppi/services/policies/immobile_patch_policy.py](../uppi/services/policies/immobile_patch_policy.py)

Responsibility:

- canonical YAML validation
- field-class policy
- `"-"` semantics
- persistable-only write-back rules

### 7. Repository layer

Code:

- [../uppi/services/repositories/](../uppi/services/repositories/)
- compatibility facade: [../uppi/services/db_repo.py](../uppi/services/db_repo.py)

Responsibility:

- SQL
- joined read models
- persistence contracts

Repositories should not own orchestration decisions.

## 2. Stage Boundaries

### Import-only boundary

Stage order:

1. `PersonSyncService`
2. `VisuraIngestService`
3. `ImmobileSyncService`

Stop boundary:

- after `ImmobileSync`

### Generation-only boundary

Stage order:

1. strict DB identity match
2. `ContractSyncService`
3. `CanoneStageService`
4. `DocumentStageService`
5. `AuditStageService`

Stop boundary:

- after generation and outer commit

## 3. Canonical Inputs

Generation input:

- canonical single-client `immobili.yml`
- configured through `UPPI_IMMOBILI_YAML`

Bulk input:

- `clients.csv`
- configured through `UPPI_CLIENTS_CSV`

Legacy compatibility only:

- flat `clients.yml`
- configured through `UPPI_CLIENTS_YAML`
- used only for the internal protected import spider path

## 4. Protected Browser Invariants

Browser-critical contracts remain protected and separate from the generation
refactor.

Canonical references:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

## 5. Related Guides

- Runtime order: [./runtime_flow.md](./runtime_flow.md)
- Operator usage: [./operator_workflow.md](./operator_workflow.md)
- Validation policy: [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)
- Local setup and tests: [./local_development_and_testing.md](./local_development_and_testing.md)
