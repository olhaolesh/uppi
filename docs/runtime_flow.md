# Runtime Flow

Цей документ описує фактичний current runtime після rollout split на три
режими.

Canonical contract:
[./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)

Operator runbook:
[./operator_workflow.md](./operator_workflow.md)

## 1. Mode Map

UPPI now has three distinct runtime modes:

1. `prepare-by-CF`
2. `bulk-import-by-clients-csv`
3. `scrapy crawl uppi`

Internal browser/import reuse lives behind the import-only spider:

- [../uppi/spiders/uppi_import_spider.py](../uppi/spiders/uppi_import_spider.py)

## 2. Prepare-by-CF Flow

Code path:

- [../uppi/cli/prepare_by_cf.py](../uppi/cli/prepare_by_cf.py)
- [../uppi/services/prepare_by_cf.py](../uppi/services/prepare_by_cf.py)
- [../uppi/services/import_only_runner.py](../uppi/services/import_only_runner.py)
- [../uppi/services/immobili_yaml_generator.py](../uppi/services/immobili_yaml_generator.py)

Flow:

1. normalize and validate `--cf`
2. read explicit DB presence criterion
3. decide between:
   - DB hit + no force
   - DB miss
   - DB hit + force refresh
4. if import is needed, call the reusable import-only runner
5. after DB is ready, generate a single-client `immobili.yml`
6. stop after writing the YAML file

Important:

- prepare owns fetch/update logic
- prepare does not run generation stages
- prepare does not read `immobili.yml` as input

## 3. Bulk CSV Import-Only Flow

Code path:

- [../uppi/cli/bulk_import_clients_csv.py](../uppi/cli/bulk_import_clients_csv.py)
- [../uppi/services/bulk_import_clients_csv.py](../uppi/services/bulk_import_clients_csv.py)
- [../uppi/domain/clients_csv.py](../uppi/domain/clients_csv.py)
- [../uppi/services/import_only_runner.py](../uppi/services/import_only_runner.py)

Flow:

1. load `clients.csv`
2. normalize CF values
3. skip invalid rows
4. dedupe by normalized CF while preserving first occurrence order
5. call the import-only runner once per unique valid CF
6. collect per-row results and summary
7. stop without YAML generation

Important:

- bulk mode is import-only
- bulk mode never calls prepare
- bulk mode never calls generation stages

## 4. Generation-Only Flow

Code path:

- [../uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)
- [../uppi/domain/immobili_document.py](../uppi/domain/immobili_document.py)
- [../uppi/utils/immobili_item_mapper.py](../uppi/utils/immobili_item_mapper.py)
- [../uppi/pipelines.py](../uppi/pipelines.py)
- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../uppi/services/visura_stages.py)

Flow:

1. load canonical single-client `immobili.yml`
2. validate document shape and field-level policy
3. filter to active immobili where `enabled != false`
4. map each active record into a generation item
5. enter `UppiPipeline`
6. `VisuraProcessor.process_generation_item()` performs strict DB match
7. for each matched immobile:
   - `ContractSyncService`
   - `CanoneStageService`
   - `DocumentStageService`
   - `AuditStageService`
8. commit the transaction

Important:

- generation does not login to AE/SISTER
- generation does not call the import-only runner
- generation does not read `clients.yml`
- missing DB immobile hard-fails with prepare guidance

## 5. Internal Import-Only Browser Path

Code path:

- [../uppi/spiders/uppi_browser_spider.py](../uppi/spiders/uppi_browser_spider.py)
- [../uppi/spiders/uppi_import_spider.py](../uppi/spiders/uppi_import_spider.py)
- [../uppi/pipelines.py](../uppi/pipelines.py)
- [../uppi/services/visura_processor.py](../uppi/services/visura_processor.py)

Flow:

1. load legacy/transitional flat `clients.yml` input prepared by the runner
2. execute the protected browser-critical flow
3. enter `UppiImportPipeline`
4. `VisuraProcessor.process_import_item()` runs:
   - `PersonSyncService`
   - `VisuraIngestService`
   - `ImmobileSyncService`
5. stop after `ImmobileSync`

Important:

- this path is internal and reused by prepare/bulk
- it exists to preserve the protected browser flow
- it is not the canonical operator-facing generation entry point

## 6. Field Policy in the Runtime

Validation and clear semantics are centralized in:

- [../uppi/services/validation/yaml_validation.py](../uppi/services/validation/yaml_validation.py)
- [../uppi/services/policies/immobili_generation_policy.py](../uppi/services/policies/immobili_generation_policy.py)

Write-back policy lives in:

- [../uppi/services/policies/contract_patch_policy.py](../uppi/services/policies/contract_patch_policy.py)
- [../uppi/services/policies/immobile_patch_policy.py](../uppi/services/policies/immobile_patch_policy.py)

Short version:

- DB-clearable persistable fields write back clears
- run-only fields clear only the current generation run
- metadata, identity, visura/display fields reject `"-"`

Full matrix:
[./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)

## 7. Protected Invariants

The rollout did not change:

- AE/SISTER flow
- `state.json` lifecycle
- visura download flow
- selector order
- wait sequence
- logout semantics
- no-blind-retry rule for browser-critical stages

Canonical docs:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)
