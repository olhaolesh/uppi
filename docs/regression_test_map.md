# Regression Test Map

Цей документ показує, які suites дають confidence для rollout-ready стану і що
саме вони покривають.

Canonical workflow docs:

- [./operator_workflow.md](./operator_workflow.md)
- [./rollout_ready_checklist.md](./rollout_ready_checklist.md)

## 1. Critical Rollout Areas

The rollout relies on test coverage in these areas:

- input contract loading and validation
- DB-driven `immobili.yml` generation
- import-only orchestration boundary
- `prepare-by-CF` decision tree
- bulk CSV import-only mode
- generation-only spider
- strict DB match
- YAML-over-DB merge and persistable-only write-back
- `"-"` semantics
- browser-sensitive regression safety contracts

## 2. Core Test Files by Area

### Input contract and validation

- [../tests/test_immobili_document_loading.py](../tests/test_immobili_document_loading.py)
- [../tests/test_validation_layer.py](../tests/test_validation_layer.py)
- [../tests/test_generation_spider.py](../tests/test_generation_spider.py)

Coverage:

- single-client `immobili.yml`
- active/enabled filtering
- field-level validation
- forbidden clear targets
- run-only normalization

### DB-driven generator and repository integration

- [../tests/test_db_repo_postgres_integration.py](../tests/test_db_repo_postgres_integration.py)
- [../tests/test_db_repo_patch_characterization.py](../tests/test_db_repo_patch_characterization.py)

Coverage:

- generator output shape
- deterministic ordering
- strict DB identity match
- persistable-only generation write-back
- real DB clear semantics

### Import-only orchestration boundary

- [../tests/test_visura_import_orchestration.py](../tests/test_visura_import_orchestration.py)
- [../tests/test_import_only_runner.py](../tests/test_import_only_runner.py)

Coverage:

- stop boundary after `ImmobileSync`
- generation stages not called on import-only path
- internal spider reuse
- protected browser/import path reuse seam

### Prepare mode

- [../tests/test_prepare_by_cf_service.py](../tests/test_prepare_by_cf_service.py)
- [../tests/test_prepare_by_cf_cli.py](../tests/test_prepare_by_cf_cli.py)

Coverage:

- Case A: DB hit + no force
- Case B: DB miss
- Case C: DB hit + force
- output path behavior
- import failure propagation

### Bulk CSV mode

- [../tests/test_clients_csv_loading.py](../tests/test_clients_csv_loading.py)
- [../tests/test_bulk_import_clients_csv_service.py](../tests/test_bulk_import_clients_csv_service.py)
- [../tests/test_bulk_import_clients_csv_cli.py](../tests/test_bulk_import_clients_csv_cli.py)

Coverage:

- CSV loading
- normalization and dedupe
- continue-on-error vs fail-fast
- import-only runner reuse
- reporting

### Generation stages and document output

- [../tests/test_visura_stage_services.py](../tests/test_visura_stage_services.py)
- [../tests/test_attestazione_generator_baseline.py](../tests/test_attestazione_generator_baseline.py)
- [../tests/test_pipeline_golden_path_integration.py](../tests/test_pipeline_golden_path_integration.py)

Coverage:

- generation stage order
- YAML-over-DB merge behavior
- run-only blank semantics in DOCX params
- end-to-end non-browser golden path

### Failure, lifecycle, and safety contracts

- [../tests/test_failure_reporting_integration.py](../tests/test_failure_reporting_integration.py)
- [../tests/test_state_json_lifecycle_contract.py](../tests/test_state_json_lifecycle_contract.py)
- [../tests/test_workspace_policy.py](../tests/test_workspace_policy.py)
- [../tests/test_retry_policy.py](../tests/test_retry_policy.py)

Coverage:

- failure registry behavior
- `state.json` lifecycle regression safety
- local artifact policy
- retry classification

## 3. Recommended Rollout Sweep

The practical rollout sweep is:

```bash
venv/bin/python -m pytest -q \
  tests/test_validation_layer.py \
  tests/test_immobili_document_loading.py \
  tests/test_generation_spider.py \
  tests/test_visura_stage_services.py \
  tests/test_attestazione_generator_baseline.py \
  tests/test_db_repo_patch_characterization.py \
  tests/test_db_repo_postgres_integration.py \
  tests/test_prepare_by_cf_service.py \
  tests/test_prepare_by_cf_cli.py \
  tests/test_clients_csv_loading.py \
  tests/test_bulk_import_clients_csv_service.py \
  tests/test_bulk_import_clients_csv_cli.py \
  tests/test_import_only_runner.py \
  tests/test_visura_import_orchestration.py \
  tests/test_pipeline_golden_path_integration.py \
  tests/test_failure_reporting_integration.py \
  tests/test_state_json_lifecycle_contract.py \
  tests/test_workspace_policy.py \
  tests/test_retry_policy.py
```

## 4. When Live Smoke Is Still Needed

Automated tests are not a substitute for live AE/SISTER smoke when a change
touches:

- `uppi/ae/*`
- `uppi/spiders/uppi_browser_spider.py`
- `uppi/spiders/uppi_import_spider.py`
- `uppi/services/import_only_runner.py`
- `state.json` lifecycle handling

Canonical checklist:
[./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

## 5. Confidence Notes

What the current suite gives strong confidence on:

- the three-mode split
- prepare owning fetch/update decisions
- generation staying DB/YAML-driven only
- field-class semantics for `"-"`
- write-back limited to persistable surfaces

What it does not replace:

- live AE/SISTER smoke after browser-critical changes
- production-like credential/environment validation
