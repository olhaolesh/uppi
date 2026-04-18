# Local Development and Testing

Цей документ описує актуальний local setup, command surface і testing guidance
після rollout split.

Operator-facing workflow:
[./operator_workflow.md](./operator_workflow.md)

Regression map:
[./regression_test_map.md](./regression_test_map.md)

## 1. Local Requirements

- Python 3.11
- `venv/`
- PostgreSQL
- S3-compatible storage such as MinIO
- Playwright browser dependencies
- `.env` with AE/SISTER, DB, and storage settings

## 2. Initial Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install
```

## 3. Database

Initialize schema:

```bash
python uppi/utils/db_utils/init_db.py
```

Schema file:

- [../uppi/utils/db_utils/uppi_schema.sql](../uppi/utils/db_utils/uppi_schema.sql)

## 4. Relevant Config Surface

Canonical generation input:

- `UPPI_IMMOBILI_YAML`

Bulk CSV input:

- `UPPI_CLIENTS_CSV`

Legacy compatibility only:

- `UPPI_CLIENTS_YAML`
  This is not the canonical generation source. It remains only for the internal
  import-only spider compatibility seam.

Other useful runtime/config modules:

- [../uppi/config/app_config.py](../uppi/config/app_config.py)
- [../uppi/config/workspace.py](../uppi/config/workspace.py)

## 5. Main Local Commands

### Prepare one client

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

Optional forced refresh:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z --force-update-visura
```

### Bulk import-only from CSV

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
```

Optional strict stop-on-failure mode:

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv --fail-fast
```

### Generation-only run

```bash
scrapy crawl uppi
```

Important:

- `scrapy crawl uppi` expects a prepared `immobili.yml`
- it does not use `clients.yml` as the canonical input
- it does not run browser/import logic

## 6. Internal Command Surface

There is an internal import-only spider:

```bash
python -m scrapy crawl uppi_import
```

This is primarily reused by services and tests. Operators normally use:

- `prepare-by-CF`
- bulk CSV import-only mode

## 7. Recommended Test Commands

### Full test run

```bash
venv/bin/python -m pytest -q
```

### Rollout-focused regression sweep

Use the command from:
[./regression_test_map.md](./regression_test_map.md)

### Rollout pre-flight checklist

Use:
[./rollout_ready_checklist.md](./rollout_ready_checklist.md)

## 8. When Manual Live Smoke Is Needed

Live smoke is required when a change touches the protected browser/import reuse
path or `state.json` lifecycle.

Canonical checklist:

- [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

Typical trigger areas:

- `uppi/ae/*`
- [../uppi/spiders/uppi_browser_spider.py](../uppi/spiders/uppi_browser_spider.py)
- [../uppi/spiders/uppi_import_spider.py](../uppi/spiders/uppi_import_spider.py)
- [../uppi/services/import_only_runner.py](../uppi/services/import_only_runner.py)
- `state.json` lifecycle handling

## 9. Troubleshooting Map

### Prepare or bulk import behavior

Read:

- [./operator_workflow.md](./operator_workflow.md)
- [./runtime_flow.md](./runtime_flow.md)

### Generation behavior

Read:

- [./runtime_flow.md](./runtime_flow.md)
- [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)
- [./document_generation.md](./document_generation.md)

### Browser/import invariants

Read:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

### Failure or artifact consistency

Read:

- [./failure_registry_contract.md](./failure_registry_contract.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)
