# Rollout-Ready Checklist

Це короткий pre-flight і smoke checklist перед практичним використанням нового
split між `prepare`, bulk import-only і generation-only.

Operator workflow:
[./operator_workflow.md](./operator_workflow.md)

Regression map:
[./regression_test_map.md](./regression_test_map.md)

## 1. Pre-Flight Checks

- `README.md` points to the correct canonical docs
- `scrapy crawl uppi` is treated as generation-only in docs and operator usage
- `prepare-by-CF` is treated as the owner of fetch/update decisions
- `clients.yml` is not described as the canonical generation input
- `UPPI_IMMOBILI_YAML` and `UPPI_CLIENTS_CSV` are documented
- browser-critical invariants are unchanged

## 2. Required Automated Test Sweep

Run:

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

Expected result:

- all tests pass

## 3. Prepare Smoke Checklist

Command:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

Verify:

- the command exits successfully
- it writes a single-client `immobili.yml`
- root document contains `LOCATORE_CF`
- document shape is `root mapping + immobili: [...]`
- run-only fields are blank by default
- immobile ordering is deterministic

If DB data may be stale:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z --force-update-visura
```

## 4. Bulk Import Smoke Checklist

Command:

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
```

Verify:

- the command prints a readable summary
- duplicates are reported as skipped duplicates
- malformed CF rows are reported as skipped invalid
- successful rows complete import-only mode
- no `immobili.yml` is generated

Optional stricter mode:

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv --fail-fast
```

## 5. Generation Smoke Checklist

Command:

```bash
scrapy crawl uppi
```

Verify:

- `immobili.yml` loads successfully
- only active/enabled immobili are processed
- strict DB matching succeeds for each active record
- generation stages run after matching
- no browser/import path is invoked
- write-back touches only persistable fields
- run-only `"-"` clears only the current run context

If generation fails with prepare guidance:

- rerun `prepare-by-CF`
- do not expect `scrapy crawl uppi` to fix missing DB state by itself

## 6. Live Smoke Trigger

Live AE/SISTER smoke is required only if the change set touched the protected
browser/import reuse path.

Use:
[./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

Typical trigger areas:

- `uppi/ae/*`
- `uppi/spiders/uppi_browser_spider.py`
- `uppi/spiders/uppi_import_spider.py`
- `uppi/services/import_only_runner.py`
- `state.json` lifecycle handling

## 7. High-Risk Regression Areas

Pay extra attention to:

- browser/import reuse
- generation-only boundary
- strict DB match
- `"-"` field semantics
- persistable-only write-back
- `state.json` lifecycle safety

## 8. Sign-Off Questions

- Are docs aligned with actual code, not just earlier rollout plans?
- Is `prepare-by-CF` clearly documented as the fetch/update owner?
- Is bulk mode clearly documented as import-only?
- Is generation clearly documented as SISTER-free?
- Are `"-"` semantics documented without ambiguity?
- Are the critical regression suites green?
- If browser-sensitive code changed, was live smoke completed?
