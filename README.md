# UPPI

UPPI працює у трьох окремих режимах:

- `prepare-by-CF`: вирішує, чи потрібен fetch/update, за потреби reuse-ить import-only browser path і генерує один single-client `immobili.yml`
- `bulk-import-by-clients-csv`: масово проходить import-only path по списку CF і оновлює БД без generation
- `scrapy crawl uppi`: generation-only command, який читає вже prepared `immobili.yml` і не ходить у SISTER

Canonical behavioral contract зафіксований у
[docs/immobili_rollout_source_of_truth.md](docs/immobili_rollout_source_of_truth.md).

## What Changed

- `immobili.yml` тепер є canonical generation input і описує рівно одного клієнта у форматі `root fields + immobili: [...]`
- `prepare-by-CF` став єдиним owner-ом fetch/update decision logic
- `clients.csv` винесено в окремий bulk import-only mode
- `scrapy crawl uppi` більше не є smart fetch/import runner
- generation не має hidden fallback у SISTER: missing DB immobile дає hard fail з підказкою спочатку запустити prepare
- field-level validation і `"-"` semantics централізовані для нового single-client contract

## Primary Commands

Prepare one client and write `immobili.yml`:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

Force visura refresh before generating YAML:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z --force-update-visura
```

Bulk import-only from `clients.csv`:

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
```

Generation-only run from prepared YAML:

```bash
scrapy crawl uppi
```

## Recommended Workflow

For one client:

1. Run `prepare-by-CF`.
2. Review and edit the generated `immobili.yml`.
3. Set `enabled: false` on any immobile you want to skip.
4. Use `"-"` only on fields documented as clear-allowed.
5. Run `scrapy crawl uppi`.

For many clients:

1. Run bulk CSV import-only mode to refresh DB state for all target CFs.
2. For each client that needs generation, run `prepare-by-CF`.
3. Review the generated YAML and then run `scrapy crawl uppi`.

Recommended operator guide:
[docs/operator_workflow.md](docs/operator_workflow.md).

## Documentation Map

Canonical contract and operator docs:

- [docs/immobili_rollout_source_of_truth.md](docs/immobili_rollout_source_of_truth.md)
- [docs/operator_workflow.md](docs/operator_workflow.md)
- [docs/validation_clear_policy_matrix.md](docs/validation_clear_policy_matrix.md)

Runtime and architecture:

- [docs/runtime_flow.md](docs/runtime_flow.md)
- [docs/current_architecture.md](docs/current_architecture.md)
- [docs/local_development_and_testing.md](docs/local_development_and_testing.md)

Testing, smoke, rollout:

- [docs/regression_test_map.md](docs/regression_test_map.md)
- [docs/rollout_ready_checklist.md](docs/rollout_ready_checklist.md)
- [docs/live_smoke_strategy_ae_sister.md](docs/live_smoke_strategy_ae_sister.md)

Protected invariants and reference docs:

- [docs/refactor_protected_invariants.md](docs/refactor_protected_invariants.md)
- [docs/state_json_lifecycle_contract.md](docs/state_json_lifecycle_contract.md)
- [docs/document_generation.md](docs/document_generation.md)
- [docs/failure_registry_contract.md](docs/failure_registry_contract.md)

Historical planning artifacts:

- [docs/uppi_update_implementation_plan.md](docs/uppi_update_implementation_plan.md)
- [docs/Uppi_Покроковий_План_Виконання.md](docs/Uppi_Покроковий_План_Виконання.md)
- [docs/archive/refactor_execution_plan_overview.md](docs/archive/refactor_execution_plan_overview.md)

## Configuration Surface

Canonical generation input:

- `UPPI_IMMOBILI_YAML`

Bulk CSV input:

- `UPPI_CLIENTS_CSV`

Legacy compatibility only:

- `UPPI_CLIENTS_YAML`
  This remains only as a transitional/internal source for the protected import spider path and is not the canonical generation contract.

## Testing

Focused regression guidance lives in
[docs/regression_test_map.md](docs/regression_test_map.md).

Default full test command:

```bash
venv/bin/python -m pytest -q
```

Rollout pre-flight and smoke checklist:
[docs/rollout_ready_checklist.md](docs/rollout_ready_checklist.md).

## Live Smoke

Live AE/SISTER smoke is only required when a change touches the protected browser/import reuse path or `state.json` lifecycle. The canonical checklist is
[docs/live_smoke_strategy_ae_sister.md](docs/live_smoke_strategy_ae_sister.md).
