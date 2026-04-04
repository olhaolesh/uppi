# `state.json` Lifecycle Contract

Цей документ є canonical technical note для `state.json`.
Protected invariant short-form лишається таким самим:

`fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`

Мета цього документа — пояснити current ownership map і current cleanup shape
без переосмислення lifecycle semantics.

## Current Ownership Map

- Fresh session cleanup:
  [uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)
  у `start()`.
- Load point для direct SISTER transition:
  [uppi/settings.py](../uppi/settings.py).
- Save point під час AE / SISTER flow:
  [uppi/ae/sister_navigation.py](../uppi/ae/sister_navigation.py)
  у `open_sister_service(...)`.
- Failed-login invalidation cleanup:
  [uppi/ae/auth.py](../uppi/ae/auth.py)
  у `authenticate_user(...)`.
- Path/wrapper surface:
  [uppi/config/workspace.py](../uppi/config/workspace.py).

Stage ownership не переносився:

- `uppi/settings.py` лише підхоплює existing path у Playwright context.
- `uppi/ae/sister_navigation.py` лише зберігає state у вже чинній точці flow.
- `uppi/ae/auth.py` лише invalidates broken state after failed login.
- `uppi/spiders/uppi_spider.py` лишається owner-ом fresh-session cleanup.

## Current Lifecycle Shape

1. New spider session починається з cleanup старого `state.json`.
2. Після успішного AE -> SISTER transition state зберігається в `state.json`.
3. Direct SISTER transition покладається на current Playwright `storage_state`
   path, якщо `state.json` уже існує в поточному runtime lifecycle.
4. Explicit logout лишається business-critical частиною session end.
5. Після logout попередній state треба вважати semantically invalid.
6. Current code не має окремого delete-hook exactly at logout time.
7. Actual delete/cleanup today відбувається:
   - на next fresh session start;
   - або після failed login, якщо state лишився битим.

## Explicitly Forbidden

- Reuse старого `state.json` між новими сесіями.
- Перенесення create/load/delete points.
- Перетворення `state.json` на persistent reusable cache.
- Заміна explicit logout на passive browser close.
- Будь-який selector/wait/click redesign під приводом state handling.

## Safe Wrapper Surface

Current wrapper API intentionally thin:

- `get_state_json_path()`
- `bind_existing_state_json_storage_state(...)`
- `save_state_json_snapshot(...)`
- `delete_state_json_if_present(...)`

Ці helper-и не міняють ownership і не вводять нових lifecycle stages.
Вони лише:

- централізують path resolution;
- роблять metadata-only logging;
- дають testable surface для characterization tests.

## Logging Rules

Для `state.json` дозволений лише metadata-only logging:

- state saved
- state loaded/bound
- cleanup triggered
- cleanup skipped because file absent
- cleanup failed

Не можна:

- логувати raw storage-state payload;
- логувати cookies/session-bearing values;
- логувати вміст `state.json`.

## Cross-References

- Protected invariants:
  [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- Current runtime flow:
  [./runtime_flow.md](./runtime_flow.md)
- Workspace / local artifacts policy:
  [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- Live smoke checklist:
  [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)
