# Sprint 2 Merge-Readiness Checklist

> Historical / archival merge artifact.
> Це закритий merge gate для Sprint 2, а не current engineering handbook.
> Для поточної навігації по проєкту використовуйте [README.md](../../README.md),
> [Поточну архітектуру](../current_architecture.md) і
> [Live smoke checklist](../live_smoke_strategy_ae_sister.md).

Цей документ є practical merge gate для Sprint 2 closeout.
Він не підтверджує merge автоматично: manual live smoke sign-off лишається
обов'язковим перед merge.

Пов'язані документи:

- Sprint 2 architecture snapshot: [./sprint_2_architecture_state.md](./sprint_2_architecture_state.md)
- canonical live smoke checklist: [../live_smoke_strategy_ae_sister.md](../live_smoke_strategy_ae_sister.md)
- live smoke sign-off template: [../live_smoke_signoff_template_ae_sister.md](../live_smoke_signoff_template_ae_sister.md)
- protected invariants: [../refactor_protected_invariants.md](../refactor_protected_invariants.md)

## 1. Automated Gates

Перед merge мають бути green:

- `venv/bin/python -m pytest -q`
- [tests/test_pipeline_golden_path_integration.py](../../tests/test_pipeline_golden_path_integration.py)
- [tests/test_db_repo_postgres_integration.py](../../tests/test_db_repo_postgres_integration.py)
- [tests/test_db_repo_patch_characterization.py](../../tests/test_db_repo_patch_characterization.py)
- [tests/test_visura_pdf_parser_baseline.py](../../tests/test_visura_pdf_parser_baseline.py)
- [tests/test_attestazione_generator_baseline.py](../../tests/test_attestazione_generator_baseline.py)
- [tests/test_validation_layer.py](../../tests/test_validation_layer.py)
- [tests/test_domain_exceptions.py](../../tests/test_domain_exceptions.py)
- [tests/test_visura_stage_services.py](../../tests/test_visura_stage_services.py)

## 2. Structure Review

Перед merge треба підтвердити:

- repo layer живе в `uppi/services/repositories/*`
- compatibility facade у [uppi/services/db_repo.py](../../uppi/services/db_repo.py) лишається валідним
- patch semantics винесені у `uppi/services/policies/*`
- validation layer живе окремо від repo SQL logic
- typed domain exceptions живуть окремо від browser flow
- `VisuraProcessor` лишається orchestrator, а не redesign-нутим pipeline engine

## 3. Confirmed Constraints

Перед merge має бути явно підтверджено:

- browser-critical flow untouched
- `state.json` semantics untouched
- no selector/wait/logout redesign
- no direct SISTER / CAPTCHA / download redesign
- no transaction-boundary redesign
- decomposition was extraction-first, not behavioral redesign

## 4. Manual Live Smoke Requirement

Manual live smoke sign-off є required before merge.

Що треба використати:

- checklist: [docs/live_smoke_strategy_ae_sister.md](../live_smoke_strategy_ae_sister.md)
- sign-off artifact: [docs/live_smoke_signoff_template_ae_sister.md](../live_smoke_signoff_template_ae_sister.md)

Коли Sprint 2 merge blocked:

- якщо automated gates не green
- якщо live smoke не прогнаний вручну
- якщо sign-off template не заповнений sanitized evidence після реального run

## 5. Evidence Requirements

У sign-off artifact мають бути:

- дата
- branch / commit
- environment
- account used
- result `PASS` / `FAIL`
- failed step, якщо був
- sanitized notes
- approver / owner

Не можна:

- прикладати raw CAPTCHA content
- прикладати session-bearing data
- прикладати reusable `state.json`
- писати повні sensitive identifiers без потреби

## 6. Recommended Closeout Sequence

1. Прогнати full `pytest`.
2. Підтвердити, що targeted suites для repo/parser/docx/stages green.
3. Виконати manual live smoke за canonical checklist.
4. Заповнити sign-off template sanitized evidence.
5. Лише після цього маркувати Sprint 2 як merge-ready.

## 7. Scope Guard For Closeout

У Sprint 2 closeout не домішувати:

- failure registry
- retry matrix
- transaction/resource-safety review
- strategy abstraction
- workspace/path abstraction
- production code relocation out of `uppi/docs/`
- behavior fixes

## 8. Merge Decision

`READY FOR MERGE` лише якщо одночасно виконані всі три умови:

- automated gates green
- manual live smoke `PASS`
- sign-off artifact заповнений і прив'язаний до конкретного branch / commit
