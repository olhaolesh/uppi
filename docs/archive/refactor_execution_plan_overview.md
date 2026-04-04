# Refactor Execution Plan Overview

> Historical / archival planning artifact.
> Цей файл корисний для історії рефакторингу, але не є current operational guide.
> Для актуального стану проєкту починайте з [README.md](../../README.md),
> [Поточної архітектури](../current_architecture.md) і
> [Основного runtime flow](../runtime_flow.md).

## Executive Summary

Цей execution-plan стискає вже підготовлений architecture audit у 3 реальні ітерації:

- `Sprint 1` — зафіксувати поточну поведінку, прибрати сліпі зони в логуванні, закрити витоки секретів, вирівняти tooling drift.
- `Sprint 2` — зробити основний структурний рефакторинг поза browser-critical flow: config/DI foundation, repository boundary cleanup, patch policy extraction, початок декомпозиції `VisuraProcessor`.
- `Sprint 3` — підготувати проект до наступного етапу стабілізації й AWS-ready execution: failure registry, transaction/resource-safety review, strategy boundary, workspace/local artifacts policy, deeper technical docs.

План навмисно не містить перепроєктування AE/SISTER/state lifecycle semantics. Для цих зон діє режим `protected invariant`.

## Protected Invariants

Повний перелік див. у `docs/refactor_protected_invariants.md`.

Коротко:

- Не змінювати AE authentication flow.
- Не змінювати direct SISTER entry flow.
- Не змінювати CAPTCHA flow.
- Не змінювати visura download flow.
- Не змінювати logout flow.
- Не змінювати selector order.
- Не змінювати wait/click/fill sequence.
- Не змінювати `state.json` lifecycle semantics:
  `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.

## Як розбиті спринти

### Sprint 1

Мета: створити безпечну основу для подальшого рефакторингу.

- Characterization tests.
- Centralized logging.
- Secret redaction.
- Cleanup sensitive logs / prints.
- Runtime/tooling drift fixes.
- Початкові docstrings і protected-invariants docs.

### Sprint 2

Мета: розділити coupling у сервісному та DB-шарі без зміни бізнес-поведінки browser flow.

- Config / DI foundation.
- `UPPI_CLIENTS_YAML` support і config normalization.
- Repository boundary cleanup.
- Patch policy extraction із `db_repo`.
- Validation layer.
- Domain exceptions.
- Decomposition `VisuraProcessor` у stage services.

### Sprint 3

Мета: підготувати проект до наступного етапу стабілізації та AWS-ready runtime boundary.

- Failure registry / failed jobs.
- Transaction / resource safety review and containment plan.
- Calculation strategy abstraction.
- Workspace / local artifacts policy.
- AWS-readiness preparation.
- Runtime-safe relocation plan для production code у нетипових папках.
- Розширені технічні docs і українські docstrings для оновлених модулів.

## Dependency Map Between Sprints

```text
Sprint 1
  -> characterization baseline
  -> logging/redaction baseline
  -> drift fixes

Sprint 2
  depends on Sprint 1 tests and observability
  -> config/DI foundation
  -> repo split
  -> patch policy extraction
  -> processor decomposition

Sprint 3
  depends on Sprint 2 stabilized boundaries
  -> failure registry
  -> transaction/resource-safety review
  -> strategy boundary
  -> workspace abstraction
  -> AWS-ready hardening
```

Ключові внутрішні залежності:

- `VisuraProcessor` decomposition не починати до появи characterization tests і repo tests.
- `db_repo` split не починати до появи baseline tests для smart patch semantics.
- Workspace abstraction не починати до появи characterization coverage для current artifact lifecycle.
- Transaction/resource-safety hardening не починати до стабілізації repo/service boundaries після Sprint 2.
- Будь-який рух навколо `state.json` можливий тільки як documentation / safe hardening, а не redesign.
- Українські docstrings і technical docs є інкрементним deliverable:
  Sprint 1 — protected modules,
  Sprint 2 — modules touched by refactor,
  Sprint 3 — final architecture docs and touched boundaries.

## Overall Risk Map

### Safe Now

- Characterization tests.
- Parser tests.
- Repo tests.
- DOCX tests.
- Golden path test fixtures.
- Centralized logging config.
- Secret redaction.
- Cleanup sensitive logs / prints.
- Tooling drift fixes.
- Ukrainian docstrings for protected-invariant modules.

### Only After Tests

- Config / DI foundation.
- `UPPI_CLIENTS_YAML` support.
- Repository split.
- Patch policy extraction.
- Validation layer.
- Domain exceptions.
- `VisuraProcessor` decomposition.
- Failure registry.
- Transaction/resource-safety review and containment tasks.
- Calculation strategy abstraction.
- Workspace abstraction with unchanged semantics.

### Defer / High Risk

- Selector changes.
- Wait timing changes.
- Click/fill sequence changes.
- Logout timing changes.
- Direct SISTER navigation changes.
- `state.json` semantics changes.
- Browser retry redesign.
- Any change that assumes old `state.json` may be reused across sessions.

## Overall Delivery Strategy

- Працювати короткими PR із малим blast radius.
- Після кожного структурного PR зберігати old import contracts через compatibility wrappers, якщо це потрібно.
- На critical boundaries використовувати спочатку characterization tests, потім code moves.
- Live smoke для AE/SISTER не робити на кожен PR, але робити:
  - наприкінці Sprint 1, якщо чіпались logging calls у browser-adjacent modules;
  - наприкінці Sprint 2, перед merge структурного refactor pipeline/repositories;
  - наприкінці Sprint 3, якщо чіпались workspace paths або local artifact management.

## Overall Test Gates

- До початку Sprint 2 мають існувати characterization tests для smart patch, parser baseline, docx baseline і хоча б один golden-path pipeline test.
- До decomposition `VisuraProcessor` мають існувати repo integration tests на temp Postgres.
- До workspace abstraction має існувати live smoke checklist для AE/SISTER і документований `state.json` invariant.
- До будь-якого transaction-boundary hardening мають існувати stable repo/service tests і golden-path baseline.

## Overall Deliverables

- Execution-plan docs.
- Protected invariants doc.
- Risk register.
- New characterization and integration tests.
- Logging policy and redaction policy.
- Refactor-ready service/repository boundaries.
- Transaction/resource-safety review note and containment plan.
- AWS-readiness prep docs and local artifacts policy.
