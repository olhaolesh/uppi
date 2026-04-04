# Refactor Execution Plan — Sprint 2

> Historical / archival planning artifact.
> Sprint 2 already implemented; цей файл не є current runbook.
> Для поточного коду читайте [README.md](../README.md),
> [Поточну архітектуру](./current_architecture.md) і
> [Основний runtime flow](./runtime_flow.md).

## Sprint Goal

Головна мета Sprint 2: зменшити coupling у сервісному й DB-шарі без зміни browser-critical behavior та без зміни `state.json` lifecycle semantics.

Чому цей спринт іде другим:

- до split `db_repo` і decomposition `VisuraProcessor` спочатку потрібні baseline tests і нормальне логування;
- structural refactor без Sprint 1 дасть високий ризик прихованого regression.

## Scope In

- Config / DI foundation.
- `UPPI_CLIENTS_YAML` support і config normalization.
- Repository boundary cleanup.
- Patch policy extraction із `db_repo`.
- Validation layer для YAML/domain inputs.
- Domain exceptions.
- Decomposition `VisuraProcessor` у thin orchestrator + stage services.
- Repo integration tests і service-level golden tests.
- Українські docstrings для всіх модулів, які торкаються цим рефакторингом.

## Scope Out

- Failure registry / failed_jobs persistence.
- Workspace abstraction and path redesign.
- Calculation strategy abstraction.
- Production code relocation out of `uppi/docs/`.
- Transaction-boundary redesign across DB / S3 / DOCX.
- Будь-які browser flow changes.
- Будь-які `state.json` lifecycle changes.

## Що НЕ можна чіпати в межах цього спринту

- `state.json` semantics не змінювати.
- AE/SISTER flow не змінювати.
- visura download flow не змінювати.
- selector order / wait sequence не змінювати без окремого high-risk stage.
- logout semantics не змінювати.
- direct SISTER entry behavior не змінювати.

## Ordered Worklist

1. Підготувати composition root для config/dependencies без зміни дефолтної поведінки:
   connection factory, storage factory, clients path resolution.
2. Додати support для `UPPI_CLIENTS_YAML` і уніфікувати config access, не ламаючи current defaults.
3. Виділити thin repositories із `uppi/services/db_repo.py` у логічні модулі:
   `address`, `person`, `visura`, `immobile`, `contract`, `audit`.
4. Винести smart patch logic, delete-on-`"-"` logic і fallback/default rules у окремі patch/policy services.
5. Ввести validation layer для YAML input, parser output і canone input preparation.
6. Ввести typed domain exceptions і класифікацію recoverable vs non-recoverable errors.
7. Перетворити `VisuraProcessor` на thin orchestrator з окремими stage services:
   `PersonSync`, `VisuraIngest`, `ImmobileSync`, `ContractSync`, `CanoneStage`, `DocumentStage`, `AuditStage`.
8. Додати repo integration tests на temp Postgres і service-level tests на нові stage boundaries.
9. Оновити українські docstrings і technical docs для нових boundaries.
10. Наприкінці спринту прогнати golden path і один live smoke run на end-to-end сценарій.

## Dependencies

- Крок 1 залежить від Sprint 1 logging/testing baseline.
- Крок 2 залежить від кроку 1.
- Крок 3 залежить від characterization tests із Sprint 1.
- Крок 4 залежить від кроку 3.
- Крок 5 можна починати після кроків 1–2, але merge бажано після кроку 4.
- Крок 6 залежить від кроку 5.
- Крок 7 залежить від кроків 3–6.
- Крок 8 залежить від кроків 3–7 і від baseline golden-path tests зі Sprint 1.
- Крок 9 виконується по мірі завершення кроків 3–7.
- Крок 10 залежить від green status усіх попередніх кроків.

## Risk Assessment

### Task Group: Config / DI Foundation

- Risk level: Medium
- Чому: змінюються точки створення залежностей.
- Що може піти не так: default behavior перестане збігатися з current env-based behavior.
- Зменшення ризику: additive constructors/factories; current defaults must stay the same; tests on config resolution.

### Task Group: `UPPI_CLIENTS_YAML` Support and Config Normalization

- Risk level: Low
- Чому: change localized and easy to verify.
- Що може піти не так: неочікувана зміна clients file resolution.
- Зменшення ризику: preserve current default path; add tests for env override and default fallback.

### Task Group: Repository Split

- Risk level: Medium
- Чому: великий файл з багатьма SQL contracts.
- Що може піти не так: загубиться SQL behavior, implicit fallback, transaction expectations.
- Зменшення ризику: спершу characterization tests; split without semantic changes; keep compatibility imports on first pass.

### Task Group: Patch Policy Extraction

- Risk level: Medium
- Чому: це справжня бізнес-логіка, не просто mechanical move.
- Що може піти не так: зміниться meaning empty/None/`"-"`/default.
- Зменшення ризику: explicit tests for each case; extract behavior into pure functions before rewiring repositories.

### Task Group: Validation Layer and Domain Exceptions

- Risk level: Medium
- Чому: validation може почати валити кейси, які раніше “мовчки проходили”.
- Що може піти не так: false positives on existing real data.
- Зменшення ризику: спершу soft validation as warnings for questionable cases; hard fail only on clearly invalid contracts or parser outputs.

### Task Group: `VisuraProcessor` Decomposition

- Risk level: Medium
- Чому: це центральний orchestration node.
- Що може піти не так: зміна call order, transaction boundaries, audit timing, artifact lookup order.
- Зменшення ризику: decomposition by extraction, not redesign; keep same stage order; protect old import surface until golden-path tests are green; transaction-boundary redesign не входить у Sprint 2.

## Regression / Test Gates

### Що має бути готове до початку

- Усі Sprint 1 characterization tests.
- Parser baseline tests.
- DOCX baseline tests.
- Golden-path baseline test.
- Live smoke checklist і documented protected invariants зі Sprint 1.

### Що треба додати в межах спринту

- Temp Postgres integration tests для split repositories.
- Pure-function tests для patch policies.
- Validation tests for YAML and parser outputs.
- Service-level tests for stage services.

### Перед merge великих задач

- `pytest -q`
- Repo integration tests.
- Golden-path tests.
- Targeted docx/parser tests.

### Де потрібен live smoke

- Після завершення decomposition `VisuraProcessor`.
- Перед merge великого PR, який змінює pipeline orchestration around storage/parse/docx.

## Acceptance Criteria

- Config access для target critical modules проходить через central provider/factory layer або явні injected dependencies.
- `UPPI_CLIENTS_YAML` підтримується з current default fallback.
- `db_repo` або розбитий, або має clearly isolated repo submodules з compatibility surface.
- Smart patch logic винесена з repository layer в окремі policy units.
- `VisuraProcessor` став thin orchestrator або суттєво thinner, ніж був до спринту.
- Validation і typed exceptions покривають critical non-browser inputs.
- Golden path не змінює current user-visible behavior.

## Deliverables

- Config/DI foundation.
- Normalized clients/config resolution.
- Split repositories or repo submodules.
- Patch policy modules.
- Validation layer.
- Domain exception hierarchy.
- Decomposed processor/stage services.
- New repo and service tests.
- Updated Ukrainian docstrings and architecture notes for modules touched in Sprint 2.

## Rollback / Containment Notes

- Робити split `db_repo` і decomposition `VisuraProcessor` окремими PR, не одним великим merge.
- На першому проході зберігати compatibility imports, щоб rollback не вимагав масового rename.
- Якщо golden path ламається після processor decomposition, відкотити лише orchestration patch, не чіпаючи вже додані tests/docs.
- Validation спершу робити warning-first там, де бізнес-семантика неочевидна.
