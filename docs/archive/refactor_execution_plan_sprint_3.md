# Refactor Execution Plan — Sprint 3

> Historical / archival planning artifact.
> Sprint 3 planning context збережений для історії, але не є current guide.
> Для актуальних boundaries дивіться [README.md](../../README.md),
> [Поточну архітектуру](../current_architecture.md) і
> [AWS-readiness note](../aws_readiness_runtime_boundaries.md).

## Sprint Goal

Головна мета Sprint 3: підготувати проект до наступного етапу стабілізації, AWS-ready runtime boundaries і deeper cleanup без втручання в browser-critical invariants.

Чому цей спринт третій:

- failure tracking, strategy abstraction і workspace policy мають спиратися на вже стабілізовані service/repository boundaries;
- робити AWS-oriented cleanup до repo/processor refactor означало б цементувати поточний coupling.

## Scope In

- Failure registry / failed_jobs / client_failures.
- Retryable / non-retryable classification.
- Transaction / resource safety review and containment plan.
- Calculation strategy abstraction.
- Workspace / local artifacts policy.
- AWS-readiness preparation docs і runtime boundary hardening.
- Compatibility-shim based cleanup preparation for production code in wrong folders.
- Розширені українські docstrings і technical docs for final architecture.

## Scope Out

- Browser flow redesign.
- `state.json` lifecycle redesign.
- Transaction-boundary rewrite before stable repo/service tests.
- Selector or Playwright sequence changes.
- Headless/runtime behavior redesign for AE/SISTER.
- Aggressive artifact lifecycle changes without live smoke.

## Що НЕ можна чіпати в межах цього спринту

- `state.json` semantics не змінювати.
- AE/SISTER flow не змінювати.
- visura download flow не змінювати.
- selector order / wait sequence не змінювати без окремого high-risk stage.
- logout semantics не змінювати.
- direct SISTER entry behavior не змінювати.

## Ordered Worklist

1. Ввести failure registry model і storage contract:
   `run_id`, `client_cf`, `stage`, `error_type`, `retryable`, `message_redacted`, `artifact_refs`.
2. Додати stage-level failure reporting у processor/stage services без зміни browser logic.
3. Визначити retry policy matrix:
   infra-only retry, no blind browser retry, explicit classification of retryable failures.
4. Провести transaction/resource-safety review:
   зафіксувати current transaction ownership, rollback points, allowed containment moves і цільові unit-of-work boundaries без transaction-boundary rewrite у цьому спринті.

   Canonical artifact після виконання цього кроку:
   - [docs/transaction_resource_safety_review.md](../transaction_resource_safety_review.md)

5. Ввести `CalculationStrategy` boundary з default `Pescara2018Strategy`, не змінюючи поточні формули.
6. Ввести workspace/local artifacts policy:
   `downloads/`, `captcha_images/`, DOCX/PDF temporary artifacts, cleanup contract, configurable workspace root with unchanged default paths on first pass.

   Canonical artifact після виконання цього кроку:
   - [docs/workspace_local_artifacts_policy.md](../workspace_local_artifacts_policy.md)

7. Для `state.json` дозволені тільки better logging, better docs, better characterization/tests і, за потреби, wrapper API без зміни контракту
   `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.
8. Підготувати AWS-readiness package:
   config provider readiness for Secrets Manager/SSM, object storage boundary, DB connection factory notes, runtime recommendations for ECS/Fargate first.

   Canonical artifact після виконання цього кроку:
   - [docs/aws_readiness_runtime_boundaries.md](../aws_readiness_runtime_boundaries.md)
9. Підготувати compatibility-shim migration slice для production code in `uppi/docs/`, не видаляючи старий import path у тому самому спринті, only if Sprint 2 tests are stable.

   Canonical artifact після виконання цього кроку:
   - [docs/compatibility_shim_migration_uppi_docs.md](../compatibility_shim_migration_uppi_docs.md)
10. Оновити final technical docs і українські docstrings для нової цільової архітектури.
11. Завершити спринт повним regression pass:
    unit + integration + golden path + live smoke where needed.

## Dependencies

- Кроки 1–3 залежать від Sprint 2 stage boundaries і typed exceptions.
- Крок 4 залежить від Sprint 2 repo/service boundaries і golden-path tests.
- Крок 5 залежить від stable canone tests і decomposition results Sprint 2.
- Крок 6 залежить від Sprint 1 artifact characterization і Sprint 2 config foundation.
- Крок 7 залежить від кроку 6, але не змінює semantics.
- Крок 8 залежить від кроків 4–7.
- Крок 9 залежить від green status усіх core tests і відсутності unresolved regressions після Sprint 2.
- Крок 10 робити по мірі завершення попередніх кроків.
- Крок 11 залежить від завершення всіх змін спринту.

## Risk Assessment

### Task Group: Failure Registry

- Risk level: Medium
- Чому: зачіпає error propagation, audit surface і partial failure semantics.
- Що може піти не так: дублювання failure records або неконсистентна stage classification.
- Зменшення ризику: ввести просту, append-only схему; не змінювати existing success flow.

### Task Group: Retry Policy Matrix

- Risk level: Medium
- Чому: легко випадково додати retry туди, де він заборонений.
- Що може піти не так: повторні дії в browser-critical flow зламають session semantics.
- Зменшення ризику: explicit “no retry” list for AE/SISTER/auth/captcha/download/logout paths.

### Task Group: Transaction / Resource Safety Review

- Risk level: Medium
- Чому: зона вже містить довгі DB + S3 + DOCX зв’язки, але Sprint 3 не повинен перетворитися на transaction rewrite.
- Що може піти не так: review непомітно переросте в behavioral change transaction boundaries.
- Зменшення ризику: обмежити Sprint 3 assessment/containment scope; будь-який unit-of-work redesign виносити в окремий follow-up після stable tests.

### Task Group: Calculation Strategy Abstraction

- Risk level: Medium
- Чому: change in abstraction around core calculation path.
- Що може піти не так: strategy wrapper змінить current serialization or method naming.
- Зменшення ризику: default strategy must produce byte-for-byte equivalent result snapshots where possible.

### Task Group: Workspace / Local Artifacts Policy

- Risk level: Medium
- Чому: path changes можуть зачепити file discovery contracts.
- Що може піти не так: visura або DOCX перестануть знаходитися локально.
- Зменшення ризику: default paths лишаються тими самими; abstraction only, no semantic relocation on first pass.

### Task Group: `state.json` Encapsulation

- Risk level: High
- Чому: будь-який необережний рух тут може бути витлумачений як lifecycle change.
- Що може піти не так: випадкове перенесення точки create/load/delete або implicit reuse logic.
- Зменшення ризику: дозволені тільки docs, logging, characterization tests і wrapper API, який зберігає точний контракт
  `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.

### Task Group: Production Code in Wrong Folder Cleanup

- Risk level: Medium
- Чому: path/import coupling already exists.
- Що може піти не так: runtime import failure in document generation path.
- Зменшення ризику: compatibility shim and two-step migration; no mass move in one PR.

## Regression / Test Gates

### Що має бути готове до початку

- Усі Sprint 1 і Sprint 2 tests.
- Stable golden-path pipeline tests.
- Stable repo integration tests.

### Що треба додати в межах спринту

- Failure registry tests.
- Retry policy tests.
- Transaction/resource-safety characterization note or tests for unchanged boundaries, якщо робляться containment changes.
- Strategy equivalence tests for current Pescara calculations.
- Workspace/path resolution tests.
- Artifact cleanup policy tests.

### Перед merge великих задач

- `pytest -q`
- Golden-path tests.
- Repo/service integration tests, якщо чіпались transaction/resource containment paths.
- Strategy equivalence tests.
- Storage/workspace tests.

### Де потрібен live smoke

- Будь-який PR, який торкається artifact path resolution in end-to-end flow.
- Будь-який PR, який інкапсулює `state.json` handling even without semantics change.
- Будь-який PR, який торкається cleanup/path handling навколо logout/state artifacts.
- Фінальний end-of-sprint smoke on known-good account.

## Acceptance Criteria

- Є documented і code-level failure registry contract.
- Є explicit retry matrix with no-blind-retry rule for browser-critical stages.
- Є transaction/resource-safety review note з чітким розділенням між allowed containment work і deferred transaction rewrite.
- Calculation path відокремлений через strategy boundary без зміни current outputs.
- Local artifacts policy задокументована й інкапсульована з unchanged default paths on first pass.
- `state.json` lifecycle задокументований і краще спостережуваний, але не змінений.
- AWS-readiness recommendations перетворені на concrete architecture boundaries, а не лише загальні поради.
- Production code in wrong folders має documented migration path або safe shim plan.

## Deliverables

- Failure registry contract and initial implementation slice.
- Retry matrix documentation and tests.
- Transaction/resource-safety review note and containment plan.
- Calculation strategy boundary.
- Workspace/local artifacts policy.
- AWS-readiness technical note.
- Compatibility-shim migration slice or documented shim plan for code in wrong folders.
- Final architecture docs and Ukrainian docstrings.

## Rollback / Containment Notes

- Failure registry вводити append-only і окремо від current success logs.
- Transaction/resource-safety work у цьому спринті обмежувати assessment/containment changes; redesign unit-of-work не змішувати з workspace або strategy PR.
- Strategy abstraction вводити wrapper-first, without changing current calculator internals.
- Workspace abstraction merge only with unchanged default paths.
- Будь-які change sets, що торкаються `state.json`, тримати окремими PR і вимагати explicit live smoke sign-off.
