# Refactor Risk Register

## R1 — Browser Flow Regression

- Severity: Critical
- Probability: Medium
- Зона: `uppi/spiders/uppi_spider.py`, `uppi/ae/*`
- Ризик: будь-яка зміна step order, selector order, wait sequence або logout semantics може зламати auth/SISTER/download.
- Mitigation: protected invariants, characterization tests, live smoke gate.
- Suggested owner notes: будь-які зміни в цих модулях рев’ювати окремо й не змішувати з structural refactor.

## R2 — `state.json` Lifecycle Misinterpretation

- Severity: Critical
- Probability: High
- Зона: `uppi/settings.py`, `uppi/spiders/uppi_spider.py`, `uppi/ae/auth.py`, `uppi/ae/sister_navigation.py`
- Ризик: команда може помилково прочитати roadmap як дозвіл переписати lifecycle.
- Mitigation: explicit invariant doc, no-semantic-change rule, live smoke gate for any encapsulation work, explicit contract
  `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.
- Suggested owner notes: у PR description окремо писати `state semantics unchanged` і явно перевіряти, що не додано persistent-state reuse.

## R3 — Hidden Business Logic in `db_repo`

- Severity: High
- Probability: High
- Зона: `uppi/services/db_repo.py`
- Ризик: split/move змінить smart patch behavior.
- Mitigation: characterization tests before split, policy extraction in pure functions, temp Postgres tests.
- Suggested owner notes: не змішувати repo split і behavior changes in one PR.

## R4 — God Object Refactor Blast Radius

- Severity: High
- Probability: Medium
- Зона: `uppi/services/visura_processor.py`
- Ризик: decomposition змінить stage order, transaction semantics, audit timing.
- Mitigation: decomposition by extraction, not redesign; golden path tests; live smoke at end of sprint.
- Suggested owner notes: робити incremental stage extraction.

## R5 — Sensitive Data Leakage

- Severity: High
- Probability: High
- Зона: logs, `captcha_images/`, `state.json`, debug outputs
- Ризик: витік CAPTCHA data, session info, CF-linked payloads.
- Mitigation: centralized logging, redaction, no raw payload logging, artifact policy.
- Suggested owner notes: Sprint 1 priority.

## R6 — Transaction / Resource Safety

- Severity: High
- Probability: Medium
- Зона: `uppi/services/visura_processor.py`, `uppi/domain/db.py`, `uppi/services/db_repo.py`
- Ризик: довгі транзакції з DB + S3 + DOCX призведуть до partial failures або завислих rollback paths.
- Mitigation: characterize current behavior first; у межах 3-sprint plan робити тільки review/containment; move transaction ownership upward і shorten units of work виносити в окремий follow-up після stable repo/service tests.
- Suggested owner notes: не міняти transaction boundaries до появи repo/service tests; у Sprint 3 тримати лише assessment/containment scope.

## R7 — Filesystem Coupling Blocks AWS Migration

- Severity: Medium
- Probability: High
- Зона: `downloads/`, `captcha_images/`, `state.json`, local DOCX/PDF artifacts
- Ризик: code assumes repo-local filesystem and current cwd.
- Mitigation: workspace abstraction with unchanged defaults; local artifacts policy.
- Suggested owner notes: Sprint 3 only after tests.

## R8 — Tooling Drift Gives False Signals

- Severity: Medium
- Probability: High
- Зона: `uppi/cli/inspect_clients.py`, `uppi/domain/db.py`, `uppi/utils/db_utils/init_db.py`
- Ризик: debugging and ops scripts show wrong schema assumptions.
- Mitigation: low-risk drift fixes in Sprint 1.
- Suggested owner notes: keep tooling fixes separate from production refactor PRs.

## R9 — Production Code in Wrong Folder

- Severity: Medium
- Probability: Medium
- Зона: `uppi/docs/attestazione_template_filler.py`
- Ризик: later cleanup may break imports and path contracts.
- Mitigation: compatibility shim and two-step migration.
- Suggested owner notes: defer until Sprint 3; не видаляти старий import path у тому самому change set.

## R10 — Validation Hardening Breaks Real Inputs

- Severity: Medium
- Probability: Medium
- Зона: YAML validation, parser output validation, canone input validation
- Ризик: “invalid but currently tolerated” data starts hard-failing.
- Mitigation: start with warning-first validation; escalate only for clearly invalid cases.
- Suggested owner notes: tie validation rollout to explicit tests and sampled real data review.
