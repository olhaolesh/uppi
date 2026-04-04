# Live Smoke Sign-Off Template — AE / SISTER

Це шаблон для ручного sign-off після реального live smoke run.
Він не є evidence сам по собі. Його треба заповнювати лише після фактичного
ручного прогону за canonical checklist:
[./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md).

Пов'язані документи:

- canonical checklist: [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)
- protected invariants: [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- current runtime flow: [./runtime_flow.md](./runtime_flow.md)

## Як використовувати

1. Виконати ручний smoke run.
2. Зафіксувати тільки sanitized evidence.
3. Заповнити цей шаблон для конкретного branch / commit.
4. Не позначати `PASS`, якщо smoke фактично не виконувався.

## Заборонено включати в sign-off

- raw CAPTCHA content
- cookies / session-bearing data
- вміст `state.json`
- reusable session artifacts
- повні секрети / токени / паролі
- повні sensitive identifiers без реальної необхідності

## Шаблон

```text
Дата:
Branch:
Commit:
Environment:
Runner:
Known-good account used:
Known-good CF / scenario:

Checklist version:
Protected invariants reviewed: YES / NO

Result: PASS / FAIL
Failed step:
Observed regression signals:
Sanitized notes:
Evidence location:
Follow-up needed: YES / NO

Approver / sign-off:
Sign-off time:
```

## Мінімальний критерій для `PASS`

- AE login пройшов
- direct SISTER transition пройшов
- `Visure catastali` відкрилися
- download реально відбувся
- explicit logout виконано
- invalid `state.json` не лишився reusable artifact

## Якщо результат `FAIL`

- зафіксувати перший зламаний крок
- не маскувати failure загальною фразою
- залишити короткий sanitized note, достатній для повторення проблеми
