# Live Smoke Checklist — AE / SISTER

Цей документ є canonical live smoke checklist для AE / SISTER.
Protected invariants, які не можна переосмислювати під час smoke-перевірок,
описані в [./refactor_protected_invariants.md](./refactor_protected_invariants.md).

## Призначення

- Дати короткий ручний smoke gate для browser-critical зони.
- Підтвердити, що risky change set не зламав AE auth, direct SISTER transition,
  CAPTCHA path, visura download або explicit logout.
- Не допустити неявного redesign `state.json` lifecycle під виглядом harmless cleanup.

## Коли live smoke обов'язковий

- Після великих змін у [uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py).
- Після змін у `uppi/ae/*`.
- Після змін у browser-adjacent logging calls або cleanup навколо AE / SISTER flow.
- Перед merge великого PR, який чіпає AE auth, direct SISTER entry, CAPTCHA,
  visura download, logout або handling `state.json`.
- Наприкінці Sprint 1 як фінальний smoke gate.
- Наприкінці Sprint 2 / Sprint 3 для change sets, що зачіпають risky browser
  або local-artifact boundaries.

## Коли live smoke зазвичай не потрібний

- Docs-only зміни в `docs/`.
- Characterization або baseline tests без змін runtime code.
- Non-browser зміни, які не торкаються protected lifecycle або browser-adjacent
  logging calls.

## Передумови

- Використовувати known-good account або окремий test account з доступом до AE
  і SISTER.
- Використовувати known-good CF і сценарій, де очікується успішний пошук та
  завантаження visura.
- Переконатися, що AE / SISTER доступні й немає відомого maintenance window.
- Використовувати зрозумілий локальний runtime setup: актуальне `.env`,
  доступний браузер, валідні залежності й робочі локальні директорії.
- Стартувати з fresh session і без reusable `state.json` від попереднього run.
- Акуратно поводитися з локальними артефактами: не зберігати старий session
  state як reusable asset і не прикладати sensitive artifacts до smoke notes.
- Smoke evidence і short notes зберігати в sanitized вигляді: без паролів, PIN,
  raw CAPTCHA content, session-bearing data і повних чутливих ідентифікаторів.

## Protected Invariant, який треба перевірити

`fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`

Додатково:

- logout є частиною бізнес-критичного session lifecycle, а не optional cleanup;
- passive browser close не є допустимою заміною logout;
- після некоректного завершення сесії повторний login може бути заблокований
  приблизно на 20 хвилин.

## Smoke Steps

1. Підготувати fresh session і переконатися, що старий `state.json` не
   використовується повторно між новими сесіями.
2. Запустити сценарій з локального runtime setup, який відповідає поточному
   production-like запуску.
3. Перевірити, що AE login проходить через чинний selector order і чинну
   wait/click/fill sequence.
4. Підтвердити, що під час AE / SISTER flow session state зберігається в межах
   поточного lifecycle, а не трактується як persistent reusable state.
5. Перевірити direct SISTER transition через чинний flow, без ручної підміни
   кроків або shortcut-ів.
6. Дійти до `Visure catastali` через чинну navigation sequence.
7. Якщо з'являється CAPTCHA, пройти її через чинний submit flow без зміни
   ordering або timing. Якщо CAPTCHA не з'являється, зафіксувати no-CAPTCHA path.
8. Перевірити, що visura download запускається через чинну послідовність
   `expect_download -> click 'Apri' -> save_as`.
9. Підтвердити, що PDF реально завантажився у локальний runtime artifact path,
   передбачений поточним кодом.
10. Завершити сценарій через explicit logout, а не через passive browser close.
11. Після завершення переконатися, що invalid `state.json` не лишається як
    reusable session artifact.

## Critical Assertions / Expected Outcomes

- AE login проходить без ручного обходу flow.
- Direct SISTER entry працює через поточний контракт.
- Відкривається потрібний сервіс / сторінка `Visure catastali`.
- CAPTCHA path, якщо він виникає, проходить без зміни submit sequence.
- Visura download реально стартує і завершується успішним створенням PDF.
- Logout виконується явно як частина сценарію.
- Після завершення немає semantic misuse старого `state.json`.

## Regression Signals

- Loop, stuck state або неочікуваний stop під час auth чи navigation.
- Злам direct SISTER transition або необхідність ручного обходу переходу.
- Симптоми зміни selector order або wait/click/fill sequence.
- CAPTCHA submit symptoms: форма не сабмітиться, submit іде не тим шляхом,
  з'являється потреба змінити timing або sequence для успіху.
- Download не стартує, не створюється файл або файл не доходить до очікуваного
  artifact path.
- Logout не виконується, замінюється пасивним закриттям браузера або лишає
  сесію в неконсистентному стані.
- Після некоректного завершення повторний login блокується або поводиться
  підозріло.

## Що НЕ можна змінювати під час smoke-related work

- `state.json` semantics.
- Contract `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.
- Direct SISTER contract.
- Selector order.
- Wait/click/fill sequence.
- CAPTCHA submit flow.
- Visura download trigger flow.
- Explicit logout semantics.

## Safe Handling Notes

- Не логувати raw CAPTCHA content або full solve payload.
- Не прикладати session-bearing data, cookie-like values або вміст `state.json`.
- Не зберігати старий `state.json` як reusable artifact між smoke runs.
- Не вставляти у smoke notes повні sensitive identifiers, якщо досить
  sanitized reference.

## Evidence / Sign-Off Template

Для реального manual sign-off використовувати окремий шаблон:
[./live_smoke_signoff_template_ae_sister.md](./live_smoke_signoff_template_ae_sister.md).

```text
Дата:
Branch / commit:
Environment:
Account used:
Known-good CF / scenario:
Result: PASS / FAIL
Failed step:
Sanitized notes:
Smoke evidence location:
Follow-up needed: YES / NO
Owner:
```

## Пов'язані документи

- Canonical invariants: [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- `state.json` lifecycle ownership note: [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- Current runtime flow: [./runtime_flow.md](./runtime_flow.md)
- Current architecture guide: [./current_architecture.md](./current_architecture.md)
- Smoke sign-off template: [./live_smoke_signoff_template_ae_sister.md](./live_smoke_signoff_template_ae_sister.md)
