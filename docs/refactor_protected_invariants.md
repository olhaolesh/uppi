# Refactor Protected Invariants

Це canonical-документ для protected invariants у browser-critical зоні.
Якщо інші docs скорочують або переказують цей контракт, пріоритет має саме цей
файл. Детальний live smoke checklist див. у `docs/live_smoke_strategy_ae_sister.md`.

## Browser-Critical Invariants

- Не змінювати порядок Playwright steps.
- Не змінювати selector order.
- Не змінювати wait/click/fill sequence.
- Не змінювати current logout path.
- Не змінювати direct SISTER navigation behavior.
- Не змінювати error handling так, щоб замість контрольованого завершення сесії просто закривався браузер.

## AE / SISTER Invariants

- AE authentication flow у `uppi/ae/auth.py` є protected.
- SISTER opening flow у `uppi/ae/sister_navigation.py::open_sister_service` є protected.
- Navigation to visura flow у `uppi/ae/sister_navigation.py::navigate_to_visure_catastali` є protected.
- CAPTCHA handling у `uppi/ae/captcha.py` є protected.
- Visura download flow у `uppi/ae/download.py` є protected.
- Logout helper у `uppi/spiders/uppi_spider.py::_logout_in_context` є protected.

## `state.json` Lifecycle Invariant

Protected invariant:

- на кожну нову сесію потрібен новий `state.json`;
- `state.json` зберігається під час AE / SISTER flow;
- цей `state.json` використовується для direct SISTER transition;
- після logout цей state вже невалідний;
- після завершення сесії invalid `state.json` має бути видалений;
- reuse старого `state.json` між новими сесіями заборонений;
- семантичний контракт:
  `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`;
- якщо сесія завершується некоректно, повторний login може бути заблокований приблизно на 20 хвилин.

Практичний наслідок:

- не трактувати розмазаний по модулях lifecycle як дозвіл змінювати semantics;
- дозволені тільки documentation, characterization, safe hardening, better logging, better isolation.
- будь-яка workspace/path abstraction не може змінювати точки create/load/delete для `state.json`;
- будь-яка інкапсуляція `state.json` має зберігати той самий контракт:
  `fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`.

## Logout / Cleanup Invariant

- Logout є частиною бізнес-критичного session lifecycle, а не optional cleanup.
- Заборонено замінювати logout на “просто close browser”.
- Cleanup локальних артефактів не може змінювати browser/session semantics.
- Якщо cleanup торкається `state.json`, він автоматично переходить у категорію `high scrutiny`.

## High-Scrutiny Areas

- `state.json` lifecycle і будь-які точки create/load/delete.
- Logout timing, cleanup timing і close behavior.
- Direct SISTER transition.
- Visura download sequence.
- Selector order.
- Wait/click/fill sequence.

## Do Not Touch Without Live Regression

- `uppi/settings.py`
- `uppi/spiders/uppi_spider.py`
- `uppi/ae/auth.py`
- `uppi/ae/sister_navigation.py`
- `uppi/ae/captcha.py`
- `uppi/ae/download.py`
- будь-який код, який визначає коли створюється, підхоплюється або очищується `state.json`
- будь-який код, який міняє logout timing або close behavior
- будь-який код, який міняє visura download sequence

## Allowed Safe Work Around Protected Areas

- Додати українські docstrings.
- Додати characterization tests.
- Додати safer logging with redaction.
- Додати better lifecycle documentation.
- Інкапсулювати lifecycle у wrapper/API без зміни порядку дій і semantics.
- Додати live smoke checklist.

## Explicitly Forbidden Without Separate High-Risk Phase

- Reuse persistent `state.json`.
- Перенесення delete/create semantics `state.json`.
- Зміна direct SISTER contract.
- Зміна selector order.
- Зміна wait timings як “optimization”.
- Зміна CAPTCHA submit flow.
- Зміна visura download trigger flow.
- Заміна explicit logout на passive browser close.
