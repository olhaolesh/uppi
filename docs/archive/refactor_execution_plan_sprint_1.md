# Refactor Execution Plan — Sprint 1

> Historical / archival planning artifact.
> Sprint 1 already implemented; цей файл потрібен лише як історія rollout-плану.
> Для актуального стану системи дивіться [README.md](../../README.md),
> [Поточну архітектуру](../current_architecture.md) і
> [Protected invariants](../refactor_protected_invariants.md).

## Sprint Goal

Головна мета Sprint 1: прибрати сліпі зони перед рефакторингом і зробити проект спостережуваним, тестованим і безпечнішим без зміни runtime semantics.

Чому цей спринт перший:

- без characterization tests будь-який наступний structural refactor буде сліпим;
- без logging/redaction важко безпечно аналізувати фейли;
- без drift fixes частина support tooling дає хибну картину стану системи.

## Scope In

- Characterization tests для поточної поведінки.
- Parser / repo / DOCX / golden-path baseline tests.
- Centralized logging config design і впровадження.
- Secret redaction policy.
- Cleanup sensitive logs / prints.
- Runtime/tooling drift fixes.
- Початкові українські docstrings для protected-invariant модулів.
- Оновлення технічної документації про protected invariants і live smoke strategy.

## Scope Out

- `VisuraProcessor` decomposition.
- Split `db_repo`.
- Patch policy extraction.
- Config/DI redesign beyond low-risk normalization.
- Failure registry.
- Workspace abstraction.
- Calculation strategy abstraction.
- Будь-які зміни browser flow.

## Що НЕ можна чіпати в межах цього спринту

- `state.json` semantics не змінювати.
- AE/SISTER flow не змінювати.
- visura download flow не змінювати.
- selector order / wait sequence не змінювати без окремого high-risk stage.
- logout semantics не змінювати.
- direct SISTER entry behavior не змінювати.

## Ordered Worklist

1. Зафіксувати protected invariants у кодовій документації та docs, щоб команда не інтерпретувала roadmap як дозвіл переписувати `state.json` lifecycle.
2. Додати characterization tests для smart patch logic у `uppi/services/db_repo.py`.
3. Додати characterization tests для current input mapping і config defaults у `uppi/config/clients.py`, `uppi/domain/clients.py`, `uppi/utils/item_mapper.py`.
4. Додати parser fixtures і baseline tests для `uppi/parsers/visura_pdf_parser.py`.
5. Додати DOCX baseline tests для `uppi/services/attestazione_generator.py` і `uppi/docs/attestazione_template_filler.py`.
6. Додати хоча б один golden-path integration test для pipeline після visura download: YAML -> parser -> DB -> DOCX params -> storage naming.
7. Впровадити centralized logging config з консольним логом і file rotation 200 MB.
8. Додати redaction/filter policy для секретів і PII-bearing полів.
9. Замінити `print` і небезпечні raw debug outputs у `uppi/utils/playwright_helpers.py`, `uppi/ae/captcha.py`, `uppi/parsers/visura_pdf_parser.py`, допоміжних CLI/utility scripts.
10. Вирівняти runtime/tooling drift:
    `uppi/domain/db.py`, `uppi/cli/inspect_clients.py`, `uppi/utils/db_utils/init_db.py`.
11. Додати українські docstrings у `uppi/spiders/uppi_spider.py`, `uppi/ae/auth.py`, `uppi/ae/sister_navigation.py`, `uppi/ae/captcha.py`, `uppi/ae/download.py`.
12. Підготувати live smoke checklist для AE/SISTER, але не міняти runtime flow.

## Dependencies

- Крок 2 залежить лише від доступності current tests.
- Кроки 4–6 бажано робити після кроків 2–3, щоб baseline test naming і fixtures були послідовні.
- Крок 7 залежить від погодженого формату structured fields.
- Крок 8 залежить від кроку 7.
- Крок 9 залежить від кроків 7–8.
- Крок 10 можна виконувати паралельно, але merge робити після baseline tests.
- Крок 11 робити по мірі торкання модулів у кроках 7–9.
- Крок 12 робити після кроків 1–11, коли вже зрозумілий actual observability surface.

## Risk Assessment

### Task Group: Characterization and Baseline Tests

- Risk level: Low
- Чому: тести фіксують поточну поведінку, не міняючи її.
- Що може піти не так: можна випадково зафіксувати баг як expected behavior.
- Зменшення ризику: тести мають описувати фактичний контракт, а не внутрішню реалізацію; окремо маркувати questionable behavior як `known_current_behavior`.

### Task Group: Centralized Logging and Redaction

- Risk level: Low
- Чому: зміни переважно infrastructural.
- Що може піти не так: шум у логах, випадкове приховування потрібного поля, або навпаки витік секретів.
- Зменшення ризику: спершу затвердити redaction policy; додати unit tests на sanitizer; міняти logging calls без зміни control flow.

### Task Group: Sensitive Log Cleanup

- Risk level: Low
- Чому: не змінює бізнес-логіку.
- Що може піти не так: втрата важливого debug context.
- Зменшення ризику: замінювати raw payload на metadata-only logging, а не просто видаляти повідомлення.

### Task Group: Runtime / Tooling Drift Fixes

- Risk level: Low
- Чому: support tooling ізольований від основного browser flow.
- Що може піти не так: можна виправити drift неповністю і лишити хибне відчуття, що tooling вже надійний.
- Зменшення ризику: окремі acceptance checks для кожного utility script.

### Task Group: Initial Docstrings and Technical Docs

- Risk level: Low
- Чому: документація не впливає на runtime.
- Що може піти не так: можна створити docs, які суперечать фактичній поведінці.
- Зменшення ризику: писати docstrings тільки після перечитування і characterization coverage відповідного модуля.

## Regression / Test Gates

### До початку спринту

- Поточний baseline `venv/bin/python -m pytest -q` має бути green.

### Додати в межах спринту

- Characterization tests для smart patch logic.
- Parser fixture tests.
- Repo contract tests для critical SQL semantics.
- DOCX baseline tests.
- Golden-path baseline test.
- Unit tests на redaction policy.

### Перед merge кожної великої задачі

- `pytest -q`
- Тести на redaction/logging, якщо змінювався logging layer.
- Targeted parser/docx/repo tests для відповідного PR.

### Де потрібен live smoke

- Якщо змінюються лише tests/docs: live smoke не потрібен.
- Якщо логування інтегрується в `uppi/spiders/uppi_spider.py` або `uppi/ae/*`: потрібен хоча б один live smoke run на known-good account перед merge великого PR.

## Acceptance Criteria

- Існує задокументований `protected invariant` для browser flow і `state.json`.
- Існує characterization coverage для smart patch behavior.
- Є baseline tests для parser, DOCX і golden path.
- У проекті є centralized logging config з console logging і file rotation 200 MB.
- У логах більше немає `print`-based raw request logging і raw CAPTCHA payload logging.
- Support tooling не використовує явно застарілі schema assumptions без documented warning.
- Browser-critical modules мають базові українські docstrings.

## Deliverables

- New characterization test suite.
- Parser fixtures і DOCX baseline tests.
- Golden-path baseline test.
- Central logging configuration.
- Redaction policy documentation.
- Оновлені українські docstrings у protected modules.
- Live smoke checklist documentation.

## Rollback / Containment Notes

- Будь-які logging changes зливати малими PR.
- Якщо logging integration у browser-adjacent module викликає підозру, rollback простий: revert only logging patch without touching flow logic.
- Drift fixes у tooling тримати окремими PR від production-adjacent changes.
- Characterization tests не мають вимагати code moves.
