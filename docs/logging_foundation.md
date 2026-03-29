# Logging Foundation

Цей change set додає базову централізовану конфігурацію логування без зміни
runtime semantics, browser-critical flow або lifecycle `state.json`.

## Де живе конфігурація

- central module: `uppi/logging_config.py`
- bootstrap wiring:
  - `uppi/spiders/uppi_spider.py`
  - `uppi/pipelines.py`

## Що саме робить foundation

- ініціалізує стандартний Python `logging` для логерів `uppi*`;
- додає console handler;
- додає rotating file handler;
- file rotation: `200 MB`;
- не дублює власні handlers при повторній ініціалізації.

## Дефолтний log path

- локальний default: `logs/uppi.log` у корені репозиторію;
- optional env overrides:
  - `UPPI_LOG_DIR`
  - `UPPI_LOG_FILE`
  - `UPPI_LOG_LEVEL`

На цьому етапі це не є workspace/path redesign. Це лише безпечний локальний
default для central logging baseline.

## Поточний формат

Формат навмисно лишається structured-ish, але простим:

`timestamp | level | logger | function:line | message`

Формат лишається structured-ish, але поверх foundation тепер уже працює
redaction/filter layer для найбільш ризикових даних.

## Redaction / Filter Policy

Sanitizer/filter живе в `uppi/logging_config.py` і застосовується на handler-рівні.

Базово редагуються:

- passwords / PIN / credentials;
- token / api_key / authorization / bearer-like values;
- cookie / session / storage-state-like values;
- CAPTCHA-related values;
- full codice fiscale / CF-like values;
- частина query params, якщо вони містять token/session/captcha-like дані.

Поточний стиль маскування:

- `<secret:redacted>`
- `<token:redacted>`
- `<session:redacted>`
- `<captcha:redacted>`
- `<cf:redacted>`

## Що цей PR навмисно НЕ робить

- не замінює всі `print` на `logger`;
- не чистить raw debug outputs;
- не змінює browser flow;
- не змінює `state.json` lifecycle;
- не вводить зовнішню observability infrastructure.

## Що ще лишається наступним кроком

- cleanup існуючих `print` / raw debug outputs у browser-adjacent і utility модулях;
- точкове накриття call sites, де чутливі дані зараз ідуть повз centralized logging path;
- за потреби розширення redaction policy під більш спеціалізовані payload shapes.

## Перший cleanup pass

Після foundation + redaction окремим вузьким change set уже прибрані найбільш
ризикові raw debug outputs у:

- `uppi/utils/playwright_helpers.py`
- `uppi/ae/captcha.py`
- `uppi/parsers/visura_pdf_parser.py`

На цьому кроці semantics не змінювались:

- browser flow;
- CAPTCHA submit ordering;
- parser extraction path;
- `state.json` lifecycle.
