# UPPI: покроковий implementation plan для веб-сервісу на AWS

## 1. Призначення документа

Цей документ розбиває реалізацію веб-сервісу UPPI на невеликі етапи, з яких потім можна генерувати окремі промти для Codex.

Документ спеціально побудований так, щоб:

* не ламати поточний runtime contract UPPI;
* не змішувати browser-critical зміни з web/API змінами;
* виконувати роботу маленькими контрольованими slices;
* мати чіткі acceptance gates після кожного етапу.

---

## 2. Базові принципи виконання

Перед стартом усіх етапів зафіксувати такі правила:

1. **Не переписувати browser-critical flow AE / SISTER в межах вебізації.**
2. **Prepare-by-CF лишається owner fetch/update decision logic.**
3. **Bulk import лишається import-only boundary.**
4. **Generation path не має ходити в SISTER напряму.**
5. **Не змінювати `state.json` lifecycle без окремого технічного етапу.**
6. **Не змішувати deployment, refactor і UI-redesign в одному slice.**
7. **Кожен етап має завершуватись green tests + коротким manual smoke.**
8. **Кожен Codex prompt має виконувати тільки один логічний slice.**

---

## 3. Загальна черга етапів

### Етап 0. Freeze baseline і підготовка до вебізації

### Етап 1. Винести web-facing backend shell

### Етап 2. Реалізувати авторизацію і session model

### Етап 3. Додати API для search / prepare flow

### Етап 4. Додати API для generation flow

### Етап 5. Додати API для bulk import flow

### Етап 6. Підняти frontend skeleton з 3 екранами

### Етап 7. Інтегрувати екран "Згенерувати Attestazione"

### Етап 8. Інтегрувати екран "Додавання клієнтів в БД"

### Етап 9. Додати job status / logs / artifacts model

### Етап 10. Підготувати Docker + AWS deployment foundation

### Етап 11. Розгорнути MVP в AWS test environment

### Етап 12. Провести stabilization і handoff

---

## 4. Деталізація по етапах

# Етап 0. Freeze baseline і підготовка до вебізації

## Мета

Зафіксувати поточний безпечний стан UPPI перед початком web/API робіт.

## Що зробити

1. Зафіксувати source-of-truth docs для runtime modes.
2. Зафіксувати список browser-critical файлів.
3. Зафіксувати baseline test suite, який не можна ламати.
4. Додати короткий ADR про те, що:

   * web layer не змінює generation/import contracts;
   * deployment target для MVP — ECS/Fargate;
   * frontend і backend стартують у монорепозиторії.
5. Описати "safe change" vs "unsafe change" для Codex.

## Артефакти етапу

* `docs/web_migration_baseline.md`
* `docs/architecture_decisions/web_service_foundation.md`
* список regression suites

## Acceptance gate

* baseline docs узгоджені з current code;
* baseline tests проходять;
* є письмово зафіксовані заборони для browser flow.

## Codex slice

Один prompt: тільки документаційний freeze + baseline inventory, без runtime-коду.

---

# Етап 1. Винести web-facing backend shell

## Мета

Створити окремий backend shell для майбутнього веб-сервісу без підключення реальної бізнес-логіки.

## Що зробити

1. Додати backend app entrypoint на FastAPI.
2. Створити структуру модулів типу:

   * `uppi/web/app.py`
   * `uppi/web/api/`
   * `uppi/web/schemas/`
   * `uppi/web/services/`
3. Додати базові endpoints:

   * `/health/live`
   * `/health/ready`
4. Додати конфіг backend shell через env/dataclass layer.
5. Не інтегрувати поки що реальні prepare/generation/import use cases.

## Артефакти етапу

* базовий FastAPI app;
* health endpoints;
* базова конфігурація;
* стартовий Dockerfile для backend shell.

## Acceptance gate

* FastAPI піднімається локально;
* `/health/live` і `/health/ready` працюють;
* pytest для web shell проходить;
* жодна бізнес-операція UPPI не змінена.

## Codex slice

Один prompt: створити web shell, але не чіпати prepare/import/generation logic.

---

# Етап 2. Реалізувати авторизацію і session model

## Мета

Додати базовий доступ до веб-сервісу через `Username / Password / Pin`.

## Що зробити

1. Описати auth config contract з урахуванням того, що для MVP `Username / Password / Pin` читаються з AWS Systems Manager Parameter Store як `SecureString` параметри.
2. Додати login endpoint.
3. Додати session model:

   * secure cookie або інший простий session механізм для MVP;
   * endpoint `GET /auth/me`;
   * endpoint `POST /auth/logout`.
4. Джерело секретів зробити абстрактним:

   * локально через env;
   * у production для MVP через AWS Systems Manager Parameter Store (`SecureString`);
   * AWS Secrets Manager лишити як optional future path, якщо пізніше знадобиться автоматична ротація або складніший secret lifecycle.
5. Заборонити зберігання credentials у frontend.

## Артефакти етапу

* auth router;
* session middleware / dependency layer;
* tests на login / invalid credentials / protected route.

## Acceptance gate

* неавторизований користувач не бачить робочі маршрути;
* авторизований користувач проходить у систему;
* logout знищує session;
* credentials не потрапляють у відповіді API і в логи;
* `Username / Password / Pin` не хардкодяться в коді, `.env.example`, frontend build або Docker image.

## Codex slice

Один prompt: тільки auth/session, без UI і без prepare/generation API.

---

# Етап 3. Додати API для search / prepare flow

## Мета

Зробити web-friendly API над current prepare-by-CF flow.

## Що зробити

1. Визначити request/response schema для search.
2. Створити endpoint `POST /attestazioni/search`.
3. Не переносити prepare logic у controller.
4. Створити adapter/service, який викликає current prepare owner path.
5. Повернути результат у web shape:

   * client info;
   * objects list;
   * general info;
   * source info (`DB` / `SISTER`);
   * logs/status.
6. Підтягнути `force update visura` як явний прапорець API.

## Важливі правила

* prepare лишається owner decision logic;
* generation ще не запускається;
* не дублювати окрему fetch/update логіку в web layer.

## Артефакти етапу

* search endpoint;
* adapter до prepare mode;
* DTO mapping layer;
* tests на DB hit / DB miss / force refresh.

## Acceptance gate

* web API успішно викликає prepare flow;
* повертає дані у frontend-friendly shape;
* не ламає current CLI semantics.

## Codex slice

Один prompt: тільки search/prepare API adapter.

---

# Етап 4. Додати API для generation flow

## Мета

Зробити web-friendly endpoint для генерації Attestazione на основі вже підготовлених даних.

## Що зробити

1. Описати payload для generation endpoint.
2. Реалізувати endpoint `POST /attestazioni/generate`.
3. Реалізувати adapter до current generation-only path.
4. Забезпечити передачу:

   * вибраних immobili;
   * редагованих form fields;
   * persistable / non-persistable changes according to current policy.
5. Забезпечити повернення:

   * job id або synchronous result для MVP;
   * artifact reference / download reference.

## Важливі правила

* generation не повинен ходити в SISTER;
* web layer не змінює output naming contract без окремого рішення;
* не змінювати документний pipeline order.

## Артефакти етапу

* generation API;
* mapping із web form у generation input contract;
* integration tests на success/failure path.

## Acceptance gate

* generation запускається з web API;
* артефакт створюється;
* не зламані baseline generation tests.

## Codex slice

Один prompt: тільки generation API adapter і mapping layer.

---

# Етап 5. Додати API для bulk import flow

## Мета

Винести current bulk CSV import-only orchestration у web API.

## Що зробити

1. Додати endpoint `POST /clients/import`.
2. Підтримати 2 input modes:

   * список CF через текстове поле;
   * CSV upload.
3. Не переносити bulk orchestration в controller.
4. Використати current bulk import service як canonical path.
5. Повернути job model з counters:

   * total;
   * processed;
   * success;
   * failed.
6. Додати endpoint для failed CSV artifact.

## Артефакти етапу

* import API router;
* parser/adapters для textarea і CSV upload;
* failed CSV artifact generation;
* tests на нормалізацію, дедуплікацію, partial failures.

## Acceptance gate

* один і той самий backend path працює і для textarea, і для CSV;
* failed rows можна скачати окремо;
* current bulk import semantics не порушені.

## Codex slice

Один prompt: тільки bulk import API + failed CSV result.

---

# Етап 6. Підняти frontend skeleton з 3 екранами

## Мета

Створити frontend shell, не інтегруючи ще реальний backend.

## Що зробити

1. Створити frontend app у `frontend/` або `web/`.
2. Налаштувати routing для 3 screen states:

   * login;
   * attestazione;
   * bulk import.
3. Підключити базовий layout.
4. Винести reusable UI components.
5. Поки що використовувати mock data / mock handlers.

## Артефакти етапу

* frontend skeleton;
* routing;
* screen containers;
* mock API layer.

## Acceptance gate

* frontend запускається окремо;
* 3 екрани відкриваються;
* є навігація між ними.

## Codex slice

Один prompt: тільки frontend scaffold з маршрутами і без реальних API integration.

---

# Етап 7. Інтегрувати екран "Згенерувати Attestazione"

## Мета

Під'єднати frontend екрана Attestazione до реального search/generation API.

## Що зробити

1. Підключити login guard.
2. Підключити search form до `POST /attestazioni/search`.
3. Відобразити execution logs.
4. Відобразити counters:

   * знайдено об'єктів;
   * обрано для генерації.
5. Відобразити картки immobili.
6. Відобразити editable / readonly fields according to current contract.
7. Підключити `Generate` до generation API.
8. Показати artifact result або download action.

## Артефакти етапу

* frontend search/generation integration;
* loading / error / success states;
* form mapping logic.

## Acceptance gate

* оператор може пройти весь flow від search до generation з frontend;
* логи відображаються;
* UI не ламає current field policy.

## Codex slice

Один prompt: тільки integration екрана Attestazione з реальними API.

---

# Етап 8. Інтегрувати екран "Додавання клієнтів в БД"

## Мета

Під'єднати bulk import screen до реального import API.

## Що зробити

1. Підключити textarea mode.
2. Підключити file upload mode.
3. Підключити import start action.
4. Показати status counters.
5. Показати console logs.
6. Показати failed CSV download action.

## Артефакти етапу

* bulk import frontend integration;
* polling або refresh state handling;
* failed artifact download.

## Acceptance gate

* користувач може виконати bulk import з frontend;
* counters і logs відображаються;
* failed CSV скачується.

## Codex slice

Один prompt: тільки integration bulk import screen.

---

# Етап 9. Додати job status / logs / artifacts model

## Мета

Стандартизувати довгі backend operations як job-oriented execution model.

## Що зробити

1. Визначити canonical job model:

   * id;
   * type;
   * status;
   * progress counters;
   * started_at / finished_at;
   * logs;
   * artifacts.
2. Визначити, що в MVP job-и можуть бути sync-like або pseudo-async, але модель має бути однакова.
3. Додати status/log read endpoints.
4. Забезпечити redaction для чутливих даних у логах.

## Артефакти етапу

* job model;
* shared status DTOs;
* log redaction integration;
* reusable polling contract для frontend.

## Acceptance gate

* search/import/generation мають однорідний status model;
* секрети і CF-like sensitive data не течуть у logs.

## Codex slice

Один prompt: тільки job/status/log model і redaction-safe output.

---

# Етап 10. Підготувати Docker + AWS deployment foundation

## Мета

Підготувати deployable foundation для AWS, не роблячи ще повний production rollout.

## Що зробити

1. Додати production-grade Dockerfile для backend/runtime.
2. Перевірити Playwright dependencies у контейнері.
3. Підготувати frontend build pipeline.
4. Підготувати env/config contract для AWS.
5. Підготувати IaC skeleton або deployment manifests для:

   * ECR;
   * ECS/Fargate;
   * ALB;
   * RDS;
   * S3;
   * CloudFront;
   * SSM Parameter Store як primary config/secret store для MVP;
   * optional Secrets Manager integration point на майбутнє.
6. Підготувати мінімальний CI/CD.

## Артефакти етапу

* backend Dockerfile;
* frontend build config;
* infra directory;
* deployment README.

## Acceptance gate

* backend image збирається;
* frontend build збирається;
* є repeatable deploy foundation;
* є окремо задокументований Parameter Store contract для MVP secrets/config.

## Codex slice

Розбити на 2 окремі prompt-и:

1. backend containerization;
2. infra/deploy scaffolding.

---

# Етап 11. Розгорнути MVP в AWS test environment

## Мета

Підняти тестове оточення, де можна пройти повний операторський сценарій через браузер.

## Що зробити

1. Розгорнути test environment.
2. Підключити secrets/config.
3. Підключити RDS і S3.
4. Налаштувати frontend hosting.
5. Налаштувати API domain / TLS.
6. Виконати smoke:

   * login;
   * search;
   * visura update;
   * generation;
   * bulk import.

## Артефакти етапу

* test environment URL;
* smoke checklist;
* known issues list.

## Acceptance gate

* повний happy path проходить у test env;
* ключові артефакти зберігаються;
* логи доступні;
* критичних browser regressions немає.

## Codex slice

Окремий prompt не для коду, а для deployment / verification instructions.

---

# Етап 12. Stabilization і handoff

## Мета

Добити стабільність MVP перед подальшим розширенням.

## Що зробити

1. Пройти regression suite.
2. Пройти manual smoke для browser-critical flows.
3. Зібрати known issues / backlog.
4. Оновити документацію:

   * README entry point;
   * operator workflow;
   * deployment runbook;
   * recovery / troubleshooting notes.
5. Підготувати список phase-2 improvement items.

## Артефакти етапу

* updated docs;
* release checklist;
* backlog phase 2.

## Acceptance gate

* MVP стабільний;
* документація синхронізована;
* можна переходити до нових функцій без повернення до архітектурного хаосу.

## Codex slice

Один prompt: тільки docs stabilization і handoff docs.

---

## 5. Рекомендований порядок генерації prompt-ів для Codex

Потім із цього implementation plan рекомендовано робити prompt-и саме в такому порядку:

1. Етап 0
2. Етап 1
3. Етап 2
4. Етап 3
5. Етап 4
6. Етап 5
7. Етап 6
8. Етап 7
9. Етап 8
10. Етап 9
11. Етап 10a
12. Етап 10b
13. Етап 11
14. Етап 12

---

## 6. Що не можна змішувати в одному prompt

### Заборонені комбінації

Не змішувати в одному Codex prompt:

* browser flow refactor + frontend integration;
* AWS deploy + business logic rewrite;
* DB schema redesign + UI changes;
* logging cleanup + runtime semantics rewrite;
* prepare refactor + generation refactor + bulk refactor одночасно.

### Допустимі комбінації

Можна поєднувати тільки дуже близькі задачі, наприклад:

* router + DTO + service adapter одного endpoint;
* frontend screen + API client для цього screen;
* Dockerfile + runtime dependency docs;
* Parameter Store contract + auth config loader одного slice.

---

## 7. Мінімальний checkpoint після кожного етапу

Після кожного етапу перевіряти:

1. Що саме змінено.
2. Що навмисно не змінювалось.
3. Які тести пройшли.
4. Який manual smoke був виконаний.
5. Які відомі обмеження лишились.

---

## 8. Результат

Після виконання всіх етапів має бути отриманий веб-сервіс UPPI, який:

* має окремий екран входу;
* дозволяє генерувати Attestazione через браузер;
* дозволяє масово додавати клієнтів у БД через браузер;
* розгорнутий в AWS;
* не ламає поточний runtime contract UPPI;
* придатний до наступних фаз розвитку.
