# UPPI — покроковий план виконання

Це не загальний архітектурний опис, а практичний порядок робіт: що саме робити, у якій послідовності, який результат має бути після кожного кроку і де зупинятись для перевірки.

Normative note:

* status: цей файл тепер є historical execution-order artifact після завершення rollout
* для current operator behavior треба читати [./operator_workflow.md](./operator_workflow.md), [./runtime_flow.md](./runtime_flow.md) і [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)
* canonical source of truth для rollout-рішень зафіксовано в [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)
* ADR-контекст див. у [./adr_0001_single_client_immobili_contract.md](./adr_0001_single_client_immobili_contract.md)
* цей файл описує execution order і не є current behavioral contract

## Базові правила, які вважаємо вже зафіксованими

* `immobili.yml` = **один клієнт**
* формат `immobili.yml` = **root fields + `immobili: [...]`**
* `prepare` володіє **fetch/update logic**
* bulk CSV режим = **import-only**
* `scrapy crawl uppi` = **generation-only**
* generation mode **не ходить у SISTER**
* якщо immobile немає в БД, generation дає помилку і відсилає до `prepare`
* `CONDUTTORE_*`, `CONTRATTO_DATA`, `DECORRENZA_DATA`, `REGISTRAZIONE_*`, `AGENZIA_ENTRATE_SEDE`, `CANONE_CONTRATTUALE_MENSILE`, `DURATA_ANNI` = **run-only fields**
* `LOCATORE_*`, `IMMOBILE_*`, `ENERGY_CLASS`, `ARREDATO`, `ISTAT`, `IGNORE_SURCHARGES`, `CONTRACT_KIND`, `A/B/C/D` = **persistable fields**
* browser-critical flow не чіпаємо
* `state.json` lifecycle не змінюємо
* AE/SISTER flow не змінюємо
* visura download flow не змінюємо
* selector order / wait sequence / logout semantics не змінюємо
* blind retry для browser-critical stages не додаємо

---

## Крок 0. Заморозити правила

### Що треба зафіксувати перед кодом

* `immobili.yml` = один клієнт
* формат: root fields + `immobili: [...]`
* `CONDUTTORE_*`, `CONTRATTO_DATA`, `DECORRENZA_DATA`, `REGISTRAZIONE_*`, `AGENZIA_ENTRATE_SEDE`, `CANONE_CONTRATTUALE_MENSILE`, `DURATA_ANNI` = run-only
* `LOCATORE_*`, `IMMOBILE_*`, `ENERGY_CLASS`, `ARREDATO`, `ISTAT`, `IGNORE_SURCHARGES`, `CONTRACT_KIND`, `A/B/C/D` = persistable
* generation mode не ходить у SISTER
* якщо immobile немає в БД, generation дає помилку і каже: спочатку `prepare`

### Результат кроку

* більше не обговорюємо формат YAML і роль режимів
* це зафіксовано в [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md) як нормативний контракт для наступних змін

### Не робити

* не чіпати runtime-код

---

## Крок 1. Замінити старий input contract

### Що робити

* прибрати залежність від `clients.yml` як основного generation input
* ввести новий input contract для `immobili.yml`
* ввести окремий input для `clients.csv`
* додати новий env/config surface для `UPPI_IMMOBILI_YAML`
* старі місця, де зашитий `clients.yml`, локалізувати в одному compatibility layer або прибрати

### Які файли ймовірно зачепить

* `uppi/config/app_config.py`
* `uppi/domain/clients.py`
* `uppi/config/clients.py`
* `uppi/utils/item_mapper.py`
* validation layer для YAML

### Результат кроку

* система вміє читати новий `immobili.yml`
* система вміє читати `clients.csv`
* старий `clients.yml` більше не є центральним контрактом

### Не робити

* не міняти spider
* не міняти browser flow
* не писати prepare logic

---

## Крок 2. Зробити генератор `immobili.yml` з БД

### Що робити

* зробити read-model: один клієнт → root fields + список `immobili`
* витягувати з БД:

  * root `LOCATORE_*`
  * immobile identity/display fields
  * persistable поля
* run-only поля лишати blank
* зробити стабільний порядок `immobili` у файлі
* формат виходу: root mapping + `immobili: [...]`

### Результат кроку

* по CF, який уже є в БД, команда або сервіс може згенерувати валідний `immobili.yml`
* у файлі є всі об’єкти клієнта
* мінімум visura-derived поля для кожного об’єкта присутні
* persistable поля з БД теж підтягуються

### Не робити

* не запускати SISTER
* не робити prepare CLI повністю
* не писати назад у БД нічого

---

## Крок 3. Винести import path в окремий runner

### Що робити

* ізолювати поточний browser/import path у окрему штуку:

  * або internal import spider
  * або import runner
* він має reuse-ити:

  * login
  * navigation
  * captcha
  * download
  * parser
  * import stages
* stop boundary: після `ImmobileSync`, без generation

### Результат кроку

* є окремий import-only шлях, який:

  * ходить у SISTER
  * качає візуру
  * парсить
  * оновлює БД
  * і на цьому зупиняється

### Не робити

* не змінювати selector order
* не змінювати `state.json`
* не змінювати logout semantics
* не додавати blind retry

---

## Крок 4. Реалізувати `prepare-by-CF`

### Що робити

* додати CLI, наприклад:

  * `--cf`
  * `--force-update-visura`
* реалізувати 3 сценарії:

#### 1. DB hit + no force

* не запускати import runner
* просто згенерувати `immobili.yml`

#### 2. DB miss

* запустити import runner
* потім згенерувати `immobili.yml`

#### 3. DB hit + force

* примусово запустити import runner
* оновити БД
* потім згенерувати `immobili.yml`

### Результат кроку

* з’являється головний сервісний сценарій для оператора
* після нього оператор має готовий `immobili.yml`

### Не робити

* не обчислювати canone
* не генерувати DOCX
* не створювати `attestazione`
* не запускати generation stages

---

## Крок 5. Реалізувати bulk CSV import-only

### Що робити

* додати CLI для `clients.csv`
* нормалізувати й дедуплікувати CF
* для кожного CF запускати той самий import runner
* без генерації YAML
* без generation path

### Результат кроку

* є окремий масовий сервісний режим для наповнення БД

### Не робити

* не створювати `immobili.yml`
* не викликати `ContractSync`, `CanoneStage`, `DocumentStage`, `AuditStage`

---

## Крок 6. Переробити `scrapy crawl uppi` у generation-only

### Що робити

* прибрати з `uppi_spider.py` логіку:

  * DB-vs-SISTER decision
  * browser login
  * refresh decision
  * import fallback
* залишити generation role:

  * читає `immobili.yml`
  * перевіряє single-client shape
  * бере active records
  * робить strict match по `LOCATORE_CF + FOGLIO + NUMERO + SUB`
  * робить merge YAML-over-DB тільки для editable fields
  * write-back тільки persistable fields
  * запускає canone + document generation + audit

### Результат кроку

* `scrapy crawl uppi` стає чистою generation-командою
* якщо immobile не знайдений у БД — hard fail з підказкою “запусти prepare”

### Не робити

* не лишати прихований fallback у SISTER
* не змішувати generation із import path

---

## Крок 7. Валідація і `"-"` semantics

### Що робити

* додати validation для:

  * single-client document
  * `immobili` list
  * active record identity fields
  * forbidden clear targets
* розвести clear semantics:

#### 1. Persistable fields

* `"-"` = clear in DB

#### 2. Run-only fields

* `"-"` = clear only in current run

#### 3. Visura identity/display fields

* `"-"` = validation error

### Окремо

* додати clear support для:

  * `LOCATORE_*`
  * `IMMOBILE_*`
* не робити DB clear для:

  * `CONDUTTORE_*`
  * `CONTRATTO_DATA`
  * `DECORRENZA_DATA`
  * `REGISTRAZIONE_*`
  * `CANONE_CONTRATTUALE_MENSILE`
  * `DURATA_ANNI`

### Результат кроку

* правила стають однозначними
* оператор не може випадково зіпсувати identity fields

---

## Крок 8. Документація, тести, smoke

### Що робити

* оновити README і docs під 3 режими
* додати або оновити тести:

  * loader нового YAML
  * DB-driven generator
  * prepare сценарії A/B/C
  * bulk mode
  * strict selection
  * YAML-over-DB precedence
  * `"-"` semantics
* зробити smoke для browser/import reuse без зміни sequence

### Результат кроку

* rollout-ready стан
* зрозумілий operator workflow
* контроль регресій

---

## Рекомендована жорстка послідовність

1. Заморозити правила
2. Новий input contract (`immobili.yml` + `clients.csv`)
3. DB-driven generator `immobili.yml`
4. Окремий import runner
5. `prepare-by-CF` mode
6. Bulk CSV import-only mode
7. Generation-only `scrapy crawl uppi`
8. Validation + clear semantics + docs + tests + smoke

---

## Контрольні точки після ключових кроків

### Після кроку 2

* можна з БД отримати правильний `immobili.yml`

### Після кроку 4

* можна по одному CF або згенерувати YAML із БД, або сходити в SISTER і потім згенерувати YAML

### Після кроку 5

* можна масово наповнювати БД без generation

### Після кроку 6

* generation більше не залежить від browser/import

---

## Найризикованіше місце

Найнебезпечніше місце — не bulk і не prepare, а саме:

* переробка `scrapy crawl uppi`
* split `VisuraProcessor`

Тому до цього кроку треба доходити вже маючи готові:

* новий YAML contract
* generator
* import runner
* prepare mode

---

## Як різати на великі задачі

### Задача 1

* Крок 1
* Крок 2

### Задача 2

* Крок 3
* Крок 4
* Крок 5

### Задача 3

* Крок 6
* Крок 7
* Крок 8

Такий поділ найменш ризиковий і найкраще підходить для поетапної реалізації.
