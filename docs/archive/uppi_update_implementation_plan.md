# UPPI Update Implementation Plan

> Archived.
> Historical reference only.
> Non-normative.
> Do not use as the current behavioral source of truth.

Status note:

- цей файл є historical implementation plan після завершення rollout;
- для current behavior треба читати
  [./immobili_rollout_source_of_truth.md](../immobili_rollout_source_of_truth.md),
  [./operator_workflow.md](../operator_workflow.md),
  [./runtime_flow.md](../runtime_flow.md) і
  [./validation_clear_policy_matrix.md](../validation_clear_policy_matrix.md);
- canonical source of truth для rollout-рішень зафіксовано в
  [./immobili_rollout_source_of_truth.md](../immobili_rollout_source_of_truth.md);
- ADR-контекст див. у
  [./adr_0001_single_client_immobili_contract.md](../adr_0001_single_client_immobili_contract.md);
- цей файл лишається planning artifact і не є current behavioral reference.

Цей документ є фінальним implementation plan для оновлення UPPI під нову
операційну модель:

- один `immobili.yml` = один клієнт;
- prepare mode володіє fetch/update logic;
- bulk CSV mode є import-only;
- `scrapy crawl uppi` є generation-focused mode.

Документ не пропонує runtime-патчів. Він фіксує цільову архітектуру, stop
boundaries, класифікацію полів і phased order реалізації.

## 1. Executive summary

UPPI зараз працює як один end-to-end flow: spider сам читає `clients.yml`, сам
вирішує, чи йти в SISTER, сам оновлює cache в БД, а потім одразу переходить до
обрахунку і генерації `attestazione`.

Ця модель більше не підходить для цільового operator workflow. Після оновлення
система має бути розділена на три окремі режими:

1. `prepare-by-codice-fiscale`
2. `bulk-import-by-clients-csv`
3. `scrapy crawl uppi` як generation-focused mode

Ключова зміна в тому, що рішення про fetch/update більше не належить основному
production command. Воно централізується в prepare mode. Generation mode
працює тільки з already prepared single-client `immobili.yml` і не торкається
browser/import logic.

Додатково фіксується новий input contract:

- `immobili.yml` є single-client document;
- root document містить client-level fields;
- `immobili:` містить усі об’єкти нерухомості клієнта;
- якщо запис активний, generation flow намагається сформувати `attestazione`;
- якщо потрібного immobile нема в БД, generation hard-fail-ить запис і вказує
  на необхідність спочатку запустити prepare.

## 2. Що було неправильним у попередніх припущеннях

Нижче зафіксовано рішення, які більше не допускають альтернативного
трактування.

- Multi-CF `immobili.yml` відкинуто. Один YAML-файл описує тільки одного
  `LOCATORE_CF`.
- Flat-list shape відкинуто. Остаточний формат є root mapping + `immobili:`.
- `scrapy crawl uppi` більше не є smart fetch/import runner.
- Fetch/update logic централізовано в prepare mode.
- Bulk CSV mode лишається окремим import-only сервісним режимом.
- Generation mode не повинен сам вирішувати, чи йти в SISTER, чи оновлювати
  visura, чи запускати browser flow.
- Single-client YAML shape вважається остаточним рішенням і зафіксований у
  [./immobili_rollout_source_of_truth.md](../immobili_rollout_source_of_truth.md).

## 3. Поточний стан проєкту

### 3.1 Поточний spider і runtime entry point

Основний entry point зараз один:

- [../scrapy.cfg](../../scrapy.cfg)
- [../uppi/settings.py](../../uppi/settings.py)
- [../uppi/spiders/uppi_spider.py](../../uppi/spiders/uppi_spider.py)

Поточний [../uppi/spiders/uppi_spider.py](../../uppi/spiders/uppi_spider.py):

- чистить `state.json` і `captcha_images/` у `start()`;
- читає `clients.yml` через `load_clients()`;
- для кожного client record вирішує, чи потрібен fetch із SISTER;
- якщо fetch потрібен, запускає browser-critical path;
- після цього yield-ить `UppiItem` у pipeline;
- у тому ж процесі generation flow доходить до `attestazione`.

Тобто поточний spider поєднує 3 відповідальності:

- input loading;
- import decision + browser/import execution;
- запуск generation stages.

Саме це треба розділити.

### 3.2 Поточна YAML loader surface

Поточний input contract розмазаний по кількох модулях:

- [../uppi/domain/clients.py](../../uppi/domain/clients.py)
- [../uppi/config/app_config.py](../../uppi/config/app_config.py)
- [../uppi/config/clients.py](../../uppi/config/clients.py)
- [../uppi/utils/item_mapper.py](../../uppi/utils/item_mapper.py)
- [../uppi/services/validation/yaml_validation.py](../../uppi/services/validation/yaml_validation.py)

Поточна модель `clients.yml` є flat list і змішує:

- client-level query data;
- immobile identity fields;
- persistable editable fields;
- operator-entered run fields;
- сервісний flag `FORCE_UPDATE_VISURA`.

Це одна з головних причин, чому новий single-client document contract треба
винести в окрему explicit model.

### 3.3 Поточний processor / stages layer

Non-browser orchestration зараз іде через:

- [../uppi/pipelines.py](../../uppi/pipelines.py)
- [../uppi/services/visura_processor.py](../../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../../uppi/services/visura_stages.py)

Поточний `VisuraProcessor` координує все одним ланцюгом:

1. `PersonSyncService`
2. `VisuraIngestService`
3. `ImmobileSyncService`
4. `db_load_immobili()` + YAML-driven selection
5. `ContractSyncService`
6. `CanoneStageService`
7. `DocumentStageService`
8. `AuditStageService`

Це означає, що зараз немає окремих import-only і generation-only boundaries.

### 3.4 Поточний repository / policy layer

Repository surface:

- [../uppi/services/repositories/address_repo.py](../../uppi/services/repositories/address_repo.py)
- [../uppi/services/repositories/person_repo.py](../../uppi/services/repositories/person_repo.py)
- [../uppi/services/repositories/visura_repo.py](../../uppi/services/repositories/visura_repo.py)
- [../uppi/services/repositories/immobile_repo.py](../../uppi/services/repositories/immobile_repo.py)
- [../uppi/services/repositories/contract_repo.py](../../uppi/services/repositories/contract_repo.py)
- [../uppi/services/repositories/audit_repo.py](../../uppi/services/repositories/audit_repo.py)
- facade: [../uppi/services/db_repo.py](../../uppi/services/db_repo.py)

Patch/business semantics:

- [../uppi/services/policies/patch_policy.py](../../uppi/services/policies/patch_policy.py)
- [../uppi/services/policies/contract_patch_policy.py](../../uppi/services/policies/contract_patch_policy.py)
- [../uppi/services/policies/immobile_patch_policy.py](../../uppi/services/policies/immobile_patch_policy.py)

Поточна проблема цього шару не в SQL як такому, а в тому, що:

- persistable fields і run-only fields ще не розділені достатньо чітко;
- current contract patching уже зберігає частину run data в `contracts`;
- current joined contract context із
  [../uppi/services/repositories/contract_repo.py](../../uppi/services/repositories/contract_repo.py)
  досі підтягує частину цих значень назад у generation path;
- prepare generator не може без додаткових правил просто брати latest contract
  row як canonical source для нового `immobili.yml`.

### 3.5 Поточні browser-critical модулі

Protected browser path живе тут:

- [../uppi/ae/auth.py](../../uppi/ae/auth.py)
- [../uppi/ae/sister_navigation.py](../../uppi/ae/sister_navigation.py)
- [../uppi/ae/download.py](../../uppi/ae/download.py)
- [../uppi/ae/captcha.py](../../uppi/ae/captcha.py)
- [../uppi/config/workspace.py](../../uppi/config/workspace.py)
- [../uppi/settings.py](../../uppi/settings.py)

Canonical constraints already documented in:

- [./refactor_protected_invariants.md](../refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](../state_json_lifecycle_contract.md)
- [./runtime_flow.md](../runtime_flow.md)

Non-negotiable invariants:

- не змінювати AE/SISTER flow;
- не змінювати `state.json` lifecycle semantics;
- не змінювати visura download flow;
- не змінювати selector order / wait sequence / logout semantics;
- не вводити blind retry для browser-critical stages.

Новий план будується навколо orchestration split, а не навколо переписування цих
модулів.

### 3.6 Поточна DB model

Поточна схема лежить у:

- [../uppi/utils/db_utils/uppi_schema.sql](../../uppi/utils/db_utils/uppi_schema.sql)

Ключові таблиці:

- `persons`
- `addresses`
- `visure`
- `immobili`
- `immobile_elements`
- `contracts`
- `canone_calcoli`
- `attestazioni`

Важливі спостереження:

- `immobili` already відділяє `visura_address_id` від `real_address_id`;
- `immobile_elements` already є окремою persistable surface для A/B/C/D;
- `contracts` today змішує persistable business flags і run-scoped contract data;
- `attestazioni.full_data_snapshot` already дає snapshot surface;
- `canone_calcoli.inputs` already дає snapshot surface для calculation inputs;
- попри коментар у схемі про `contracts` як “джерело істини”, для нового rollout
  це треба трактувати обережніше: run-only поля не мають знову ставати
  canonical prepare defaults;
- schema достатня для першого rollout без обов’язкової міграції, якщо розділення
  master vs run state буде зроблене на orchestration/policy level.

### 3.7 Поточні clear semantics / patch policies

Already supported `"-"` semantics:

- `ENERGY_CLASS` через
  [../uppi/services/policies/immobile_patch_policy.py](../../uppi/services/policies/immobile_patch_policy.py)
- A/B/C/D через
  [../uppi/services/policies/immobile_patch_policy.py](../../uppi/services/policies/immobile_patch_policy.py)
- `ARREDATO`, `ISTAT`, `DURATA_ANNI`, `IGNORE_SURCHARGES` через
  [../uppi/services/policies/contract_patch_policy.py](../../uppi/services/policies/contract_patch_policy.py)

Current characterization coverage:

- [../tests/test_db_repo_patch_characterization.py](../../tests/test_db_repo_patch_characterization.py)

Поточні gaps:

- plain-text run fields не мають коректної clear semantics;
- `CONDUTTORE_*` зараз не відділені від persistable surfaces;
- current `db_upsert_contract()` still treats частину run fields як long-lived DB
  state;
- current generation path не розрізняє “clear current run value” і “clear
  persistable DB value”.
- водночас current
  [../uppi/services/attestazione_generator.py](../../uppi/services/attestazione_generator.py)
  already бере `CONTRATTO_DATA`, `DECORRENZA_DATA`, `REGISTRAZIONE_*` і
  `CONDUTTORE_*` напряму з adapter/YAML, а не з joined DB context; це хороший
  базовий сигнал для нового поділу на run-only vs persistable surfaces.

## 4. Нова цільова архітектура

### 4.1 Prepare-by-codice-fiscale

Окремий CLI режим приймає:

- обов’язковий `codice fiscale`
- optional `force_update_visura`

Він є єдиним owner-ом рішень:

- чи є sufficient data в БД;
- чи потрібен import path;
- чи потрібен forced visura refresh;
- коли формувати `immobili.yml`.

Recommended implementation shape:

- окремий CLI в `uppi/cli/`;
- reuse current browser/import path через dedicated import runner;
- import runner рекомендовано реалізувати як внутрішній import spider
  (`uppi_import`) або еквівалентний wrapper над чинним protected flow, щоб не
  дублювати Playwright/Scrapy integration.

### 4.2 Bulk-import-by-clients-csv

Окремий CLI режим приймає `clients.csv` і для кожного CF:

- запускає import path;
- оновлює БД;
- не формує `immobili.yml`;
- не йде в generation stages.

Це сервісний режим для масового кешування локальної БД.

### 4.3 Generation-focused `scrapy crawl uppi`

Після оновлення основний production command:

- читає already prepared single-client `immobili.yml`;
- працює тільки з активними immobili;
- робить strict DB match для кожного record;
- застосовує YAML-over-DB precedence тільки для editable fields;
- оновлює назад у БД тільки persistable fields;
- виконує обрахунок і generation.

Він не виконує:

- visura fetch decision;
- browser login;
- SISTER import;
- visura refresh decision.

## 5. Data flow по кожному режиму

### 5.1 Prepare-by-CF

**Input**

- CLI args: `--cf`, optional `--force-update-visura`

**Decision points**

- Чи існує client data в БД
- Чи треба forced visura refresh
- Чи запускати import runner

**DB reads**

- visura state через
  [../uppi/services/repositories/visura_repo.py](../../uppi/services/repositories/visura_repo.py)
- current immobile set через
  [../uppi/services/repositories/immobile_repo.py](../../uppi/services/repositories/immobile_repo.py)
- persistable client/immobile fields через `persons`, `addresses`,
  `immobile_elements`, selective contract reads

**DB writes**

- тільки якщо йде import path:
  - `persons`
  - `visure`
  - `immobili`
  - visura-derived addresses
  - `immobile_elements` only якщо import path вже так робить зараз

**File writes**

- один single-client `immobili.yml`

**Stop boundary**

- після побудови і запису `immobili.yml`

**Що точно НЕ відбувається**

- немає canone calculation
- немає DOCX generation
- немає `attestazione`
- немає generation audit path

**Випадок A: БД already populated, `force_update_visura=false`**

- import runner не запускається
- YAML генерується з БД

**Випадок B: БД порожня**

- запускається import runner
- після commit генерується YAML

**Випадок C: БД populated, `force_update_visura=true`**

- запускається forced import runner
- БД оновлюється свіжими visura data
- після commit генерується новий YAML

### 5.2 Bulk-import-by-clients-csv

**Input**

- `clients.csv`

**Decision points**

- нормалізація та дедуплікація CF
- per-CF запуск import runner

**DB reads**

- minimal visura/client state only якщо потрібен log/reporting context

**DB writes**

- `persons`
- `visure`
- `immobili`
- visura-derived addresses
- пов’язані import artifacts in DB

**File writes**

- стандартні runtime artifacts browser/import path
- без `immobili.yml`

**Stop boundary**

- після import commit per CF

**Що точно НЕ відбувається**

- немає YAML generation
- немає `ContractSync`
- немає `CanoneStage`
- немає `DocumentStage`
- немає `AuditStage`

### 5.3 Generation-focused `scrapy crawl uppi`

**Input**

- prepared single-client `immobili.yml`

**Decision points**

- single-client validation
- active/enabled filtering
- strict DB match per record by `LOCATORE_CF + FOGLIO + NUMERO + SUB`
- YAML-over-DB merge only для editable surfaces

**DB reads**

- client-level master data
- `immobili` master data
- current persistable fields
- latest allowed DB defaults for persistable business fields
- existing calculation / generation context where needed

**DB writes**

- тільки persistable fields:
  - root-level locatore address-like overrides
  - per-immobile override address fields
  - `ENERGY_CLASS`
  - `ARREDATO`
  - `ISTAT`
  - `IGNORE_SURCHARGES`
  - `CONTRACT_KIND`
  - `A/B/C/D`
  - optional stable field(s), див. припущення нижче
- calculation snapshot у `canone_calcoli.inputs`
- generation audit snapshot у `attestazioni.full_data_snapshot`

**File writes**

- generated DOCX files

**Stop boundary**

- після generation для всіх active immobili в документі

**Що точно НЕ відбувається**

- немає SISTER fetch decision
- немає browser flow
- немає visura refresh logic
- немає automatic import fallback, якщо immobile відсутній у БД

**Hard-fail policy**

- якщо strict DB match не знаходить immobile, record завершується помилкою з
  чітким повідомленням: спочатку потрібно запустити prepare mode

## 6. Остаточний контракт `immobili.yml`

### 6.1 Чому root mapping + `immobili: [...]`

Single-client document кращий за flat list, бо:

- напряму кодує правило “один файл = один клієнт”;
- прибирає повторення root-level data (`LOCATORE_CF`, `COMUNE`,
  `TIPO_CATASTO`, `UFFICIO_PROVINCIALE_LABEL`);
- спрощує validation;
- робить операторський workflow очевидним;
- зменшує ризик випадково змішати кількох клієнтів в одному generation run.

### 6.2 Рекомендована conceptual shape

```yaml
LOCATORE_CF: RSSMRA80A01H501Z
COMUNE: PESCARA
TIPO_CATASTO: F
UFFICIO_PROVINCIALE_LABEL: PESCARA Territorio

LOCATORE_COMUNE_RES: ""
LOCATORE_VIA: ""
LOCATORE_CIVICO: ""

immobili:
  - enabled: true
    FOGLIO: "12"
    NUMERO: "345"
    SUB: "7"
    RENDITA: "€ 123.45"
    SUPERFICIE_TOTALE: 98.7
    CATEGORIA: "A/2"
    VISURA_COMUNE: "PESCARA"
    VISURA_VIA: "VIA ROMA"
    VISURA_CIVICO: "10"

    IMMOBILE_COMUNE: ""
    IMMOBILE_VIA: ""
    IMMOBILE_CIVICO: ""
    IMMOBILE_PIANO: ""
    IMMOBILE_INTERNO: ""

    ENERGY_CLASS: ""
    ARREDATO: ""
    ISTAT: ""
    IGNORE_SURCHARGES: ""
    CONTRACT_KIND: ""
    DURATA_ANNI: ""

    A1: ""
    A2: ""
    B1: ""
    ...
    D13: ""

    CONTRATTO_DATA: ""
    CONDUTTORE_NOME: ""
    CONDUTTORE_CF: ""
    CONDUTTORE_COMUNE: ""
    CONDUTTORE_VIA: ""
    DECORRENZA_DATA: ""
    REGISTRAZIONE_DATA: ""
    REGISTRAZIONE_NUM: ""
    AGENZIA_ENTRATE_SEDE: ""
    CANONE_CONTRATTUALE_MENSILE: ""
```

### 6.3 Root-level fields

Root-level у single-client document мають жити:

- `LOCATORE_CF`
- `COMUNE`
- `TIPO_CATASTO`
- `UFFICIO_PROVINCIALE_LABEL`
- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`

Пояснення:

- `LOCATORE_*` є client-level data і не повинні дублюватися в кожному immobile
  record;
- `COMUNE / TIPO_CATASTO / UFFICIO_PROVINCIALE_LABEL` належать prepare/import
  metadata surface, не per-immobile surface.

### 6.4 Immobile-level fields

Для кожного запису в `immobili:` мають бути:

- `enabled`
- strict identity fields
- visura-derived display fields
- persistable editable fields
- operator-entered run fields

### 6.5 Active/enabled semantics

Остаточне runtime правило:

- якщо record є в `immobili:` і `enabled != false`, generation його обробляє;
- якщо оператор нічого не вимкнув, generation проходить по всіх immobili;
- якщо оператор видаляє запис, generation його не бачить;
- якщо оператор ставить `enabled: false`, generation його пропускає.

### 6.6 Identity fields

Canonical strict match fields:

- `FOGLIO`
- `NUMERO`
- `SUB`

Generation path має використовувати саме їх для пошуку в БД разом із root-level
`LOCATORE_CF`.

Поточна БД already гарантує uniqueness на рівні:

- `owner_cf, foglio, numero, sub`

див. [../uppi/utils/db_utils/uppi_schema.sql](../../uppi/utils/db_utils/uppi_schema.sql)

### 6.7 Visura-derived fields

У generated `immobili.yml` як мінімум мають бути присутні:

- `FOGLIO`
- `NUMERO`
- `SUB`
- `RENDITA`
- `SUPERFICIE_TOTALE`
- `CATEGORIA`
- visura address/display fields для зручного вибору оператором

Ці поля:

- потрібні для ідентифікації й operator readability;
- не є основною editable override surface;
- для фінального generation canonical source після match лишається БД.

### 6.8 DB-derived persistable fields

У generated `immobili.yml` мають підставлятись із БД:

- root-level `LOCATORE_*`
- per-immobile `IMMOBILE_*` override fields
- `ENERGY_CLASS`
- `ARREDATO`
- `ISTAT`
- `IGNORE_SURCHARGES`
- `CONTRACT_KIND`
- `A/B/C/D`
- optional stable field(s), якщо вони явно підтверджені бізнесом

### 6.9 Operator-entered run fields

Run fields:

- `CONDUTTORE_*`
- `CONTRATTO_DATA`
- `DECORRENZA_DATA`
- `REGISTRAZIONE_*`
- `AGENZIA_ENTRATE_SEDE`
- `CANONE_CONTRATTUALE_MENSILE`

Для цього плану також приймається робоче припущення:

- `DURATA_ANNI` трактуємо як run field, а не persistable master field, якщо
  окремо не буде погоджено інше бізнес-рішення

Правило:

- вони можуть бути blank у generated YAML;
- оператор заповнює їх перед generation;
- вони можуть входити в calculation / document snapshot;
- вони можуть потрапляти тільки в snapshot/audit surface конкретного запуску,
  але не в future prepare defaults;
- вони не повинні ставати canonical source для наступного prepare.

### 6.10 Precedence rules

Precedence застосовується тільки до editable fields.

- Якщо YAML містить значення editable поля, generation використовує саме його.
- Якщо YAML поле blank, generation бере DB value там, де це дозволено policy.
- Якщо YAML поле дорівнює `"-"` і поле є clear-allowed, застосовується explicit
  clear semantics.
- Якщо поле належить до visura identity/display group, DB лишається canonical
  source після match, а YAML не стає write-back surface.

### 6.11 `"-"` semantics

Потрібно чітко розділити три випадки.

1. Persistable clear-allowed fields
- `"-"` означає write-back clear у БД.

2. Run-only fields
- `"-"` означає clear only for current generation context;
- write-back у БД не відбувається.

3. Visura identity/display fields
- `"-"` є validation error.

## 7. Field classification matrix

| Група полів | Scope | Приклади | Джерело в generated YAML | Canonical source під час generation | Write-back у БД | `"-"` |
| --- | --- | --- | --- | --- | --- | --- |
| Client import metadata | root | `LOCATORE_CF`, `COMUNE`, `TIPO_CATASTO`, `UFFICIO_PROVINCIALE_LABEL` | prepare generator / CLI input | `LOCATORE_CF` для selection; інше mostly metadata | ні для generation | заборонено, validation error |
| Root persistable client fields | root | `LOCATORE_COMUNE_RES`, `LOCATORE_VIA`, `LOCATORE_CIVICO` | БД, якщо є | YAML > DB | так | дозволено, clear in DB |
| Immobile selection keys | immobile | `FOGLIO`, `NUMERO`, `SUB` | visura / БД | YAML значення використовуються тільки для strict match | ні | заборонено, validation error |
| Visura display fields | immobile | `RENDITA`, `SUPERFICIE_TOTALE`, `CATEGORIA`, `VISURA_*` | visura / БД | БД після match | ні | заборонено, validation error |
| Immobile persistable override fields | immobile | `IMMOBILE_COMUNE`, `IMMOBILE_VIA`, `IMMOBILE_CIVICO`, `IMMOBILE_PIANO`, `IMMOBILE_INTERNO` | БД, якщо є | YAML > DB | так | дозволено, clear in DB |
| Persistable technical/business fields | immobile | `ENERGY_CLASS`, `ARREDATO`, `ISTAT`, `IGNORE_SURCHARGES`, `CONTRACT_KIND`, `A/B/C/D` | БД, якщо є | YAML > DB | так | дозволено, clear in DB |
| Run-only contract fields | immobile | `CONDUTTORE_*`, `CONTRATTO_DATA`, `DECORRENZA_DATA`, `REGISTRAZIONE_*`, `AGENZIA_ENTRATE_SEDE`, `CANONE_CONTRATTUALE_MENSILE`, `DURATA_ANNI` | blank or operator-entered | YAML / in-memory run context | ні як master state | дозволено тільки як in-memory clear |
| Generated/audit-only fields | runtime | `run_id`, `canone_result`, DOCX path, storage object, `full_data_snapshot` | не зберігаються в YAML | runtime only | snapshot only | не застосовується |

### 7.1 Clear-allowed persistable fields

Already supported or directly aligned with current policy surface:

- `ENERGY_CLASS`
- `ARREDATO`
- `ISTAT`
- `IGNORE_SURCHARGES`
- `A/B/C/D`

To be added explicitly:

- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`
- `IMMOBILE_COMUNE`
- `IMMOBILE_VIA`
- `IMMOBILE_CIVICO`
- `IMMOBILE_PIANO`
- `IMMOBILE_INTERNO`

Recommended explicit rule:

- persistable clear semantics мають лишатися `write-back clear in DB`, включно з
  `CONTRACT_KIND`;
- якщо current schema вимагає non-null representation, це треба вирішувати на
  implementation level без зміни semantic contract цього документа.

### 7.2 Run-only fields and clear

Для run-only fields `"-"` не має ставати DB clear.

Recommended rule:

- normalize `"-"` до “порожнє значення для поточного запуску”;
- не писати його назад у master tables;
- якщо поле не допускає порожнього значення для конкретного generation case,
  generation validation кидає domain error до старту document stage.

### 7.3 Assumptions fixed by this plan

Щоб уникнути повторного переузгодження на етапі implementation, цей документ
фіксує такі робочі припущення:

- `LOCATORE_*` живуть на root-level, а не дублюються в кожному immobile record;
- `DURATA_ANNI` для цього change set трактуємо як run-only field;
- `CONTRACT_KIND` лишається persistable field;
- latest `contracts` row не є canonical source для prepare-generated run fields;
- prepare generator читає з `contracts` тільки allowlisted persistable fields.

## 8. Зміни по шарах системи

### 8.1 Config / input source

**Що змінюється**

- `clients.yml`-centric config surface замінюється на `immobili.yml` + `clients.csv`
- `UPPI_CLIENTS_YAML` має бути замінено або закрито compat layer-ом на
  `UPPI_IMMOBILI_YAML`

**Ймовірно зачіпаються**

- [../uppi/config/app_config.py](../../uppi/config/app_config.py)
- [../uppi/domain/clients.py](../../uppi/domain/clients.py)
- [../uppi/config/clients.py](../../uppi/config/clients.py)
- [../tests/test_config_di_foundation.py](../../tests/test_config_di_foundation.py)
- [../tests/test_clients_mapping_characterization.py](../../tests/test_clients_mapping_characterization.py)

**Ризик**

- low to medium

**Protected invariants**

- не зачіпає browser-critical sequence

### 8.2 YAML loader / serializer

**Що змінюється**

- flat client loader замінюється на single-client document loader
- додається DB-driven serializer для `immobili.yml`
- додається explicit field classification / validation rules

**Ймовірно зачіпаються**

- new module(s) під `uppi/config/` або `uppi/domain/`
- [../uppi/services/validation/yaml_validation.py](../../uppi/services/validation/yaml_validation.py)
- [../uppi/utils/item_mapper.py](../../uppi/utils/item_mapper.py)

**Ризик**

- medium

**Protected invariants**

- не зачіпає browser-critical sequence

### 8.3 CLI / entry points

**Що змінюється**

- додається prepare CLI
- додається bulk-import CLI
- оновлюється support tooling, яке зараз implicitly читає `clients.yml`

**Ймовірно зачіпаються**

- new `uppi/cli/prepare_immobili.py`
- new `uppi/cli/bulk_import_visure.py`
- [../uppi/cli/inspect_clients.py](../../uppi/cli/inspect_clients.py)

**Ризик**

- medium

**Protected invariants**

- prepare/bulk можуть викликати protected import runner, але самі invariants не
  повинні змінювати

### 8.4 Spider orchestration

**Що змінюється**

- current `uppi` spider стає generation-focused
- recommended internal `uppi_import` spider або еквівалентний import runner
  reuse-ить existing protected browser flow

**Ймовірно зачіпаються**

- [../uppi/spiders/uppi_spider.py](../../uppi/spiders/uppi_spider.py)
- new `../uppi/spiders/uppi_import_spider.py`
- [../uppi/settings.py](../../uppi/settings.py) only if needed for safe registration

**Ризик**

- high

**Protected invariants**

- так, це шар підвищеної уваги
- зміни допустимі тільки як orchestration split, не як browser sequence rewrite

### 8.5 Import-only orchestration

**Що змінюється**

- import chain виділяється в окремий processor/orchestrator
- stop boundary проходить після `ImmobileSync`

**Ймовірно зачіпаються**

- [../uppi/services/visura_processor.py](../../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../../uppi/services/visura_stages.py)
- [../uppi/pipelines.py](../../uppi/pipelines.py)

**Ризик**

- medium

**Protected invariants**

- browser invariants не мають змінюватися

### 8.6 Generation-only orchestration

**Що змінюється**

- generation chain виділяється окремо
- current `VisuraProcessor` більше не є єдиним monolith path
- strict DB match замінює current permissive YAML filtering
- run-only fields відділяються від persistable write-back

**Ймовірно зачіпаються**

- [../uppi/services/visura_processor.py](../../uppi/services/visura_processor.py)
- [../uppi/services/visura_stages.py](../../uppi/services/visura_stages.py)
- [../uppi/services/attestazione_generator.py](../../uppi/services/attestazione_generator.py)

**Ризик**

- medium to high

**Protected invariants**

- не повинен торкатися browser-critical sequence

### 8.7 Repositories / DB reads

**Що змінюється**

- додаються prepare-oriented DB reads для single-client document generation
- current contract/context reads мають бути allowlisted для persistable fields
- current generation writes мають перестати трактувати run-only fields як master
  defaults

**Ймовірно зачіпаються**

- [../uppi/services/repositories/immobile_repo.py](../../uppi/services/repositories/immobile_repo.py)
- [../uppi/services/repositories/contract_repo.py](../../uppi/services/repositories/contract_repo.py)
- [../uppi/services/repositories/person_repo.py](../../uppi/services/repositories/person_repo.py)
- [../uppi/services/repositories/address_repo.py](../../uppi/services/repositories/address_repo.py)

**Ризик**

- medium

**Protected invariants**

- не зачіпає browser-critical sequence

### 8.8 Patch policies / validation

**Що змінюється**

- current patch policy split-иться на:
  - persistable write-back policy
  - run-only generation merge policy
- `"-"` semantics стає explicit per field class
- single-client document validation стає mandatory

**Ймовірно зачіпаються**

- [../uppi/services/policies/contract_patch_policy.py](../../uppi/services/policies/contract_patch_policy.py)
- [../uppi/services/policies/immobile_patch_policy.py](../../uppi/services/policies/immobile_patch_policy.py)
- [../uppi/services/validation/yaml_validation.py](../../uppi/services/validation/yaml_validation.py)

**Ризик**

- medium

**Protected invariants**

- не зачіпає browser-critical sequence

### 8.9 Docs / smoke / rollout

**Що змінюється**

- docs мають бути оновлені під нову mode split model
- smoke strategy має окремо фіксувати, що browser path reuse-иться без sequence
  changes

**Ймовірно зачіпаються**

- [../README.md](../../README.md)
- [./runtime_flow.md](../runtime_flow.md)
- [./current_architecture.md](../current_architecture.md)
- [./local_development_and_testing.md](../local_development_and_testing.md)
- [./live_smoke_strategy_ae_sister.md](../live_smoke_strategy_ae_sister.md)

**Ризик**

- low

**Protected invariants**

- documentation only

## 9. Чому single-client YAML — правильне рішення

### 9.1 Простота системи

Single-client YAML прибирає потребу в multi-client grouping logic у generation
path. Це різко зменшує orchestration complexity і кількість edge cases.

### 9.2 Кращий operator workflow

Оператор працює з одним кейсом за раз:

- один клієнт;
- один editable файл;
- один набір immobili;
- одна зрозуміла підготовка перед generation.

Це відповідає реальному сервісному процесу значно краще, ніж batch-like
multi-client YAML.

### 9.3 Краща validation model

Single-client document дозволяє легко гарантувати:

- один `LOCATORE_CF`;
- узгоджені root-level fields;
- повний контроль над active records;
- predictable strict immobile matching.

У flat multi-client format ці перевірки були б складнішими і шумнішими.

### 9.4 Менший ризик для browser-sensitive частини

Коли fetch/update винесено в prepare mode, а generation mode взагалі не ходить у
SISTER, production command перестає бути змішаним browser/data runner-ом.

Це:

- знижує ризик випадково зачепити browser flow;
- знижує залежність generation від `state.json`;
- робить replay операторського workflow передбачуваним.

### 9.5 Чому multi-CF architecture тут зайва

Multi-CF generation bundle був би доречний тільки як generic batch framework.
Для реального UPPI workflow це зайва універсальність, яка:

- ускладнює input contract;
- ускладнює validation;
- ускладнює runtime logs;
- збільшує ризик регресій у spider/pipeline orchestration;
- не дає практичної переваги оператору.

## 10. Остаточний phased implementation plan

### Phase 0: contract and ADR alignment

**Ціль**

Зафіксувати final architecture decisions до будь-якого runtime change.

**Підзадачі**

- оформити ADR для 3 режимів
- зафіксувати final `immobili.yml` shape
- зафіксувати field classification matrix
- зафіксувати `"-"` policy matrix
- зафіксувати rule: prepare owns fetch/update logic

**Залежності**

- завершений architecture analysis

**Критерії готовності**

- погоджений source-of-truth doc
- немає відкритої неоднозначності щодо single-client contract

**Ризики**

- нечітке відділення persistable vs run-only fields

### Phase 1: input contract rename and source abstraction

**Ціль**

Замінити `clients.yml`-centric input surface на `immobili.yml` + `clients.csv`.

**Підзадачі**

- ввести new config source abstraction
- ввести single-client document loader
- ввести CSV loader
- прибрати `FORCE_UPDATE_VISURA` з generation document contract
- запланувати migration path для `UPPI_CLIENTS_YAML`

**Залежності**

- Phase 0

**Критерії готовності**

- generation input model читає single-client document
- bulk input model читає CSV
- старі hardcoded references локалізовані

**Ризики**

- drift у tests / tooling, які ще очікують flat `clients.yml`

### Phase 2: DB-driven `immobili.yml` generator

**Ціль**

Навчити систему будувати final single-client `immobili.yml` з БД.

**Підзадачі**

- побудувати read model для одного клієнта
- виділити root-level vs immobile-level fields
- забезпечити deterministic ordering immobili
- підтягувати allowlisted persistable fields
- залишати run-only fields blank

**Залежності**

- Phase 1

**Критерії готовності**

- для CF з already populated DB генерується коректний `immobili.yml`
- всі immobile клієнта потрапляють у файл
- root mapping + `immobili:` shape стабільна

**Ризики**

- випадково використати run-only `contracts` fields як prepare defaults

### Phase 3: prepare-by-CF mode

**Ціль**

Зробити prepare CLI єдиним owner-ом fetch/update decision logic.

**Підзадачі**

- додати CLI `--cf`
- додати CLI `--force-update-visura`
- реалізувати case A/B/C decision tree
- reuse protected import runner без browser rewrite
- після import або DB-read будувати YAML через Phase 2 generator

**Залежності**

- Phase 2
- reuse import runner design

**Критерії готовності**

- case A: DB hit, no browser, YAML з БД
- case B: DB miss, import, YAML після import
- case C: DB hit + force, forced import, YAML після refresh

**Ризики**

- неправильно централізувати decision logic, залишивши її частину в generation
  spider

### Phase 4: import-only bulk CSV mode

**Ціль**

Додати окремий масовий import-only режим.

**Підзадачі**

- CSV parser
- dedupe / normalize CF
- loop per CF
- reuse prepare import runner, але без YAML generation
- summary logging/reporting

**Залежності**

- Phase 3 import runner

**Критерії готовності**

- bulk mode оновлює БД по списку CF
- `immobili.yml` не генерується
- generation stages не викликаються

**Ризики**

- протягнути generation logic у bulk mode через reuse старого `VisuraProcessor`

### Phase 5: generation-focused `scrapy crawl uppi`

**Ціль**

Перетворити основний production command на generation-only runner.

**Підзадачі**

- repurpose `uppi` spider до YAML-driven generation input
- optionally додати internal import spider для prepare/bulk
- split current processor на import-only та generation-only orchestration
- замінити permissive YAML filtering на strict immobile match
- обмежити DB write-back тільки persistable fields
- винести run-only fields в in-memory generation context / snapshots

**Залежності**

- Phase 1-4

**Критерії готовності**

- `scrapy crawl uppi` не ходить у SISTER
- generation іде тільки по active immobili з prepared YAML
- missing DB immobile дає hard-fail із посиланням на prepare

**Ризики**

- найвищий runtime risk у цьому change set
- треба дуже уважно не змішати browser/import flow з generation flow

### Phase 6: validation / clear semantics / docs / tests / rollout

**Ціль**

Закріпити нові контракти, validation rules і rollout discipline.

**Підзадачі**

- додати validation для single-client document
- додати validation для active records
- додати validation для forbidden clears
- завершити field-level `"-"` policy
- оновити docs
- оновити smoke checklist
- закріпити regression plan

**Залежності**

- всі попередні фази

**Критерії готовності**

- new contract documented
- field classification and clear semantics стабільні
- rollout checklist готовий

**Ризики**

- неповне покриття edge cases навколо clear semantics і prepare/generation
  boundaries

## 11. Practical recommended order

1. Заморозити
   [./immobili_rollout_source_of_truth.md](../immobili_rollout_source_of_truth.md)
   як canonical contract і не повертатися до multi-CF generation architecture.
2. Винести input contract у single-client `immobili.yml` + CSV abstraction.
3. Побудувати DB-driven generator для `immobili.yml`.
4. Винести protected import path у dedicated import runner / internal import
   spider без зміни browser sequence.
5. Реалізувати prepare-by-CF mode поверх цього import runner.
6. Реалізувати bulk CSV import-only mode поверх того самого import runner.
7. Тільки після цього переробити `scrapy crawl uppi` у generation-focused mode.
8. Наприкінці закрити validation, `"-"` policy, docs, smoke checklist і rollout.
