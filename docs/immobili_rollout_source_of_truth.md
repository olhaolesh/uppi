# UPPI Rollout Source of Truth

Цей документ є canonical source of truth для finalized rollout-контракту UPPI.

Він є нормативним для:

- цільових режимів роботи;
- shape `immobili.yml`;
- класифікації полів;
- `"-"` semantics;
- меж між `prepare`, bulk import і generation.

Цей документ не переписує protected browser/runtime invariants. Для них
пріоритет мають [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
і [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md).

Поточний runtime уже має відповідати цьому документу. Operator-oriented usage
див. у [./operator_workflow.md](./operator_workflow.md), а точну matrix для
`"-"` див. у [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md).

## 1. Three target modes

### `prepare-by-codice-fiscale`

- Єдиний режим, який володіє fetch/update logic.
- За потреби може reuse-ити чинний protected browser/import path без зміни його
  semantics.
- Будує один single-client `immobili.yml`.
- Відповідає за те, щоб generation працював тільки з already prepared data.

### `bulk-import-by-clients-csv`

- Працює тільки як import-only режим.
- Читає список клієнтів із `clients.csv`.
- Може запускати тільки import/browser path.
- Не генерує `immobili.yml`.
- Не запускає generation stages.

### `scrapy crawl uppi`

- Є generation-only mode.
- Читає тільки already prepared single-client `immobili.yml`.
- Не ходить у SISTER.
- Не володіє fetch/update logic.
- Не має hidden fallback у browser/import path.
- Якщо immobile не знайдений у БД, generation має hard-fail і явно відсилати до
  `prepare-by-codice-fiscale`.

## 2. Canonical `immobili.yml` shape

Остаточний shape:

- один `immobili.yml` = один клієнт;
- документ має root fields + `immobili: [...]`;
- root mapping містить client-level metadata і root persistable fields;
- `immobili:` містить всі immobili цього клієнта.

Conceptual example:

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

    RENDITA: "EUR 123.45"
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

    A1: ""
    B1: ""
    C1: ""
    D1: ""

    CONDUTTORE_NOME: ""
    CONDUTTORE_CF: ""
    CONTRATTO_DATA: ""
    DECORRENZA_DATA: ""
    REGISTRAZIONE_DATA: ""
    REGISTRAZIONE_NUM: ""
    AGENZIA_ENTRATE_SEDE: ""
    CANONE_CONTRATTUALE_MENSILE: ""
    DURATA_ANNI: ""
```

`enabled` є generation control flag для конкретного run. Він не змінює правило,
що canonical business contract документа лишається single-client root mapping +
`immobili: [...]`.

## 3. Field classification

### Root metadata

- `LOCATORE_CF`
- `COMUNE`
- `TIPO_CATASTO`
- `UFFICIO_PROVINCIALE_LABEL`

Ці поля ідентифікують клієнта або prepare/import metadata surface. Вони не є
generation write-back surface.

У межах `LOCATORE_*` тут залишається тільки `LOCATORE_CF`. Persistable root
subset винесено окремо нижче.

### Root persistable fields

- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`

Це persistable client fields. Для них allowed YAML override surface існує, а
`prepare` має підставляти їх із БД, якщо значення вже є.

Тобто `LOCATORE_*` у rollout-контракті розділяються так:

- `LOCATORE_CF` = root metadata;
- `LOCATORE_COMUNE_RES`, `LOCATORE_VIA`, `LOCATORE_CIVICO` = root persistable
  fields.

### Immobile identity fields

- `FOGLIO`
- `NUMERO`
- `SUB`

Generation використовує ці поля разом із root `LOCATORE_CF` для strict match у
БД. Після match canonical source лишається БД, а не YAML.

### Visura display fields

- `RENDITA`
- `SUPERFICIE_TOTALE`
- `CATEGORIA`
- `VISURA_COMUNE`
- `VISURA_VIA`
- `VISURA_CIVICO`

Ці поля потрібні для operator readability і selection confidence, але вони не є
persistable editable surface.

### Immobile persistable editable fields

- `IMMOBILE_COMUNE`
- `IMMOBILE_VIA`
- `IMMOBILE_CIVICO`
- `IMMOBILE_PIANO`
- `IMMOBILE_INTERNO`
- `ENERGY_CLASS`
- `ARREDATO`
- `ISTAT`
- `IGNORE_SURCHARGES`
- `CONTRACT_KIND`
- `A*`
- `B*`
- `C*`
- `D*`

Це persistable fields. Під час generation діє правило `YAML > DB`, а explicit
clear має бути write-back clear у БД.

### Run-only fields

- `CONDUTTORE_*`
- `CONTRATTO_DATA`
- `DECORRENZA_DATA`
- `REGISTRAZIONE_*`
- `AGENZIA_ENTRATE_SEDE`
- `CANONE_CONTRATTUALE_MENSILE`
- `DURATA_ANNI`

Ці поля існують тільки для поточного generation run. Вони можуть потрапляти в
calculation/document snapshot, але не стають canonical prepare defaults.

## 4. `"-"` semantics

### Persistable clear

Для persistable fields `"-"` означає explicit clear і має трактуватися як
write-back clear у БД.

Це стосується:

- root persistable fields;
- immobile override persistable fields;
- `ENERGY_CLASS`;
- `ARREDATO`;
- `ISTAT`;
- `IGNORE_SURCHARGES`;
- `A/B/C/D`.

Implementation-specific note:

- `ARREDATO` and `ISTAT` clear to nullable DB state;
- `IGNORE_SURCHARGES` clears to `False` because the current schema uses a
  non-null boolean;
- `CONTRACT_KIND` is persistable but not clear-allowed.

### Run-only clear

Для run-only fields `"-"` означає clear only for current run.

Наслідки:

- значення очищається тільки в in-memory generation context;
- write-back у master DB state не відбувається;
- значення не стає новим prepare default;
- DOCX generation і calculation бачать blank current-run value, а не literal
  `"-"`.

### Validation error groups

Для таких груп `"-"` є validation error:

- root metadata;
- immobile identity fields;
- visura display fields.

Окремо:

- `CONTRACT_KIND` є explicit forbidden clear target.

### Active record identity requirement

Для кожного active immobile record generation input повинен містити:

- `FOGLIO`
- `NUMERO`
- `SUB`

`SUB` must be present even when the cadastral sub is blank. If an active record
does not carry full identity, generation must stop at validation time before
strict DB matching.

## 5. Ownership and fallback rules

### `prepare` owns fetch/update logic

Фінальне правило:

- тільки `prepare-by-codice-fiscale` вирішує, чи треба йти в browser/import
  path, чи вистачає already prepared DB state;
- `bulk-import-by-clients-csv` не займається generation;
- `scrapy crawl uppi` не займається fetch/update.

### Generation has no hidden SISTER fallback

Generation mode:

- не викликає SISTER;
- не запускає import/browser path;
- не робить implicit repair of missing DB state;
- не приймає silent fallback decision.

Якщо immobile, заданий у `immobili.yml`, не знайдений у БД за
`LOCATORE_CF + FOGLIO + NUMERO + SUB`, generation має завершитися hard-fail з
прямою вказівкою спочатку запустити `prepare-by-codice-fiscale`.

## 6. Protected invariants that remain unchanged

Цей rollout не змінює:

- browser-critical flow;
- `state.json` lifecycle;
- AE/SISTER flow;
- visura download flow;
- selector order;
- wait sequence;
- logout semantics;
- no-blind-retry rule для browser-critical stages.

Canonical invariants already frozen in:

- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- [./live_smoke_strategy_ae_sister.md](./live_smoke_strategy_ae_sister.md)

## 7. Related docs

- ADR: [./adr_0001_single_client_immobili_contract.md](./adr_0001_single_client_immobili_contract.md)
- Operator workflow: [./operator_workflow.md](./operator_workflow.md)
- Validation / clear matrix:
  [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)
- Regression test map: [./regression_test_map.md](./regression_test_map.md)
- Rollout-ready checklist: [./rollout_ready_checklist.md](./rollout_ready_checklist.md)
- Detailed rollout plan:
  [./uppi_update_implementation_plan.md](./uppi_update_implementation_plan.md)
- Practical step-by-step plan:
  [./Uppi_Покроковий_План_Виконання.md](./Uppi_Покроковий_План_Виконання.md)
