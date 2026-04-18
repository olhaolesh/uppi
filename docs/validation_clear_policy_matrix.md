# Validation and `"-"` Policy Matrix

Цей документ фіксує фактичну policy/validation behavior для canonical
single-client `immobili.yml` після Етапу 7.

Canonical contract:
[./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)

Operator workflow:
[./operator_workflow.md](./operator_workflow.md)

## 1. Active Record Validation

Для кожного active immobile record generation input повинен містити:

- `FOGLIO`
- `NUMERO`
- `SUB`

Rules:

- `enabled != false` means active
- missing `enabled` means active
- `SUB` must be present even when the cadastral sub is blank
- if active identity is incomplete, validation fails before strict DB match

## 2. `"-"` Semantics by Field Class

| Field class | Examples | `"-"` meaning | DB write-back |
| --- | --- | --- | --- |
| Root persistable | `LOCATORE_COMUNE_RES`, `LOCATORE_VIA`, `LOCATORE_CIVICO` | explicit clear | yes |
| Immobile override persistable | `IMMOBILE_COMUNE`, `IMMOBILE_VIA`, `IMMOBILE_CIVICO`, `IMMOBILE_PIANO`, `IMMOBILE_INTERNO` | explicit clear | yes |
| Persistable technical/business clearable | `ENERGY_CLASS`, `ARREDATO`, `ISTAT`, `IGNORE_SURCHARGES`, `A/B/C/D` | explicit clear | yes |
| Persistable but non-clearable | `CONTRACT_KIND` | validation error | no |
| Run-only | `CONDUTTORE_*`, `CONTRATTO_DATA`, `DECORRENZA_DATA`, `REGISTRAZIONE_*`, `AGENZIA_ENTRATE_SEDE`, `CANONE_CONTRATTUALE_MENSILE`, `DURATA_ANNI` | clear current run only | no |
| Root metadata | `LOCATORE_CF`, `COMUNE`, `TIPO_CATASTO`, `UFFICIO_PROVINCIALE_LABEL` | validation error | no |
| Identity | `FOGLIO`, `NUMERO`, `SUB` | validation error | no |
| Visura/display | `RENDITA`, `SUPERFICIE_TOTALE`, `CATEGORIA`, `VISURA_COMUNE`, `VISURA_VIA`, `VISURA_CIVICO` | validation error | no |

## 3. DB-Clearable Fields

### Root persistable

- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`

Effect:

- `"-"` clears the persisted residence-address override for the owner

### Immobile override persistable

- `IMMOBILE_COMUNE`
- `IMMOBILE_VIA`
- `IMMOBILE_CIVICO`
- `IMMOBILE_PIANO`
- `IMMOBILE_INTERNO`

Effect:

- `"-"` clears the persisted real-address override for the immobile

### Persistable technical/business

- `ENERGY_CLASS`
- `ARREDATO`
- `ISTAT`
- `IGNORE_SURCHARGES`
- `A1..D13`

Effect in the current implementation:

- `ENERGY_CLASS` clears to `NULL`
- `ARREDATO` clears to `NULL`
- `ISTAT` clears to `NULL`
- `IGNORE_SURCHARGES` clears to `False`
- `A/B/C/D` clear by deleting the corresponding `immobile_elements` rows

## 4. Run-Only Fields

Run-only fields exist only in the current generation context:

- `CONDUTTORE_NOME`
- `CONDUTTORE_CF`
- `CONDUTTORE_COMUNE`
- `CONDUTTORE_VIA`
- `CONTRATTO_DATA`
- `DECORRENZA_DATA`
- `REGISTRAZIONE_DATA`
- `REGISTRAZIONE_NUM`
- `AGENZIA_ENTRATE_SEDE`
- `CANONE_CONTRATTUALE_MENSILE`
- `DURATA_ANNI`

Rules:

- `"-"` becomes blank current-run state
- no DB clear happens
- these values do not become new prepare defaults
- DOCX generation sees blank values, not a literal `"-"`

## 5. Explicit Forbidden Clear Targets

### Root metadata

- `LOCATORE_CF`
- `COMUNE`
- `TIPO_CATASTO`
- `UFFICIO_PROVINCIALE_LABEL`

### Identity

- `FOGLIO`
- `NUMERO`
- `SUB`

### Visura/display

- `RENDITA`
- `SUPERFICIE_TOTALE`
- `CATEGORIA`
- `VISURA_COMUNE`
- `VISURA_VIA`
- `VISURA_CIVICO`

### Non-clearable persistable

- `CONTRACT_KIND`

Why `CONTRACT_KIND` is special:

- it is still persistable
- YAML can override DB with a concrete value
- but `"-"` is rejected because the current schema/runtime contract does not use a safe cleared state there

## 6. YAML-over-DB Merge Scope

YAML-over-DB precedence applies only to editable fields.

Editable write-back surfaces:

- root persistable fields
- immobile persistable override fields
- `ENERGY_CLASS`
- `ARREDATO`
- `ISTAT`
- `IGNORE_SURCHARGES`
- `CONTRACT_KIND`
- `A/B/C/D`

Not write-back surfaces:

- root metadata
- identity fields
- visura/display fields
- run-only fields

## 7. Operator-Facing Failure Modes

Examples of early validation errors:

- active record missing `FOGLIO`
- active record missing `NUMERO`
- active record missing `SUB`
- `LOCATORE_CF: "-"` at root
- `VISURA_VIA: "-"` inside an immobile
- `CONTRACT_KIND: "-"` inside an immobile

These fail before generation reaches strict DB matching or document generation.
