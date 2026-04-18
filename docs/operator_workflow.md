# Operator Workflow

Цей документ є practical runbook для оператора або junior-розробника. Якщо треба
зрозуміти, що запускати і в якій послідовності, починати треба звідси, а не з
коду.

Нормативний behavioral contract зафіксований у
[./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md).
Policy matrix для `"-"` див. у
[./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md).

## 1. Три режими роботи

### `prepare-by-CF`

Command:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

Input:

- `--cf`
- optional `--force-update-visura`
- optional `--output`

What it does:

- перевіряє, чи вже є достатній DB state для цього `LOCATORE_CF`
- якщо DB miss або задано `--force-update-visura`, запускає import-only path
- після цього генерує один single-client `immobili.yml`

Output:

- готовий YAML-файл для одного клієнта
- за замовчуванням це canonical generation path `clients/immobili.yml`
- якщо задано `--output`, файл буде записаний туди

If you use a custom output path and then want `scrapy crawl uppi` to read that
file, set:

```bash
export UPPI_IMMOBILI_YAML=/absolute/path/to/immobili.yml
```

### `bulk-import-by-clients-csv`

Command:

```bash
venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
```

Input:

- `--csv`
- optional `--force-update-visura`
- optional `--fail-fast`

What it does:

- читає `clients.csv`
- нормалізує й дедуплікує CF
- для кожного валідного унікального CF запускає import-only boundary

Output:

- DB updates only
- operator summary in stdout
- no `immobili.yml`
- no generation

### `scrapy crawl uppi`

Command:

```bash
scrapy crawl uppi
```

Input:

- prepared single-client `immobili.yml`

What it does:

- валідовує canonical YAML document
- бере тільки active immobili
- робить strict DB match по `LOCATORE_CF + FOGLIO + NUMERO + SUB`
- застосовує YAML-over-DB merge тільки для editable fields
- пише назад тільки persistable fields
- запускає generation stages

Output:

- DB write-back only for persistable surfaces
- canone snapshot
- DOCX generation / upload
- audit records

It does not:

- логінитися в AE/SISTER
- refresh-ити visura
- робити import fallback

## 2. Recommended Single-Client Flow

1. Run prepare:

   ```bash
   venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
   ```

2. Open the generated `clients/immobili.yml`.

3. Review the root fields:

- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`

4. Review the `immobili:` list.

5. Disable any immobile you do not want to generate now:

```yaml
enabled: false
```

6. Edit only the operator-controlled fields you actually want to override.

7. Run generation:

```bash
scrapy crawl uppi
```

## 3. Recommended Bulk Flow

Use bulk mode when the goal is DB refresh, not document generation.

1. Prepare a `clients.csv` file with `LOCATORE_CF` values.
2. Run:

   ```bash
   venv/bin/python -m uppi.cli.bulk_import_clients_csv --csv clients/clients.csv
   ```

3. Review the summary.
4. For any client that now needs generation, switch back to `prepare-by-CF`.

Canonical rule:

- bulk mode is not a wrapper around prepare
- bulk mode never generates YAML
- generation still starts from `prepare-by-CF`

## 4. Editing `immobili.yml`

### `enabled`

- Missing `enabled` means active.
- `enabled: false` skips that immobile during generation.
- If an immobile is removed from the document, generation never sees it.

### Identity fields for active records

For every active immobile record, the document must contain:

- `FOGLIO`
- `NUMERO`
- `SUB`

`SUB` must be present even when the cadastral sub is blank. A missing identity
field is a validation error and generation stops before DB matching.

### Fields the operator is expected to edit

Root persistable:

- `LOCATORE_COMUNE_RES`
- `LOCATORE_VIA`
- `LOCATORE_CIVICO`

Immobile persistable editable:

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
- `A/B/C/D`

Run-only:

- `CONDUTTORE_*`
- `CONTRATTO_DATA`
- `DECORRENZA_DATA`
- `REGISTRAZIONE_*`
- `AGENZIA_ENTRATE_SEDE`
- `CANONE_CONTRATTUALE_MENSILE`
- `DURATA_ANNI`

## 5. How `"-"` Works

Short version:

- DB-clearable persistable fields: `"-"` means explicit DB clear
- Run-only fields: `"-"` means clear only for the current generation run
- Metadata, identity, visura/display fields: `"-"` is a validation error
- `CONTRACT_KIND`: `"-"` is a validation error

Full matrix:
[./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)

## 6. Common Operator Errors

### Generation says to run prepare first

Meaning:

- generation did a strict DB match
- the requested immobile was not found in DB

Action:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z
```

If the DB state may be stale:

```bash
venv/bin/python -m uppi.cli.prepare_by_cf --cf RSSMRA80A01H501Z --force-update-visura
```

### Validation fails on `immobili.yml`

Typical reasons:

- root document is not a mapping
- `immobili` is not a list
- active record is missing `FOGLIO`, `NUMERO`, or `SUB`
- forbidden `"-"` target such as `LOCATORE_CF`, `FOGLIO`, `VISURA_VIA`, `CONTRACT_KIND`

Action:

- fix the YAML
- rerun `scrapy crawl uppi`

### Bulk mode reports invalid or duplicate rows

Meaning:

- blank CFs are skipped
- malformed CFs are skipped
- duplicate CFs are skipped after the first normalized occurrence

Action:

- clean the CSV if needed
- rerun bulk mode

## 7. Internal Detail You Usually Do Not Need

There is an internal import-only spider:

- [../uppi/spiders/uppi_import_spider.py](../uppi/spiders/uppi_import_spider.py)

Operators normally do not call it directly. It is reused by:

- `prepare-by-CF`
- `bulk-import-by-clients-csv`
