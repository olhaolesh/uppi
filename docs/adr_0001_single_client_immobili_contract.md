# ADR 0001: Single-Client `immobili.yml` and Generation Boundary

- Status: Accepted
- Date: 2026-04-06
- Source of truth:
  [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)

## Context

Поточний runtime змішує в одному spider-run:

- flat-list `clients.yml` input;
- рішення про fetch/update;
- browser/import path;
- generation path.

Це робить input contract неоднозначним, а production command одночасно і
import-runner-ом, і generation-runner-ом.

## Decision

Прийнято три рішення.

### 1. Відмовляємося від multi-CF / flat-list input

- Один YAML-файл більше не описує кілька клієнтів.
- Flat-list модель більше не є цільовим контрактом.
- `clients.csv` лишається тільки для bulk-import surface.

### 2. Single-client YAML є цільовим контрактом

- `immobili.yml` описує тільки одного клієнта.
- Документ має shape `root fields + immobili: [...]`.
- Root містить client-level metadata і root persistable fields.
- `immobili:` містить immobili цього клієнта, їх identity/display data,
  persistable editable fields і run-only fields.

### 3. Generation mode відділяється від import/browser path

- `prepare-by-codice-fiscale` володіє fetch/update logic.
- `bulk-import-by-clients-csv` лишається import-only.
- `scrapy crawl uppi` стає generation-only mode.
- Generation mode не має hidden fallback у SISTER і hard-fail-ить, якщо
  потрібного immobile немає в БД.

## Rationale

### Чому не multi-CF / flat-list

- Важче валідувати один run як один операторський пакет роботи.
- Root data повторюються або змішуються з per-immobile полями.
- Вищий ризик випадково змішати кількох клієнтів у одному generation run.

### Чому single-client YAML

- Контракт прямо кодує правило `один файл = один клієнт`.
- Root-level data більше не дублюються в кожному immobile record.
- Простішою стає класифікація полів і clear semantics.

### Чому generation окремо від import/browser path

- Browser-critical flow лишається protected і не повинен отримувати новий
  orchestration coupling із generation.
- Generation стає детермінованішим: або DB state already prepared, або треба
  явно запускати `prepare`.
- Зникає приховане змішування import/update decision з DOCX generation.

## Consequences

- Новий rollout має орієнтуватися на
  [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md),
  а не на legacy `clients.yml`-centric docs.
- Документи, що описують поточний mixed spider flow, мають бути явно позначені
  як legacy/current-runtime notes для цієї хвилі.
- Protected invariants навколо browser path, `state.json`, AE/SISTER, visura
  download, selector order, wait sequence і logout semantics лишаються
  незмінними.
