# Workspace / Local Artifacts Policy

Цей документ фіксує current Sprint 3 policy для локальних артефактів і
workspace root без зміни browser-critical semantics.

## Scope

Поточний policy layer покриває:

- `downloads/`
- `captcha_images/`
- локальні PDF/DOCX artifacts
- `logs/` як локальний runtime output root
- compatibility notes для `state.json`

Canonical code-level surface:

- [uppi/config/workspace.py](../uppi/config/workspace.py)
- [uppi/domain/storage.py](../uppi/domain/storage.py)
- AWS-readiness note:
  [./aws_readiness_runtime_boundaries.md](./aws_readiness_runtime_boundaries.md)

## First-Pass Rule

На цьому кроці діє жорстке правило:

- default paths stay the same

Тобто без додаткового конфігу current paths лишаються repo-local:

- `downloads/`
- `captcha_images/`
- `logs/`
- `state.json`

## Configurable Workspace Root

Додано additive seam:

- `UPPI_WORKSPACE_ROOT`
- `WorkspaceConfig.from_env(workspace_root=...)`

Що це змінює:

- `downloads_dir`
- `captcha_images_dir`
- `logs_dir`

Що це навмисно НЕ змінює:

- `state.json` path contract

## Artifact Categories

### Downloads tree

Code path:

- [uppi/domain/storage.py](../uppi/domain/storage.py)

Current shape:

- `downloads/{CF}/VISURA_{CF}.pdf`
- `downloads/{CF}/ATTESTAZIONE_{CF}_{contract_id}_{slug}.docx`

Default behavior:

- by default лишається repo-local `downloads/`
- optional workspace override only changes root, not naming

### CAPTCHA screenshots

Code path:

- [uppi/ae/captcha.py](../uppi/ae/captcha.py)

Current shape:

- `captcha_images/{CF}/captcha.png`

Default behavior:

- by default лишається repo-local `captcha_images/`
- fresh session cleanup у spider лишається тим самим semantic step

### Logs

Code path:

- [uppi/logging_config.py](../uppi/logging_config.py)
- [uppi/config/workspace.py](../uppi/config/workspace.py)

Current note:

- local workspace policy documentує `logs/` як runtime-local output root
- цей PR не redesign-ить logging lifecycle

### `state.json`

Code paths:

- [uppi/settings.py](../uppi/settings.py)
- [uppi/spiders/uppi_spider.py](../uppi/spiders/uppi_spider.py)
- [uppi/ae/auth.py](../uppi/ae/auth.py)
- [uppi/ae/sister_navigation.py](../uppi/ae/sister_navigation.py)

Current rule:

- `state.json` лишається в current relative location
- lifecycle semantics не змінюються
- workspace root не переносить `state.json`
- ownership і current cleanup shape окремо зафіксовані в
  [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)

Причина:

- це protected lifecycle artifact, а не звичайний локальний cache/output

## Cleanup Contract

### Already existing cleanup

- `state.json`
  - cleanup на fresh spider start
  - cleanup після failed login
- `captcha_images/`
  - cleanup на fresh spider start
- local visura PDF
  - optional cleanup only after outer commit
  - only when `DELETE_LOCAL_VISURA_AFTER_UPLOAD=True`

### Current non-cleanup behavior

- generated local DOCX не чиститься агресивно
- uploaded storage objects не мають compensating delete logic
- artifact lifecycle не redesign-иться в цьому PR

## Explicit Non-Goals

У цьому кроці свідомо не робиться:

- `state.json` lifecycle changes
- transaction-boundary changes
- aggressive artifact cleanup redesign
- AWS-ready storage/workspace implementation
- browser flow changes

## Merge / Verification Note

Оскільки PR торкається artifact path resolution і cleanup-adjacent path handling,
manual live smoke sign-off потрібен перед merge.
