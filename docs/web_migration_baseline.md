# Web Migration Baseline

- Status: Accepted baseline freeze
- Date: 2026-05-02
- Scope: Stage 0 documentation freeze before any web/API/AWS runtime implementation

Цей документ фіксує current baseline UPPI перед стартом web/API робіт.
Він не змінює runtime semantics і не дає дозволу змішувати backend shell,
frontend, Docker, AWS provisioning або browser/runtime refactor в одному slice.

Canonical references:

- [./immobili_rollout_source_of_truth.md](./immobili_rollout_source_of_truth.md)
- [./operator_workflow.md](./operator_workflow.md)
- [./current_architecture.md](./current_architecture.md)
- [./runtime_flow.md](./runtime_flow.md)
- [./refactor_protected_invariants.md](./refactor_protected_invariants.md)
- [./state_json_lifecycle_contract.md](./state_json_lifecycle_contract.md)
- [./aws_readiness_runtime_boundaries.md](./aws_readiness_runtime_boundaries.md)
- [./validation_clear_policy_matrix.md](./validation_clear_policy_matrix.md)
- [./document_generation.md](./document_generation.md)
- [./transaction_resource_safety_review.md](./transaction_resource_safety_review.md)
- [./workspace_local_artifacts_policy.md](./workspace_local_artifacts_policy.md)
- [./logging_foundation.md](./logging_foundation.md)
- [./adr_0001_single_client_immobili_contract.md](./adr_0001_single_client_immobili_contract.md)
- [./2_Uppi_Aws_Implementation_Plan.md](./2_Uppi_Aws_Implementation_Plan.md)
- [./3_Uppi_Aws_Provisioning_Checklist.md](./3_Uppi_Aws_Provisioning_Checklist.md)

## 1. Current Runtime Modes

UPPI має три окремі runtime modes:

1. `prepare-by-CF` / `prepare-by-codice-fiscale`
2. `bulk-import-by-clients-csv`
3. `scrapy crawl uppi`

Поточний Stage 0 freeze означає:

- ще немає FastAPI app;
- ще немає frontend app;
- ще немає Dockerfile для web/backend runtime;
- ще немає AWS provisioning artifacts у runtime slice.

## 2. Ownership Boundaries

Current ownership map already frozen:

- `prepare-by-CF` володіє fetch/update decision logic.
- `prepare-by-CF` вирішує `DB hit` vs `DB miss` vs `force refresh`.
- bulk import лишається import-only boundary.
- `scrapy crawl uppi` лишається generation-only mode.
- generation є SISTER-free і читає тільки already prepared single-client
  `immobili.yml`.
- generation не має hidden fallback у browser/import path.
- web layer у майбутніх slices не повинен дублювати orchestration rules для
  prepare, import або generation.
- web controllers/API adapters повинні викликати existing service boundaries, а
  не переносити decision logic у web surface.

## 3. Canonical Inputs

Current canonical inputs лишаються такими:

- generation input:
  - `clients/immobili.yml`
  - або явний override через `UPPI_IMMOBILI_YAML`
- bulk import input:
  - `clients.csv`
  - або явний override через `UPPI_CLIENTS_CSV`
- legacy compatibility only:
  - flat `clients.yml`
  - або `UPPI_CLIENTS_YAML`
  - використовується тільки для internal protected import spider compatibility

Операторський і business contract лишається:

- один `immobili.yml` = один клієнт;
- bulk import не створює `immobili.yml`;
- generation не читає `clients.yml`.

## 4. Protected Browser / Import Flow

Protected import/browser path лишається internal reusable boundary для
`prepare-by-CF` і bulk import.

Current protected flow включає:

- AE authentication
- direct AE -> SISTER transition
- SISTER navigation
- CAPTCHA handling
- visura download
- explicit logout semantics
- `state.json` lifecycle

Protected `state.json` contract лишається таким:

`fresh session -> save state -> use for direct SISTER -> logout -> delete invalid state`

Практичний зміст цього freeze:

- не змінювати точки create/load/delete для `state.json`;
- не перетворювати `state.json` на reusable cache між новими сесіями;
- не замінювати explicit logout на passive browser close;
- не змінювати selector order або wait/click/fill sequence під приводом
  webization чи cloud-hardening.

## 5. Browser-Critical Files And Surfaces

Під час normal web/API slices не можна змінювати такі browser-critical files і
surfaces:

- `uppi/settings.py`
- `uppi/spiders/uppi_browser_spider.py`
- `uppi/spiders/uppi_import_spider.py`
- `uppi/ae/auth.py`
- `uppi/ae/sister_navigation.py`
- `uppi/ae/captcha.py`
- `uppi/ae/download.py`
- `uppi/ae/uppi_selectors.py`
- будь-який код, який змінює timing create/load/delete для `state.json`
- будь-який код, який змінює logout timing
- будь-який код, який замінює explicit logout на passive browser close semantics

Зміни в цих зонах переходять у `high scrutiny` і потребують окремого
browser-sensitive slice з live smoke.

## 6. Generation / Document Boundaries

Current generation contract already frozen:

- strict DB match виконується за `LOCATORE_CF + FOGLIO + NUMERO + SUB`
- generation hard-fail-ить, якщо immobile не знайдений у БД
- YAML-over-DB діє тільки для allowed editable fields
- root metadata, identity fields, visura/display fields і run-only fields не
  стають write-back surface поза чинною policy

Current document-generation contract також лишається без casual changes:

- execution order у `DocumentStageService` лишається:
  1. `build_template_params(...)`
  2. `get_attestazione_path(...)`
  3. `fill_attestazione_template(...)`
  4. `storage_service.upload_file(...)`
  5. `audit_stage.log_generated(...)`
- document generation path/order не змішувати з web migration work
- placeholder precedence, underline semantics і blank behavior не міняти без
  окремого review
- output naming contract не змінювати у web migration slices

Current naming contract:

- local DOCX:
  `downloads/{CF}/ATTESTAZIONE_{CF}_{contract_id}_{slug}.docx`
- remote object:
  `attestazioni/{CF}/{contract_id}.docx`

## 7. Current AWS Readiness Baseline

Current AWS-readiness baseline already зафіксований, але actual migration ще не
почалася:

- MVP deployment target: ECS/Fargate
- first target is not Lambda
- runtime все ще потребує writable ephemeral local filesystem для:
  - `downloads/`
  - `captcha_images/`
  - `logs/`
  - generated DOCX/PDF artifacts before upload
- S3/object storage boundary already exists conceptually and code-level
  encapsulation already exists
- DB connection factory seam already exists
- config/provider seam already exists via current dataclasses/factories
- future config provider має залишатися provider-agnostic і гідрувати existing
  dataclasses, а не робити direct AWS SDK calls із business services/stages
- MVP primary config/secret store: AWS Systems Manager Parameter Store
- AWS Secrets Manager лишається optional future path
- AWS provisioning track already documented separately and must stay separate
  from runtime implementation changes

Current config seams, які треба зберегти:

- `DatabaseConfig`
- `ClientsSourceConfig`
- `VisuraProcessorRuntimeConfig`
- `ObjectStorageConfig`
- `WorkspaceConfig`

## 8. Regression / Smoke Checklist

Для наступних slices baseline regression checklist такий:

- запустити existing `pytest` suite
- перевірити, що CLI `prepare-by-CF` semantics не змінилися
- перевірити, що bulk import CLI semantics не змінилися
- перевірити, що `scrapy crawl uppi` лишається generation-only mode
- перевірити, що generation не ходить у SISTER і не дублює prepare logic
- browser-sensitive changes вимагають live smoke
- цей Stage 0 slice не повинен робити browser-sensitive changes і тому не
  повинен вимагати browser-flow rewrite або live browser regression

## 9. Safe Vs Unsafe Changes For Future Codex Prompts

### Safe examples

- adding web docs
- adding isolated FastAPI shell later without touching business flows
- adding DTOs/adapters that call existing service boundaries
- adding provider abstraction that hydrates existing config dataclasses

### Unsafe examples

- changing AE/SISTER selector order
- changing Playwright wait/click/fill sequence
- changing `state.json` lifecycle
- making generation call SISTER
- duplicating prepare decision logic in web controllers
- mixing AWS provisioning, frontend UI, and business runtime changes in one PR

## 10. Stage 0 Outcome

Stage 0 freeze встановлює базове правило:

- спочатку фіксуємо current contracts письмово;
- перший реальний code slice після цього freeze має бути additive backend shell;
- browser-critical runtime, import orchestration, generation contract і AWS
  provisioning мають рухатися окремими slices.
