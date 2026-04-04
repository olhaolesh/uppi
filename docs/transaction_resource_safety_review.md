# Transaction / Resource Safety Review

Цей документ фіксує current non-browser transaction/resource-safety surface після
Sprint 2 та early Sprint 3 changes.

Мета цього PR:

- чесно описати current ownership map;
- зафіксувати rollback points;
- виділити partial-failure windows між DB, S3, DOCX і local artifacts;
- відокремити allowed containment work від deferred redesign.

Цей документ не змінює runtime semantics і не є дозволом на transaction rewrite.

## Scope і non-goals

У scope цього review:

- [uppi/services/visura_processor.py](../uppi/services/visura_processor.py)
- [uppi/services/visura_stages.py](../uppi/services/visura_stages.py)
- [uppi/services/storage_minio.py](../uppi/services/storage_minio.py)
- [uppi/services/repositories/audit_repo.py](../uppi/services/repositories/audit_repo.py)

Свідомо не робиться:

- transaction-boundary redesign
- unit-of-work rewrite
- compensating actions для S3/local artifacts
- browser-adjacent behavior changes
- retry engine redesign

## Current Ownership Map

### Connection / transaction owner

Current outer transaction owner:

- [VisuraProcessor.process_item()](../uppi/services/visura_processor.py)

Current behavior:

1. `conn = self.connection_factory()` створюється один раз на item.
2. Той самий `conn` передається в усі non-browser stage services.
3. Stage services не володіють transaction boundary.
4. `commit()` викликається один раз наприкінці `process_item()`.
5. `rollback()` викликається тільки в outer `except`.
6. `close()` викликається тільки в outer `finally`.

### Що stage services роблять, а чого не роблять

- `PersonSyncService`: використовує shared `conn`, не commit/rollback.
- `VisuraIngestService`: робить S3 upload + repo insert через shared `conn`, не commit/rollback.
- `ImmobileSyncService`: parser + repo writes на shared `conn`, не commit/rollback.
- `ContractSyncService`: real address / elements / contract / context chain на shared `conn`, не commit/rollback.
- `CanoneStageService`: calc insert + context reload на shared `conn`, не commit/rollback.
- `DocumentStageService`: DOCX generation + upload + audit call, але не commit/rollback.
- `AuditStageService`: audit inserts на shared `conn`, не commit/rollback.

Отже current boundary лишається outer-orchestrator transaction, а не stage-local transaction.

## Current Rollback Map

### Failures, які bubbling-ом доходять до outer rollback

- `PersonSyncService.sync()` failure -> re-raise -> outer rollback
- `VisuraIngestService.ingest()` failure -> re-raise -> outer rollback
- `ImmobileSyncService.sync()` failure -> re-raise -> outer rollback
- `ContractSyncService.sync()` failure -> re-raise -> outer rollback
- `AuditStageService.log_generated()` failure -> re-raise -> outer rollback
- `AuditStageService.log_failed()` failure -> re-raise -> outer rollback
- outer orchestration failure поза stage boundary -> `PipelineFatal` + outer rollback

### Failures, які current code не ескалує до outer rollback

- `CanoneStageService.run()`:
  - current behavior ловить помилку;
  - пише failure record;
  - логує warning;
  - повертає existing `contract_ctx`;
  - outer rollback не викликається.
- `DocumentStageService.run()`:
  - current behavior ловить generation/upload failure;
  - пише failure record;
  - викликає `AuditStageService.log_failed(...)`;
  - повертає `None`;
  - outer rollback не відбувається, якщо `log_failed(...)` успішний.

## Partial-Failure Windows

Нижче перелічені current windows, які вже існують і в цьому PR не “лагодяться”.

### 1. Visura upload succeeded, DB state later rolled back

Current order у `VisuraIngestService`:

1. локальний PDF знайдений
2. checksum порахований
3. S3 upload викликаний
4. `db_upsert_visura(...)`
5. outer `commit()` лише значно пізніше

Window:

- якщо upload успішний, але `db_upsert_visura(...)` або будь-яка наступна stage падає,
  outer rollback скасовує DB state;
- зовнішній S3 object уже існує;
- compensating delete у current flow немає.

### 2. Local visura PDF survives failed processing

Current local cleanup:

- `safe_unlink(visura_ingest.pdf_to_delete)` викликається тільки після outer `commit()`
- і тільки якщо `DELETE_LOCAL_VISURA_AFTER_UPLOAD=True`

Window:

- якщо item processing падає після upload, але до outer commit,
  local PDF лишається на диску;
- це current behavior, а не regression цього PR.

### 3. Canone failure does not rollback prior DB work

Current order:

- `ContractSyncService` уже міг записати real address / elements / contract
- `CanoneStageService` може впасти пізніше

Window:

- canone failure не викликає outer rollback;
- попередні DB writes лишаються в pending transaction і зазвичай будуть committed наприкінці item;
- canone row може бути відсутнім, але DOCX stage далі все одно може виконуватись.

### 4. DOCX generation/upload failed, but failed-audit committed

Current order у `DocumentStageService`:

1. DOCX generation
2. upload у storage
3. on failure -> `AuditStageService.log_failed(...)`
4. `return None`
5. outer `commit()` пізніше

Window:

- якщо generation або upload впав, але `log_failed(...)` успішний,
  DB transaction може бути committed з failed-audit state;
- попередні repo writes для цього item теж committed;
- compensating cleanup local/output artifacts current flow не робить.

### 5. DOCX upload succeeded, audit write failed

Current order:

1. local DOCX already generated
2. upload already succeeded
3. `log_generated(...)` падає
4. outer rollback

Window:

- зовнішній DOCX object уже існує;
- DB state відкочується;
- audit row відсутній;
- local DOCX file теж може лишитися.

### 6. Failed-audit write itself failed

Current order:

1. generation/upload failure already happened
2. `log_failed(...)` itself падає
3. exception bubbling -> outer rollback

Window:

- failed audit row не створюється;
- ранні DB writes відкочуються;
- local DOCX може вже існувати;
- storage object може існувати або ні, залежно від точки падіння.

## Allowed Containment Moves In Sprint 3

Нижче перелік того, що ще дозволено робити без transaction redesign.

- documentation updates для ownership / rollback / partial-failure map
- clearer comments about current ownership, якщо вони не змінюють кодову семантику
- characterization/integration tests, які фіксують unchanged boundaries
- failure registry enrichment:
  - точніший `stage`
  - safe `artifact_refs`
  - better redacted notes
- conservative retry classification, already isolated від orchestration
- explicit review notes для downstream PRs

## Explicitly Deferred Follow-Up Work

Нижче work items, які цей PR лише готує, але не реалізує.

### Deferred transaction/unit-of-work redesign

- move transaction ownership upward або split per-stage / per-immobile
- shorten unit of work around contract/canone/doc generation
- separate DB commit from external S3 side effects
- decouple audit persistence from document upload timing
- introduce compensating deletes for uploaded artifacts
- redesign local artifact cleanup contract
- transactional outbox / async publish / two-phase style coordination

### Deferred safety hardening

- stronger atomicity between repo writes and external artifacts
- resumable processing model
- resource-finalization registry
- broader failure containment automation

## Candidate Target Boundaries For Future Design

Це не approved redesign, а лише candidate boundaries для follow-up discussion.

- `VisuraIngest` as its own explicit unit of work
- `per-immobile ContractSync + CanoneStage + DocumentStage` as separate unit candidate
- `AuditStage` as explicitly decoupled persistence concern
- `local artifact lifecycle` as dedicated policy instead of incidental cleanup

## Confirmed Constraints

Після цього review усе ще чинні такі обмеження:

- browser-critical flow untouched
- `state.json` semantics untouched
- no selector/wait/logout redesign
- no transaction-boundary redesign in Sprint 3 current scope
- no success-path rewrite

## Practical Reading Order

Щоб зрозуміти current ownership surface без домислів:

1. [uppi/services/visura_processor.py](../uppi/services/visura_processor.py)
2. [uppi/services/visura_stages.py](../uppi/services/visura_stages.py)
3. [uppi/services/storage_minio.py](../uppi/services/storage_minio.py)
4. [uppi/services/repositories/audit_repo.py](../uppi/services/repositories/audit_repo.py)
5. [docs/failure_registry_contract.md](./failure_registry_contract.md)

Цей документ є canonical review artifact для transaction/resource-safety scope у Sprint 3.
