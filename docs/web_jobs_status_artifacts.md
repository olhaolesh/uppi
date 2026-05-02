# Web Jobs / Status / Artifacts

Цей документ описує Stage 9 lightweight web job/run registry для web shell і
frontend екрана `/jobs`.

Canonical references:

- [./web_backend_shell.md](./web_backend_shell.md)
- [./web_attestazioni_search_prepare.md](./web_attestazioni_search_prepare.md)
- [./web_attestazioni_generation.md](./web_attestazioni_generation.md)
- [./web_clients_bulk_import.md](./web_clients_bulk_import.md)
- [./frontend_skeleton.md](./frontend_skeleton.md)

## Що це таке

Stage 9 додає lightweight registry для web-run-ів. Це не async queue і не scheduler.
Поточні endpoints лишаються synchronous, але після виконання записують safe job records.

MVP storage location:

- `clients/web_jobs/jobs.json`

Registry зберігає UTF-8 JSON, створює директорію автоматично і не вимагає DB schema changes.

## Які flows записуються

- `POST /attestazioni/search`
- `POST /attestazioni/generate`
- `POST /clients/bulk-import`

## Jobs API

Protected endpoints:

- `GET /jobs`
- `GET /jobs/{run_id}`

Для обох потрібна активна web session.

## Job types

- `attestazioni_search`
- `attestazioni_generate`
- `clients_bulk_import`

## Job statuses

- `running`
- `completed`
- `failed`
- `aborted`
- `partial`

`partial` використовується тільки коли synchronous generation завершився HTTP-success,
але має одночасно generated artifacts і failed items.

## Safe events і messages

Registry зберігає тільки safe events/messages на кшталт:

- `Search started`
- `Search completed`
- `Generation started`
- `Generation completed`
- `Generation completed with failures`
- `Bulk import started`
- `Bulk import completed`
- `Bulk import aborted`
- `Operation failed: <safe message>`

Registry не читає raw log files і не віддає log tail у UI.

## Artifact refs

Registry зберігає тільки safe refs:

- prepared `immobili.yml` path для search
- generation `immobili.yml` path для generate
- generated DOCX refs, якщо вони вже є у current generation response
- web-run `clients.csv` path для bulk import

Download endpoint у цьому slice не додається. Якщо `download_url` відсутній, frontend
показує лише technical refs.

## Чого registry не зберігає і не expose-ить

- raw traceback
- raw logs
- password / pin / session secret
- raw cookies або session payload
- `state.json`
- AE credentials
- raw `csv_content`

## Frontend `/jobs`

Екран `/jobs` тепер:

- читає `GET /jobs`
- показує newest-first список run-ів
- дозволяє ручний refresh
- відкриває detail через `GET /jobs/{run_id}`
- показує safe events/messages
- показує artifact refs без фейкового download link

## Чого цей slice не робить

- не додає async queue
- не додає Celery/RQ/SQS
- не додає WebSocket/SSE live logs
- не додає artifact download endpoint
- не додає Docker/AWS/IaC/SSM/Secrets Manager clients
