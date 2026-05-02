# Web Service Foundation

- Status: Accepted
- Date: 2026-05-02

## Context

UPPI мігрує у web service на AWS.
Поточний runtime уже розділений на prepare/import/generation boundaries, і
web/API work має зберегти ці межі без runtime rewrite.

Canonical references:

- [../2_Uppi_Aws_Implementation_Plan.md](../2_Uppi_Aws_Implementation_Plan.md)
- [../3_Uppi_Aws_Provisioning_Checklist.md](../3_Uppi_Aws_Provisioning_Checklist.md)
- [../current_architecture.md](../current_architecture.md)
- [../runtime_flow.md](../runtime_flow.md)
- [../aws_readiness_runtime_boundaries.md](../aws_readiness_runtime_boundaries.md)
- [../web_migration_baseline.md](../web_migration_baseline.md)

## Decisions

1. Web layer буде additive і не переписуватиме current prepare/import/generation contracts.
2. MVP backend target: FastAPI, але Stage 0 не створює FastAPI app.
3. Deployment target для MVP: ECS/Fargate, а не Lambda, тому що Playwright/browser automation і document generation потребують fuller process model та writable ephemeral filesystem.
4. Frontend і backend стартують в одному monorepo.
5. AWS Systems Manager Parameter Store є primary MVP config/secret source; AWS Secrets Manager відкладений як optional future path, якщо пізніше знадобиться rotation або інший secret lifecycle.
6. Future provider layer має гідрувати existing config dataclasses, а не робити direct AWS SDK calls із business services або stages.
7. AWS provisioning відстежується окремо від runtime implementation і не змішується з business logic changes.

## Consequences

- перший real code slice після цього documentation freeze має бути backend shell only
- auth/session має йти окремим пізнішим slice
- prepare/search API, generation API і bulk import API мають бути трьома окремими slices
- Docker/AWS deployment foundation приходить пізніше, після web/API foundations
- browser-critical changes лишаються зонами high scrutiny і потребують live smoke
