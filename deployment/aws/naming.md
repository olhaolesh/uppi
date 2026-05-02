# AWS Naming Convention

Цей документ фіксує Stage 10B naming convention для майбутнього test environment.
Він не створює ресурси й не означає, що конкретні names already зайняті в AWS.

## Environment key

Базовий environment для MVP test rollout:

- `test`

Optional later environments:

- `dev`
- `prod`

## Recommended resource prefixes

Використовувати префікс:

- `uppi-test-`

## Recommended names

### Backend / ECS

- ECS cluster: `uppi-test-cluster`
- ECS task definition family: `uppi-test-backend`
- ECS service: `uppi-test-backend-service`
- ECR repository: `uppi-backend-test`
- CloudWatch log group: `/ecs/uppi-test-backend`

### Networking / edge

- ALB: `uppi-test-alb`
- Backend target group: `uppi-test-backend-tg`
- ACM certificate: stage-specific, bound to chosen test domain

### Data / storage

- RDS instance identifier: `uppi-test-postgres`
- Frontend bucket: `uppi-test-frontend`
- Visure bucket: `uppi-test-visure`
- Attestazioni bucket: `uppi-test-attestazioni`

## Route / domain convention

Use placeholders until Stage 11 confirms the real domain:

- frontend: `https://test.<DOMAIN>`
- backend API: `https://api.test.<DOMAIN>`

Do not commit a real production domain in this stage.

## Parameter Store root

Use one root per environment:

- `/uppi/test/...`

Examples are documented in:

- [./parameter_store.md](./parameter_store.md)

## Tagging convention

Recommended common tags:

- `Project=UPPI`
- `Environment=test`
- `Owner=<TEAM_OR_OWNER>`
- `ManagedBy=manual-stage-11`

## Notes

- Keep names stable between task definition, IAM policies and runbooks.
- Avoid sharing `state.json` or local artifact paths between tasks even if
  resource names stay stable.
