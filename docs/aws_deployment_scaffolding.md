# AWS Deployment Scaffolding

Цей документ є canonical Stage 10B map для deploy/infra scaffolding у repo.
Він не створює live AWS resources і не додає AWS runtime integration у UPPI.

## Що додано

Deployment scaffold живе в:

- [../deployment/README.md](../deployment/README.md)
- [../deployment/aws/README.md](../deployment/aws/README.md)

Ключові artifacts:

- ECS/Fargate backend task definition template
- IAM policy templates
- Parameter Store path contract
- backend/frontend test env examples
- Stage 11 manual runbooks
- local-only template validation/render scripts

## Що це не робить

- не виконує `terraform apply`
- не виконує `cdk deploy`
- не виконує `aws ecs create-service`
- не виконує `aws rds create-db-instance`
- не додає SSM/Secrets Manager client у FastAPI runtime
- не змінює current prepare/import/generation semantics

## Stage 11 relationship

Stage 10B готує безпечний contract для Stage 11:

1. naming
2. Parameter Store paths
3. IAM policy intent
4. ECS task definition rendering
5. manual runbooks for push/provision/smoke

Фактичне створення AWS test environment лишається окремим наступним етапом.
