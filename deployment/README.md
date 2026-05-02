# Deployment Scaffolding

Ця директорія містить Stage 10B deployment scaffolding для UPPI.

Що тут є:

- шаблони ECS/Fargate task definition
- IAM policy templates
- Parameter Store path contract
- test-environment env examples
- Stage 11 manual runbooks
- локальні scripts для template validation і rendering без AWS API

Що тут навмисно НЕ робиться:

- не створюються AWS resources
- не виконуються `aws ... create`, `terraform apply`, `cdk deploy`,
  `cloudformation deploy`
- не додається runtime SSM/Secrets Manager client
- не змінюється backend/frontend runtime behavior

Stage 10A Docker foundation лишається базою для backend image:

- [../Dockerfile.backend](../Dockerfile.backend)
- [../docs/backend_containerization.md](../docs/backend_containerization.md)

AWS-specific map:

- [./aws/README.md](./aws/README.md)

Canonical repo-level doc:

- [../docs/aws_deployment_scaffolding.md](../docs/aws_deployment_scaffolding.md)
