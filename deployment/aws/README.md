# AWS Deployment Scaffolding

Ця директорія фіксує Stage 10B scaffold для майбутнього AWS test environment.
Усі файли тут є templates, env examples, runbooks або validation helpers.

Жоден файл у цій директорії сам по собі не створює live AWS resources.

## Що додано

### Templates

- [./ecs_task_definition.backend.template.json](./ecs_task_definition.backend.template.json)
- [./iam_policies/ecs_task_execution_policy.template.json](./iam_policies/ecs_task_execution_policy.template.json)
- [./iam_policies/uppi_backend_task_policy.template.json](./iam_policies/uppi_backend_task_policy.template.json)

### Contracts / naming

- [./naming.md](./naming.md)
- [./parameter_store.md](./parameter_store.md)

### Env examples

- [./env/backend.test.env.example](./env/backend.test.env.example)
- [./env/frontend.test.env.example](./env/frontend.test.env.example)

### Runbooks

- [./runbooks/ecr_image_push.md](./runbooks/ecr_image_push.md)
- [./runbooks/frontend_static_hosting.md](./runbooks/frontend_static_hosting.md)
- [./runbooks/stage_11_test_environment.md](./runbooks/stage_11_test_environment.md)

### Local-only scripts

- [./scripts/validate_deployment_templates.py](./scripts/validate_deployment_templates.py)
- [./scripts/render_backend_task_definition.py](./scripts/render_backend_task_definition.py)

## Як це використовувати у Stage 11

1. Підтвердити naming convention і environment scope.
2. Створити Parameter Store parameters вручну за [./parameter_store.md](./parameter_store.md).
3. Підготувати локальний values JSON для render script без commit-у секретів.
4. Прогнати validator:

   ```bash
   python deployment/aws/scripts/validate_deployment_templates.py
   ```

5. Зрендерити task definition локально:

   ```bash
   python deployment/aws/scripts/render_backend_task_definition.py \
     --values-file /absolute/path/to/local-values.json
   ```

6. Використати runbooks як manual Stage 11 checklist; live AWS commands мають
   виконуватись окремо, не в межах цього slice.

## Placeholder format

JSON templates використовують `{{PLACEHOLDER_NAME}}`.

Важливо:

- placeholders не є live values
- у repo немає real AWS account id, real ARN або real secrets
- render script не викликає AWS API і не потребує AWS credentials

## Relationship with Stage 10A

Backend image source для ECS task definition already зафіксований у:

- [../../Dockerfile.backend](../../Dockerfile.backend)
- [../../docs/backend_containerization.md](../../docs/backend_containerization.md)
