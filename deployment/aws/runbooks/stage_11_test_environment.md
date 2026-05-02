# Stage 11 Test Environment Draft

Це manual checklist draft для Stage 11 AWS test environment provisioning.
Він не означає, що якісь ресурси вже створені.

## Provisioning order

1. Confirm AWS account, selected region, MFA posture and billing alerts.
2. Confirm environment naming convention:
   - `uppi-test-*`
3. Create or choose VPC, public/private subnets, route tables and NAT strategy.
4. Create security groups:
   - ALB
   - ECS tasks
   - RDS PostgreSQL
5. Create S3 buckets:
   - frontend bucket
   - visure bucket
   - attestazioni bucket
6. Create Systems Manager Parameter Store parameters using:
   - [../parameter_store.md](../parameter_store.md)
7. Create RDS PostgreSQL instance and subnet group.
8. Create or confirm ECR repository.
9. Create IAM roles and attach:
   - [../iam_policies/ecs_task_execution_policy.template.json](../iam_policies/ecs_task_execution_policy.template.json)
   - [../iam_policies/uppi_backend_task_policy.template.json](../iam_policies/uppi_backend_task_policy.template.json)
10. Build and push backend image using:
    - [./ecr_image_push.md](./ecr_image_push.md)
11. Render and register ECS task definition from:
    - [../ecs_task_definition.backend.template.json](../ecs_task_definition.backend.template.json)
12. Create ECS service behind ALB.
13. Build frontend and upload `frontend/dist/` via:
    - [./frontend_static_hosting.md](./frontend_static_hosting.md)
14. Configure ACM / Route 53 if test domain is ready.
15. Run smoke tests:
    - backend `/health/live`
    - backend `/health/ready`
    - web login/session
    - one safe operator-facing web flow
16. Record rollback notes and cleanup steps.

## Stage 11 cautions

- do not share `state.json` between ECS tasks or sessions
- keep writable local artifact dirs task-local and ephemeral
- do not mix provisioning with runtime refactor
- do not treat this draft as proof that infrastructure already exists
