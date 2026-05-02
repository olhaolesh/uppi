# ECR Image Push Runbook

Цей runbook описує Stage 11 manual flow для push backend image в ECR.
Команди нижче є прикладами і не виконуються в Stage 10B.

## Передумови

- Docker image already builds locally from `Dockerfile.backend`
- AWS account, region, IAM access і ECR repository decision already confirmed
- repository name узгоджений з [../naming.md](../naming.md)

## Example command flow

```bash
aws ecr get-login-password --region <REGION> \
  | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com

aws ecr create-repository --repository-name uppi-backend-test

docker build -f Dockerfile.backend -t uppi-backend:local .

docker tag uppi-backend:local \
  <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/uppi-backend-test:<TAG>

docker push \
  <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/uppi-backend-test:<TAG>
```

## Notes

- `<ACCOUNT_ID>`, `<REGION>` і `<TAG>` лишаються placeholders
- repo не містить real account id або credentials
- якщо repository already exists, `create-repository` крок пропускається
- tag convention бажано робити детермінованим:
  - git SHA
  - або `test-YYYYMMDD-HHMM`

## After push

1. Зафіксувати final image URI.
2. Підставити його у local values JSON для:
   - [../ecs_task_definition.backend.template.json](../ecs_task_definition.backend.template.json)
3. Зареєструвати task definition у Stage 11 manual provisioning flow.
