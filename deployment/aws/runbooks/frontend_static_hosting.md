# Frontend Static Hosting Runbook

Цей runbook описує Stage 11 manual approach для frontend static hosting.
Stage 10B не робить upload і не створює AWS resources.

## Intended shape

- build `frontend/` locally or in CI
- upload `frontend/dist/` to S3 frontend bucket
- serve frontend through CloudFront
- use ACM certificate and Route 53 later, якщо домен already ready

## Build step

```bash
cd frontend
VITE_UPPI_API_BASE_URL=https://api.test.<DOMAIN> pnpm build
```

## Upload approach

Recommended Stage 11 manual pattern:

1. create or choose frontend bucket, e.g. `uppi-test-frontend`
2. keep bucket private
3. front it with CloudFront
4. upload `frontend/dist/` contents to bucket
5. invalidate CloudFront after upload

Example commands for Stage 11 only:

```bash
aws s3 sync frontend/dist/ s3://uppi-test-frontend/ --delete

aws cloudfront create-invalidation \
  --distribution-id <DISTRIBUTION_ID> \
  --paths "/*"
```

## Notes

- `VITE_UPPI_API_BASE_URL` is build-time config
- do not point test frontend at a production API URL
- do not make the S3 bucket public if CloudFront is used
- no frontend AWS upload script is committed in Stage 10B
