#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/app"
WORKSPACE_ROOT="${UPPI_WORKSPACE_ROOT:-$APP_ROOT}"

workspace_dirs=(
  "downloads"
  "captcha_images"
  "logs"
)

repo_local_dirs=(
  "clients"
  "clients/web_prepare"
  "clients/web_generation"
  "clients/web_bulk_import"
  "clients/web_jobs"
)

for relative_dir in "${workspace_dirs[@]}"; do
  mkdir -p "${WORKSPACE_ROOT}/${relative_dir}"
done

for relative_dir in "${repo_local_dirs[@]}"; do
  mkdir -p "${APP_ROOT}/${relative_dir}"
done

exec uvicorn uppi.web.app:app --host 0.0.0.0 --port 8000
