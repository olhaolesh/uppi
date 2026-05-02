#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${IMAGE_TAG:-uppi-backend:local}"
HOST_PORT="${HOST_PORT:-18000}"
CONTAINER_NAME="uppi-backend-smoke-${RANDOM}${RANDOM}"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker build -f Dockerfile.backend -t "${IMAGE_TAG}" .

docker run -d \
  --name "${CONTAINER_NAME}" \
  -p "${HOST_PORT}:8000" \
  -e UPPI_WEB_ENV=local \
  -e UPPI_WEB_SESSION_SECRET=local-dev-session-secret \
  -e UPPI_WEB_AUTH_USERNAME=operator \
  -e UPPI_WEB_AUTH_PASSWORD=operator-password \
  -e UPPI_WEB_AUTH_PIN=123456 \
  "${IMAGE_TAG}" >/dev/null

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${HOST_PORT}/health/live" >/dev/null \
    && curl -fsS "http://127.0.0.1:${HOST_PORT}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done

curl -fsS "http://127.0.0.1:${HOST_PORT}/health/live"
echo
curl -fsS "http://127.0.0.1:${HOST_PORT}/health/ready"
echo

container_logs="$(docker logs "${CONTAINER_NAME}" 2>&1 || true)"
if printf '%s' "${container_logs}" | rg -q 'local-dev-session-secret|operator-password|123456'; then
  echo "Container logs exposed a smoke secret placeholder."
  exit 1
fi

echo "Smoke passed for ${IMAGE_TAG}"
