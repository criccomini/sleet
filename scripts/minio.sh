#!/bin/sh
# Start a MinIO container for the S3 semantics test and create its
# bucket via the S3 API. Replaces any previous instance. The test
# reads the endpoint from SLEET_S3_ENDPOINT:
#
#   scripts/minio.sh [port]        # default port 9000
#   SLEET_S3_ENDPOINT=http://127.0.0.1:9000 cargo test --test s3
set -eu

PORT="${1:-9000}"
IMAGE="minio/minio:RELEASE.2025-09-07T16-13-09Z"
NAME="sleet-minio"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p "127.0.0.1:$PORT:9000" "$IMAGE" \
  server /data >/dev/null

ready=""
for _ in $(seq 1 150); do
  if curl -sf "http://127.0.0.1:$PORT/minio/health/ready" >/dev/null; then
    ready=1
    break
  fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "minio never became ready" >&2; exit 1; }

docker exec "$NAME" sh -c '
  mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null
  mc mb --ignore-existing local/sleet >/dev/null
'

echo "SLEET_S3_ENDPOINT=http://127.0.0.1:$PORT"
