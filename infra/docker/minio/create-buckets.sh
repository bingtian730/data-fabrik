#!/bin/sh
set -eu

mc alias set local "http://minio:9000" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

for bucket in datafabrik-raw datafabrik-clean; do
  if ! mc ls "local/${bucket}" >/dev/null 2>&1; then
    mc mb "local/${bucket}"
    echo "Created bucket: ${bucket}"
  else
    echo "Bucket already exists: ${bucket}"
  fi
done

echo "MinIO bucket initialization complete."
