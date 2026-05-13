#!/usr/bin/env bash
# Upload a local file (or directory) to a MinIO/S3 bucket path.
# Usage: ./scripts/upload-data.sh <local-path> <bucket>/<prefix>/
set -euo pipefail

cd "$(dirname "$0")/.."

# ── colours ────────────────────────────────────────────────────────────────
bold()  { printf "\033[1m%s\033[0m" "$1"; }
cyan()  { printf "\033[36m%s\033[0m" "$1"; }
green() { printf "\033[32m%s\033[0m" "$1"; }
red()   { printf "\033[31m%s\033[0m" "$1"; }
yellow(){ printf "\033[33m%s\033[0m" "$1"; }

# ── usage ────────────────────────────────────────────────────────────────────
usage() {
  echo
  echo "  Usage: $(bold './scripts/upload-data.sh') <local-path> <bucket>/<prefix>/"
  echo
  echo "  Examples:"
  echo "    $(cyan './scripts/upload-data.sh data/stripe/charges.csv customer-landing/stripe/')"
  echo "    $(cyan './scripts/upload-data.sh data/acme/          customer-landing/acme/')"
  echo
  echo "  Environment (optional — defaults to local MinIO):"
  echo "    MINIO_ENDPOINT   endpoint URL  (default: http://localhost:9000)"
  echo "    MINIO_USER       access key    (default: minioadmin)"
  echo "    MINIO_PASSWORD   secret key    (default: minioadmin)"
  echo
  exit 1
}

[[ $# -lt 2 ]] && usage

LOCAL_PATH="$1"
DEST="$2"

# ── split bucket / key-prefix ────────────────────────────────────────────────
BUCKET="${DEST%%/*}"
PREFIX="${DEST#*/}"
[[ -z "$BUCKET" ]] && { echo "  $(red 'error:') destination must be <bucket>/<prefix>/"; exit 1; }

# ── config ───────────────────────────────────────────────────────────────────
ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
AWS_KEY="${MINIO_USER:-minioadmin}"
AWS_SECRET="${MINIO_PASSWORD:-minioadmin}"

# ── resolve upload tool ───────────────────────────────────────────────────────
# prefer aws-cli if present (works against any S3-compatible endpoint);
# fall back to MinIO mc client inside the running minio-init container.
if command -v aws &>/dev/null; then
  UPLOAD_VIA="aws-cli"
elif docker ps --format '{{.Names}}' 2>/dev/null | grep -q "datafabrik-minio-init\|datafabrik-minio$"; then
  UPLOAD_VIA="mc-docker"
else
  echo "  $(red 'error:') neither 'aws' CLI nor a running MinIO container found."
  echo "  Install awscli (pip install awscli) or start the stack with docker compose up -d."
  exit 1
fi

echo
echo "$(bold 'DataFabrik — upload data')"
echo "────────────────────────────────────────"
echo "  source    : $(cyan "$LOCAL_PATH")"
echo "  bucket    : $(cyan "$BUCKET")"
echo "  prefix    : $(cyan "$PREFIX")"
echo "  endpoint  : $(cyan "$ENDPOINT")"
echo "  via       : $UPLOAD_VIA"
echo

[[ -e "$LOCAL_PATH" ]] || { echo "  $(red 'error:') path not found: $LOCAL_PATH"; exit 1; }

# ── upload ────────────────────────────────────────────────────────────────────
upload_via_awscli() {
  export AWS_ACCESS_KEY_ID="$AWS_KEY"
  export AWS_SECRET_ACCESS_KEY="$AWS_SECRET"
  export AWS_DEFAULT_REGION="us-east-1"
  if [[ -d "$LOCAL_PATH" ]]; then
    aws --endpoint-url "$ENDPOINT" s3 cp "$LOCAL_PATH" "s3://${BUCKET}/${PREFIX}" \
      --recursive --no-progress
  else
    aws --endpoint-url "$ENDPOINT" s3 cp "$LOCAL_PATH" "s3://${BUCKET}/${PREFIX}" \
      --no-progress
  fi
}

upload_via_mc() {
  # Use mc inside the running MinIO container (no extra install required).
  ALIAS="local"
  MC="docker exec datafabrik-minio-init mc"

  # Register alias if not already present (idempotent).
  $MC alias set "$ALIAS" "$ENDPOINT" "$AWS_KEY" "$AWS_SECRET" --insecure &>/dev/null || true

  if [[ -d "$LOCAL_PATH" ]]; then
    # Resolve absolute path so Docker can see it (bind-mount not needed — mc
    # runs inside the container, so we copy the file in via stdin for single
    # files; for directories we stream each file).
    while IFS= read -r -d '' file; do
      rel="${file#"$LOCAL_PATH"/}"
      dest_key="${PREFIX}${rel}"
      docker exec -i datafabrik-minio-init mc pipe "${ALIAS}/${BUCKET}/${dest_key}" < "$file"
      echo "  $(green 'uploaded') $file → s3://${BUCKET}/${dest_key}"
    done < <(find "$LOCAL_PATH" -type f -print0)
  else
    filename="$(basename "$LOCAL_PATH")"
    dest_key="${PREFIX}${filename}"
    docker exec -i datafabrik-minio-init mc pipe "${ALIAS}/${BUCKET}/${dest_key}" < "$LOCAL_PATH"
    echo "  $(green 'uploaded') $LOCAL_PATH → s3://${BUCKET}/${dest_key}"
  fi
}

case "$UPLOAD_VIA" in
  aws-cli)   upload_via_awscli ;;
  mc-docker) upload_via_mc     ;;
esac

echo
echo "  $(green 'done') — files are in $(cyan "s3://${BUCKET}/${PREFIX}")"
echo
echo "$(bold 'Next steps:')"
echo "  Trigger the pipeline:  $(cyan "./scripts/run-pipeline.sh <pipeline_id>")"
echo
