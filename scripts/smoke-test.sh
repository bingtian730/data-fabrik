#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

pass() { printf "  \033[32mPASS\033[0m  %s\n" "$1"; }
fail() { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; exit 1; }

echo "Postgres"
docker compose exec -T postgres pg_isready -U postgres >/dev/null && pass "pg_isready" || fail "postgres not ready"

echo "MinIO buckets"
buckets=$(docker compose run --rm --no-deps --entrypoint sh \
  -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD \
  minio-init -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls local/')
echo "$buckets" | grep -q datafabrik-raw && pass "datafabrik-raw bucket present" || fail "datafabrik-raw bucket missing"
echo "$buckets" | grep -q datafabrik-staging && pass "datafabrik-staging bucket present" || fail "datafabrik-staging bucket missing"
echo "$buckets" | grep -q datafabrik-curated && pass "datafabrik-curated bucket present" || fail "datafabrik-curated bucket missing"

echo "Airflow webserver"
curl -fsS http://localhost:8080/health >/dev/null && pass "GET /health" || fail "airflow webserver unreachable"

echo "Presto coordinator"
curl -fsS http://localhost:8081/v1/info >/dev/null && pass "GET /v1/info" || fail "presto unreachable"

echo "Presto query"
docker compose exec -T presto presto-cli --server localhost:8080 \
  --catalog tpch --schema tiny --execute 'SELECT count(*) FROM nation' \
  | grep -q '"25"' && pass "SELECT count(*) FROM tpch.tiny.nation = 25" || fail "presto query failed"

echo "FastAPI"
curl -fsS http://localhost:8000/health >/dev/null && pass "GET /health" || fail "fastapi unreachable"

echo
echo "All checks passed."
