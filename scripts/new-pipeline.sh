#!/usr/bin/env bash
# Scaffold a new DataFabrik pipeline — YAML config + dbt model stubs.
set -euo pipefail

cd "$(dirname "$0")/.."

# ── colours ────────────────────────────────────────────────────────────────
bold()  { printf "\033[1m%s\033[0m" "$1"; }
cyan()  { printf "\033[36m%s\033[0m" "$1"; }
green() { printf "\033[32m%s\033[0m" "$1"; }
yellow(){ printf "\033[33m%s\033[0m" "$1"; }
ask()   { printf "\n$(cyan '?') $(bold "$1") "; }

# ── helpers ─────────────────────────────────────────────────────────────────
require() {
  [[ -n "$1" ]] || { echo "  value cannot be empty"; exit 1; }
}

pick() {
  # pick <prompt> <opt1> <opt2> ...  → prints chosen value
  local prompt="$1"; shift
  local opts=("$@")
  printf "\n$(cyan '?') $(bold "$prompt")\n"
  local i=1
  for o in "${opts[@]}"; do printf "  %d) %s\n" "$i" "$o"; ((i++)); done
  while true; do
    printf "  choice [1-%d]: " "${#opts[@]}"
    read -r idx
    if [[ "$idx" =~ ^[0-9]+$ ]] && (( idx >= 1 && idx <= ${#opts[@]} )); then
      echo "${opts[$((idx-1))]}"
      return
    fi
    echo "  invalid — enter a number between 1 and ${#opts[@]}"
  done
}

# ── gather inputs ────────────────────────────────────────────────────────────
echo
echo "$(bold 'DataFabrik — new pipeline scaffold')"
echo "────────────────────────────────────────"

ask "Pipeline name (alphanumeric / _ / -, e.g. salesforce_opportunities):"
read -r PIPELINE_ID
require "$PIPELINE_ID"
[[ "$PIPELINE_ID" =~ ^[a-z0-9_-]+$ ]] || {
  echo "  pipeline_id must be lowercase alphanumeric / _ / -"; exit 1
}

ask "Short description:"
read -r DESCRIPTION
require "$DESCRIPTION"

ask "Owner (default: data-platform):"
read -r OWNER
OWNER="${OWNER:-data-platform}"

SOURCE_TYPE=$(pick "Ingestion source type" "s3_csv" "http_api" "jdbc")
SCHEDULE=$(pick "Schedule" "@daily" "@hourly" "@weekly" "custom cron")
if [[ "$SCHEDULE" == "custom cron" ]]; then
  ask "Cron expression (e.g. 0 6 * * *):"
  read -r CRON_EXPR
  require "$CRON_EXPR"
  SCHEDULE_BLOCK="  cron: \"${CRON_EXPR}\""
else
  SCHEDULE_BLOCK="  preset: \"${SCHEDULE}\""
fi

TRANSFORM_TYPE=$(pick "Transformation type" "dbt" "sql" "none")
DELIVER_TYPE=$(pick "Delivery destination" "s3_publish" "slack_notify" "webhook" "none")

# ── derive paths ─────────────────────────────────────────────────────────────
YAML_PATH="orchestration/airflow/configs/pipelines/${PIPELINE_ID}.yaml"
DBT_DIR="dbt/datafabrik_models/models/${PIPELINE_ID}"

# ── guard against overwrite ───────────────────────────────────────────────────
if [[ -f "$YAML_PATH" ]]; then
  echo; echo "  $(yellow 'warning:') $YAML_PATH already exists — aborting to avoid overwrite."
  exit 1
fi

# ── build ingestion block ─────────────────────────────────────────────────────
case "$SOURCE_TYPE" in
  s3_csv)
    INGESTION_BLOCK="  ingestion:
    type: s3_csv
    source_bucket: customer-landing
    source_key: ${PIPELINE_ID}/*.csv
    dest_bucket: datafabrik-raw
    dest_prefix: ${PIPELINE_ID}/{{ ds }}/"
    ;;
  http_api)
    INGESTION_BLOCK="  ingestion:
    type: http_api
    url: https://api.example.com/${PIPELINE_ID}
    method: GET
    headers: {}
    dest_bucket: datafabrik-raw
    dest_key: ${PIPELINE_ID}/{{ ds }}/data.json"
    ;;
  jdbc)
    ask "Source table (e.g. public.orders):"
    read -r JDBC_TABLE
    require "$JDBC_TABLE"
    ask "Watermark column (default: updated_at):"
    read -r WM_COL
    WM_COL="${WM_COL:-updated_at}"
    INGESTION_BLOCK="  ingestion:
    type: jdbc
    connection_id: postgres_default
    table: ${JDBC_TABLE}
    watermark_column: ${WM_COL}
    watermark_init: \"1970-01-01 00:00:00\"
    dest_bucket: datafabrik-raw
    dest_prefix: ${PIPELINE_ID}/"
    ;;
esac

# ── build transformation block ────────────────────────────────────────────────
case "$TRANSFORM_TYPE" in
  dbt)
    TRANSFORM_BLOCK="
  transformation:
    type: dbt
    project_dir: /usr/app/dbt
    profiles_dir: /usr/app/dbt
    select: ${PIPELINE_ID}
    target: dev"
    ;;
  sql)
    TRANSFORM_BLOCK="
  transformation:
    type: sql
    connection_id: postgres_default
    sql: \"SELECT 1\"  # TODO: replace with real SQL"
    ;;
  none) TRANSFORM_BLOCK="" ;;
esac

# ── build validation block ────────────────────────────────────────────────────
if [[ "$TRANSFORM_TYPE" == "dbt" ]]; then
  VALIDATION_BLOCK="
  validation:
    - type: row_count
      connection_id: postgres_default
      table: analytics.stg_${PIPELINE_ID}
      min_rows: 1"
else
  VALIDATION_BLOCK=""
fi

# ── build delivery block ──────────────────────────────────────────────────────
case "$DELIVER_TYPE" in
  s3_publish)
    DELIVERY_BLOCK="
  delivery:
    type: s3_publish
    source_bucket: datafabrik-raw
    source_prefix: ${PIPELINE_ID}/{{ ds }}/
    dest_bucket: datafabrik-curated
    dest_prefix: ${PIPELINE_ID}/{{ ds }}/"
    ;;
  slack_notify)
    ask "Slack channel (e.g. #data-alerts):"
    read -r SLACK_CHANNEL
    require "$SLACK_CHANNEL"
    DELIVERY_BLOCK="
  delivery:
    type: slack_notify
    connection_id: slack_default
    channel: \"${SLACK_CHANNEL}\"
    message: \"${PIPELINE_ID} finished for {{ ds }}\""
    ;;
  webhook)
    ask "Webhook URL:"
    read -r WEBHOOK_URL
    require "$WEBHOOK_URL"
    DELIVERY_BLOCK="
  delivery:
    type: webhook
    url: ${WEBHOOK_URL}
    method: POST
    payload:
      pipeline: ${PIPELINE_ID}
      ds: \"{{ ds }}\""
    ;;
  none) DELIVERY_BLOCK="" ;;
esac

# ── write YAML ────────────────────────────────────────────────────────────────
mkdir -p "$(dirname "$YAML_PATH")"
cat > "$YAML_PATH" <<YAML
pipeline_id: ${PIPELINE_ID}
description: ${DESCRIPTION}
owner: ${OWNER}
tags:
  - ${PIPELINE_ID}

schedule:
${SCHEDULE_BLOCK}
  timezone: UTC
  start_date: $(date +%Y)-01-01T00:00:00
  catchup: false
  max_active_runs: 1
  retries: 1
  retry_delay_minutes: 5

stages:
${INGESTION_BLOCK}${TRANSFORM_BLOCK}${VALIDATION_BLOCK}${DELIVERY_BLOCK}
YAML

echo
echo "  $(green 'created') $YAML_PATH"

# ── write dbt stubs ───────────────────────────────────────────────────────────
if [[ "$TRANSFORM_TYPE" == "dbt" ]]; then
  mkdir -p "$DBT_DIR"

  STG_PATH="${DBT_DIR}/stg_${PIPELINE_ID}.sql"
  SUMMARY_PATH="${DBT_DIR}/${PIPELINE_ID}_summary.sql"

  cat > "$STG_PATH" <<SQL
-- TODO: replace VALUES with a real source reference once ingestion loads to Postgres
SELECT
    id,
    created_at::DATE AS event_date,
    updated_at
FROM (VALUES
    (1, NOW(), NOW())
) AS t(id, created_at, updated_at)
SQL

  cat > "$SUMMARY_PATH" <<SQL
SELECT
    event_date,
    COUNT(*) AS total_records
FROM {{ ref('stg_${PIPELINE_ID}') }}
GROUP BY event_date
ORDER BY event_date
SQL

  echo "  $(green 'created') $STG_PATH"
  echo "  $(green 'created') $SUMMARY_PATH"
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo
echo "$(bold 'Next steps:')"
if [[ "$TRANSFORM_TYPE" == "dbt" ]]; then
  echo "  1. Edit the dbt models in $(cyan "$DBT_DIR/")"
  echo "  2. Test locally:  docker compose exec dbt dbt run --select ${PIPELINE_ID}"
fi
echo "  $(( [[ "$TRANSFORM_TYPE" == "dbt" ]] && echo 3 || echo 1 )). Upload test data: $(cyan "./scripts/upload-data.sh <file> <bucket>/<prefix>/")"
echo "  $(( [[ "$TRANSFORM_TYPE" == "dbt" ]] && echo 4 || echo 2 )). Run the pipeline: $(cyan "./scripts/run-pipeline.sh ${PIPELINE_ID}")"
echo
