#!/usr/bin/env bash
# Trigger an Airflow DAG run and poll until it finishes, then print a summary.
# Usage: ./scripts/run-pipeline.sh <pipeline_id> [logical_date]
set -euo pipefail

cd "$(dirname "$0")/.."

# ── colours ────────────────────────────────────────────────────────────────
bold()  { printf "\033[1m%s\033[0m" "$1"; }
cyan()  { printf "\033[36m%s\033[0m" "$1"; }
green() { printf "\033[32m%s\033[0m" "$1"; }
red()   { printf "\033[31m%s\033[0m" "$1"; }
yellow(){ printf "\033[33m%s\033[0m" "$1"; }

# ── usage ─────────────────────────────────────────────────────────────────────
usage() {
  echo
  echo "  Usage: $(bold './scripts/run-pipeline.sh') <pipeline_id> [logical_date]"
  echo
  echo "  Examples:"
  echo "    $(cyan './scripts/run-pipeline.sh stripe_daily')"
  echo "    $(cyan './scripts/run-pipeline.sh stripe_daily 2026-05-01')"
  echo
  echo "  Environment (optional):"
  echo "    AIRFLOW_URL       base URL       (default: http://localhost:8080)"
  echo "    AIRFLOW_USER      username       (default: admin)"
  echo "    AIRFLOW_PASSWORD  password       (default: admin)"
  echo "    POLL_INTERVAL     seconds        (default: 5)"
  echo "    POLL_TIMEOUT      max wait secs  (default: 600)"
  echo
  exit 1
}

[[ $# -lt 1 ]] && usage

PIPELINE_ID="$1"
LOGICAL_DATE="${2:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
POLL_TIMEOUT="${POLL_TIMEOUT:-600}"

AUTH=(-u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}")
API="${AIRFLOW_URL}/api/v1"

# ── helpers ───────────────────────────────────────────────────────────────────
airflow_get() {
  curl -sSf "${AUTH[@]}" -H "Content-Type: application/json" "$@"
}

airflow_post() {
  curl -sSf "${AUTH[@]}" -H "Content-Type: application/json" -X POST "$@"
}

check_prereqs() {
  command -v curl &>/dev/null || { echo "  $(red 'error:') curl is required"; exit 1; }
  command -v jq   &>/dev/null || { echo "  $(red 'error:') jq is required (brew install jq)"; exit 1; }
}

check_airflow() {
  local health
  health=$(curl -sf "${AIRFLOW_URL}/api/v1/health" 2>/dev/null || true)
  if [[ -z "$health" ]]; then
    echo "  $(red 'error:') Airflow is not reachable at ${AIRFLOW_URL}"
    echo "  Start the stack:  docker compose up -d"
    exit 1
  fi
}

check_dag_exists() {
  local code
  code=$(curl -so /dev/null -w "%{http_code}" "${AUTH[@]}" "${API}/dags/${PIPELINE_ID}" 2>/dev/null)
  if [[ "$code" != "200" ]]; then
    echo "  $(red 'error:') DAG '${PIPELINE_ID}' not found in Airflow (HTTP $code)"
    echo "  Is the YAML config at orchestration/airflow/configs/pipelines/${PIPELINE_ID}.yaml?"
    exit 1
  fi
}

# ── main ──────────────────────────────────────────────────────────────────────
check_prereqs
check_airflow

echo
echo "$(bold 'DataFabrik — run pipeline')"
echo "────────────────────────────────────────"
echo "  pipeline  : $(cyan "$PIPELINE_ID")"
echo "  date      : $(cyan "$LOGICAL_DATE")"
echo "  airflow   : $(cyan "$AIRFLOW_URL")"
echo

check_dag_exists

# Unpause the DAG in case it was paused.
airflow_post "${API}/dags/${PIPELINE_ID}" \
  -d '{"is_paused": false}' &>/dev/null

# Trigger a new DAG run.
TRIGGER_RESP=$(airflow_post "${API}/dags/${PIPELINE_ID}/dagRuns" \
  -d "{\"logical_date\": \"${LOGICAL_DATE}\", \"conf\": {}}")
RUN_ID=$(echo "$TRIGGER_RESP" | jq -r '.dag_run_id')

if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
  echo "  $(red 'error:') failed to trigger DAG run"
  echo "$TRIGGER_RESP" | jq . 2>/dev/null || echo "$TRIGGER_RESP"
  exit 1
fi

echo "  $(green 'triggered') run_id=$(cyan "$RUN_ID")"
echo

# ── poll for completion ────────────────────────────────────────────────────────
ELAPSED=0
SPINNER=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
SP_IDX=0
LAST_STATE=""

while true; do
  RUN_INFO=$(airflow_get "${API}/dags/${PIPELINE_ID}/dagRuns/${RUN_ID}")
  STATE=$(echo "$RUN_INFO" | jq -r '.state')

  if [[ "$STATE" != "$LAST_STATE" ]]; then
    printf "\r  state     : $(cyan "$STATE")                \n"
    LAST_STATE="$STATE"
  fi

  case "$STATE" in
    success|failed|upstream_failed)
      break
      ;;
  esac

  if (( ELAPSED >= POLL_TIMEOUT )); then
    echo "  $(yellow 'warning:') timed out after ${POLL_TIMEOUT}s — last state: $STATE"
    echo "  Check the Airflow UI: $(cyan "${AIRFLOW_URL}/dags/${PIPELINE_ID}")"
    exit 1
  fi

  printf "\r  waiting   : %s %ds " "${SPINNER[$SP_IDX]}" "$ELAPSED"
  SP_IDX=$(( (SP_IDX + 1) % ${#SPINNER[@]} ))
  sleep "$POLL_INTERVAL"
  ELAPSED=$(( ELAPSED + POLL_INTERVAL ))
done

printf "\r                                          \r"

# ── task summary ──────────────────────────────────────────────────────────────
echo "$(bold 'Task summary')"
echo "────────────────────────────────────────"

TASKS=$(airflow_get "${API}/dags/${PIPELINE_ID}/dagRuns/${RUN_ID}/taskInstances")
TASK_LIST=$(echo "$TASKS" | jq -r '.task_instances[] | [.task_id, .state, (.duration // 0 | round | tostring) + "s"] | @tsv')

OK=0
FAIL=0
while IFS=$'\t' read -r task_id task_state duration; do
  case "$task_state" in
    success)
      printf "  $(green '✓') %-35s %s  (%s)\n" "$task_id" "$task_state" "$duration"
      (( OK++ ))
      ;;
    failed|upstream_failed)
      printf "  $(red '✗') %-35s %s\n" "$task_id" "$task_state"
      (( FAIL++ ))
      ;;
    *)
      printf "  $(yellow '~') %-35s %s\n" "$task_id" "$task_state"
      ;;
  esac
done <<< "$TASK_LIST"

echo

if [[ "$STATE" == "success" ]]; then
  echo "  $(green "Pipeline '${PIPELINE_ID}' completed successfully") ($OK task(s) passed)"
  echo
  echo "$(bold 'Check results:')"
  echo "  Airflow UI  : $(cyan "${AIRFLOW_URL}/dags/${PIPELINE_ID}/grid")"
  echo "  MinIO UI    : $(cyan "http://localhost:9001")"
  echo "  Postgres    : psql -h localhost -U datafabrik -d datafabrik -c \"SELECT * FROM pipeline_metadata.ingestion_log WHERE pipeline_id = '${PIPELINE_ID}' ORDER BY extracted_at DESC LIMIT 5;\""
  echo
  exit 0
else
  echo "  $(red "Pipeline '${PIPELINE_ID}' FAILED") ($FAIL task(s) failed)"
  echo
  echo "$(bold 'Debug:')"
  echo "  Airflow UI  : $(cyan "${AIRFLOW_URL}/dags/${PIPELINE_ID}/grid")"
  echo "  Logs        : docker compose logs --tail=100 airflow-worker"
  echo
  exit 1
fi
