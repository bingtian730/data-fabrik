#!/usr/bin/env bash
# Print a colour-coded health summary of all DataFabrik pipelines.
# Combines Airflow REST API (run state/duration) with pipeline_metadata.ingestion_log (rows, watermark).
set -euo pipefail

cd "$(dirname "$0")/.."

# ── colours ───────────────────────────────────────────────────────────────────
bold()  { printf "\033[1m%s\033[0m"  "$1"; }
cyan()  { printf "\033[36m%s\033[0m" "$1"; }
green() { printf "\033[32m%s\033[0m" "$1"; }
red()   { printf "\033[31m%s\033[0m" "$1"; }
yellow(){ printf "\033[33m%s\033[0m" "$1"; }
dim()   { printf "\033[2m%s\033[0m"  "$1"; }

# ── config (override via env) ─────────────────────────────────────────────────
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"
PG_CONTAINER="${PG_CONTAINER:-datafabrik-postgres}"
PG_USER="${PG_USER:-datafabrik}"
PG_DB="${PG_DB:-datafabrik}"

# ── prereqs ───────────────────────────────────────────────────────────────────
for cmd in curl jq python3; do
  command -v "$cmd" &>/dev/null || { echo "  $(red 'error:') '$cmd' not found"; exit 1; }
done

airflow_get() { curl -sSf -u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" "$@"; }

# ── check connectivity ────────────────────────────────────────────────────────
if ! airflow_get "${AIRFLOW_URL}/health" &>/dev/null; then
  echo "  $(red 'error:') Airflow unreachable at ${AIRFLOW_URL} — is the stack running?"
  echo "  Run: docker compose up -d"
  exit 1
fi

# ── fetch DAG list ────────────────────────────────────────────────────────────
DAGS_JSON=$(airflow_get "${AIRFLOW_URL}/api/v1/dags?limit=100")
DAG_IDS=$(echo "$DAGS_JSON" | jq -r '.dags[].dag_id')
PAUSED_IDS=$(echo "$DAGS_JSON" | jq -r '.dags[] | select(.is_paused) | .dag_id')

# ── fetch ingestion_log into a temp file (pipeline_id|rows|duration|status|watermark) ──
IL_TMPFILE=$(mktemp)
trap 'rm -f "$IL_TMPFILE"' EXIT
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${PG_CONTAINER}$"; then
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A -F'|' -c "
    SELECT pipeline_id,
           rows_extracted,
           ROUND(duration_seconds::numeric, 1),
           status,
           COALESCE(watermark_to::text, '-')
    FROM pipeline_metadata.ingestion_log
    WHERE (pipeline_id, extracted_at) IN (
      SELECT pipeline_id, MAX(extracted_at)
      FROM pipeline_metadata.ingestion_log
      GROUP BY pipeline_id
    )
    ORDER BY pipeline_id;
  " 2>/dev/null > "$IL_TMPFILE" || true
fi

# look up a column (1-based) for a given pipeline_id from the temp file
il_get() {
  local pid="$1" col="$2"
  { grep "^${pid}|" "$IL_TMPFILE" 2>/dev/null || true; } | cut -d'|' -f"$col" | tr -d '[:space:]'
}

# ── duration helper (works on macOS + Linux) ──────────────────────────────────
calc_duration() {
  local start="$1" end="$2"
  [[ "$start" == "null" || "$end" == "null" ]] && { echo "—"; return; }
  python3 -c "
from datetime import datetime, timezone
s = datetime.fromisoformat('${start}'[:19])
e = datetime.fromisoformat('${end}'[:19])
d = int((e - s).total_seconds())
print(f'{d}s')
" 2>/dev/null || echo "—"
}

format_date() {
  local dt="$1"
  [[ "$dt" == "null" || -z "$dt" ]] && { echo "—"; return; }
  echo "${dt:0:16}" | tr 'T' ' '
}

# ── print header ──────────────────────────────────────────────────────────────
echo
echo "$(bold 'DataFabrik — pipeline health')"
echo "$(dim "$(date -u '+%Y-%m-%d %H:%M UTC')")"
echo
printf "$(bold '%-32s') %-10s %-18s %-10s %-8s %s\n" \
  "PIPELINE" "STATE" "LAST RUN (UTC)" "DURATION" "ROWS" "WATERMARK"
printf '%0.s─' {1..90}; echo

# ── per-DAG row ────────────────────────────────────────────────────────────────
TOTAL=0 OK=0 FAILED=0 PAUSED_COUNT=0

while IFS= read -r dag_id; do
  TOTAL=$(( TOTAL + 1 ))

  # check paused
  if echo "$PAUSED_IDS" | grep -qx "$dag_id"; then
    PAUSED_COUNT=$(( PAUSED_COUNT + 1 ))
    state_str="$(yellow '⏸ paused')"
    printf "%-41s %-19s %-18s %-10s %-8s %s\n" \
      "$dag_id" "$state_str" "—" "—" "—" "—"
    continue
  fi

  # latest run
  RUN=$(airflow_get "${AIRFLOW_URL}/api/v1/dags/${dag_id}/dagRuns?limit=1&order_by=-start_date" 2>/dev/null \
        | jq '.dag_runs[0] // {}')
  STATE=$(echo "$RUN" | jq -r '.state // "no runs"')
  START=$(echo "$RUN" | jq -r '.start_date // "null"')
  END=$(echo "$RUN"   | jq -r '.end_date // "null"')

  LAST_RUN=$(format_date "$START")
  DURATION=$(calc_duration "$START" "$END")

  # state badge
  case "$STATE" in
    success)  state_str="$(green '✓ success')"; OK=$(( OK + 1 )) ;;
    failed)   state_str="$(red   '✗ failed')";  FAILED=$(( FAILED + 1 )) ;;
    running)  state_str="$(cyan  '↻ running')" ;;
    queued)   state_str="$(cyan  '⋯ queued')"  ;;
    *)        state_str="$(dim   "$STATE")" ;;
  esac

  # ingestion_log extras
  ROWS=$(il_get "$dag_id" 2); ROWS="${ROWS:-—}"
  WM=$(il_get "$dag_id" 5);   WM="${WM:-—}"
  IL_DUR=$(il_get "$dag_id" 3)
  [[ -n "$IL_DUR" ]] && DURATION="${IL_DUR}s"

  printf "%-41s %-19s %-18s %-10s %-8s %s\n" \
    "$dag_id" "$state_str" "$LAST_RUN" "$DURATION" "$ROWS" "$WM"

done <<< "$DAG_IDS"

# ── summary footer ────────────────────────────────────────────────────────────
printf '%0.s─' {1..90}; echo
echo
echo "  $(green "$OK ok")   $(red "$FAILED failed")   $(yellow "$PAUSED_COUNT paused")   of $TOTAL pipeline(s)"
echo
if (( FAILED > 0 )); then
  echo "  $(bold 'Debug failed runs:')"
  echo "  Airflow UI : $(cyan "${AIRFLOW_URL}/dags")"
  echo "  Logs       : docker compose logs --tail=100 airflow-worker"
  echo
fi
