#!/usr/bin/env bash
# DataFabrik — one-command local setup
set -euo pipefail

BOLD='\033[1m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
DIM='\033[2m'
NC='\033[0m'

echo ""
echo -e "  ${BOLD}DataFabrik — Local Data Platform${NC}"
echo -e "  ${DIM}────────────────────────────────────${NC}"
echo ""

# ── 1. Check Docker ─────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  echo -e "  ${RED}✗ Docker not found.${NC}"
  echo "    Download Docker Desktop → https://www.docker.com/products/docker-desktop"
  exit 1
fi

if ! docker info &>/dev/null; then
  echo -e "  ${RED}✗ Docker is not running.${NC}"
  echo "    Start Docker Desktop, then re-run this script."
  exit 1
fi

if ! docker compose version &>/dev/null; then
  echo -e "  ${RED}✗ Docker Compose v2 not found.${NC}"
  echo "    Upgrade to Docker Desktop 4.30+ which bundles Compose v2."
  exit 1
fi

echo -e "  ${GREEN}✓${NC} Docker is ready"

# ── 2. Environment file ─────────────────────────────────────────────────
if [ ! -f .env ]; then
  cp .env.example .env
  # On Linux, Airflow needs the host UID to write to mounted log volumes.
  # On macOS, Docker Desktop handles this — leave AIRFLOW_UID at default.
  if [[ "$(uname)" == "Linux" ]]; then
    HOST_UID=$(id -u)
    sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=${HOST_UID}/" .env
    echo -e "  ${GREEN}✓${NC} Created .env  ${DIM}(set AIRFLOW_UID=${HOST_UID} for Linux)${NC}"
  else
    echo -e "  ${GREEN}✓${NC} Created .env"
  fi
else
  echo -e "  ${YELLOW}–${NC} .env already exists — keeping your settings"
fi

# ── 3. Start the stack ──────────────────────────────────────────────────
echo ""
echo -e "  ${BOLD}Starting DataFabrik…${NC}  ${DIM}(first run pulls ~2 GB of images, ~3–5 min)${NC}"
echo ""
docker compose up -d --build

# ── 4. Wait for the portal (FastAPI) to be healthy ──────────────────────
echo ""
echo -n "  Waiting for services to be ready"
MAX=240; ELAPSED=0
until curl -sf http://localhost:8000/health &>/dev/null; do
  if (( ELAPSED >= MAX )); then
    echo ""
    echo -e "  ${RED}✗ Timed out after ${MAX}s waiting for the portal.${NC}"
    echo "    Check what went wrong:"
    echo "      docker compose logs fastapi"
    echo "      docker compose logs airflow-webserver"
    exit 1
  fi
  sleep 5; ELAPSED=$((ELAPSED + 5)); echo -n "."
done
echo ""

# ── 5. Done ─────────────────────────────────────────────────────────────
echo ""
echo -e "  ${GREEN}${BOLD}✓ DataFabrik is ready!${NC}"
echo ""
echo -e "  ${BOLD}App${NC}         ${BLUE}http://localhost:3000${NC}          ${DIM}← open this first${NC}"
echo -e "  ${BOLD}Airflow${NC}     ${BLUE}http://localhost:8080${NC}          ${DIM}admin / admin${NC}"
echo -e "  ${BOLD}MinIO${NC}       ${BLUE}http://localhost:9001${NC}          ${DIM}minioadmin / minioadmin${NC}"
echo -e "  ${BOLD}Postgres${NC}    localhost:5433                 ${DIM}postgres / postgres${NC}"
echo ""
echo -e "  ${DIM}Stop:   docker compose down${NC}"
echo -e "  ${DIM}Reset:  docker compose down -v${NC}"
echo ""
