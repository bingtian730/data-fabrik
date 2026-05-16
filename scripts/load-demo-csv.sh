#!/usr/bin/env bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Copying CSV files into Postgres container..."
docker compose -f "$REPO_ROOT/docker-compose.yml" cp "$REPO_ROOT/demo/data/customers.csv"       postgres:/tmp/customers.csv
docker compose -f "$REPO_ROOT/docker-compose.yml" cp "$REPO_ROOT/demo/data/invoices.csv"         postgres:/tmp/invoices.csv
docker compose -f "$REPO_ROOT/docker-compose.yml" cp "$REPO_ROOT/demo/data/customer_orders.csv"  postgres:/tmp/customer_orders.csv

echo "Loading into raw schema..."
docker compose -f "$REPO_ROOT/docker-compose.yml" exec -T postgres \
  psql -U datafabrik -d datafabrik < "$REPO_ROOT/scripts/load-demo-csv.sql"

echo "Done."
