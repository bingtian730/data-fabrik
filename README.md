# DataFabrik

DataFabrik is an AI-native cloud data platform that automates data engineering, analytics, and reporting workflows using AWS infrastructure and intelligent agents.

## Core Stack

- AWS
- Terraform
- Airflow
- EMR / Spark
- dbt
- Presto
- Looker
- Claude on Amazon Bedrock

## MVP Goal

Create a config-driven dynamic pipeline factory where adding a new customer pipeline requires mostly YAML configuration instead of custom engineering work.

## Local Development

The repo ships with a [docker-compose.yml](docker-compose.yml) that boots the full local stack: Airflow, Postgres, MinIO, dbt, Presto, and the FastAPI service.

### Prerequisites

- Docker Desktop 4.30+ (or Docker Engine 24+ with Compose v2)
- ~6 GB free memory for the stack

### First-time setup

```bash
cp .env.example .env
# Linux only: set AIRFLOW_UID to your host UID so Airflow can write to mounted log volumes.
# Do NOT set this on macOS — the Docker Desktop VM handles permissions, and an
# unknown UID inside the container breaks `airflow` commands.
#   echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose up -d --build
```

Initial boot pulls images and runs Airflow DB migrations — expect ~3 minutes on a cold cache.

### Service endpoints

| Service       | URL                          | Credentials             |
| ------------- | ---------------------------- | ----------------------- |
| Airflow UI    | http://localhost:8080        | `admin` / `admin`       |
| FastAPI       | http://localhost:8000/docs   | —                       |
| MinIO console | http://localhost:9001        | `minioadmin` / `minioadmin` |
| MinIO S3 API  | http://localhost:9000        | same as console         |
| Presto UI     | http://localhost:8081        | —                       |
| Postgres      | `localhost:5433`             | `postgres` / `postgres` |

Default databases: `airflow` (Airflow metadata) and `datafabrik` (app data, dbt target).

Default MinIO buckets: `datafabrik-raw`, `datafabrik-staging`, `datafabrik-curated`.

### Smoke test

```bash
./scripts/smoke-test.sh
```

Verifies Postgres, MinIO buckets, Airflow, Presto (`SELECT count(*) FROM tpch.tiny.nation`), and FastAPI.

### Running dbt

```bash
docker compose exec dbt dbt debug
docker compose exec dbt dbt run
```

### Tearing down

```bash
docker compose down            # stop containers, keep volumes
docker compose down -v         # also delete data volumes
```