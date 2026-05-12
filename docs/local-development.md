# Local Development Environment

This document describes the local DataFabrik stack that runs on your laptop via Docker Compose. It is the canonical reference for what's installed, where things live, and how to use them.

For the higher-level project overview, see the [README](../README.md).

## TL;DR

```bash
cd ~/datafabrik
cp .env.example .env           # first time only
docker compose up -d           # boot the stack (3-5 min cold start)
./scripts/smoke-test.sh        # verify everything works
```

Then open [Airflow](http://localhost:8080) (`admin` / `admin`) in your browser.

## Services

| Service       | Local URL                          | Default credentials             | Container name                 |
| ------------- | ---------------------------------- | ------------------------------- | ------------------------------ |
| Airflow UI    | http://localhost:8080              | `admin` / `admin`               | `datafabrik-airflow-webserver` |
| FastAPI docs  | http://localhost:8000/docs         | —                               | `datafabrik-fastapi`           |
| FastAPI health| http://localhost:8000/health       | —                               | `datafabrik-fastapi`           |
| MinIO console | http://localhost:9001              | `minioadmin` / `minioadmin`     | `datafabrik-minio`             |
| MinIO S3 API  | http://localhost:9000              | same as console                 | `datafabrik-minio`             |
| Presto UI     | http://localhost:8081              | —                               | `datafabrik-presto`            |
| Postgres      | `localhost:5433`                   | `postgres` / `postgres`         | `datafabrik-postgres`          |
| dbt           | (no exposed port — use `exec`)     | —                               | `datafabrik-dbt`               |

Postgres is on **5433** (not the default 5432) to avoid conflicting with a local Postgres install. Override with `POSTGRES_HOST_PORT` in `.env` if you need a different port.

## What's pre-provisioned

### Postgres databases

| Database     | User         | Password     | Used by                                  |
| ------------ | ------------ | ------------ | ---------------------------------------- |
| `airflow`    | `airflow`    | `airflow`    | Airflow metadata                         |
| `datafabrik` | `datafabrik` | `datafabrik` | FastAPI app + dbt target schema          |
| `postgres`   | `postgres`   | `postgres`   | Superuser, admin tasks                   |

Connect from the host:

```bash
psql -h localhost -p 5433 -U datafabrik -d datafabrik
```

### MinIO buckets (medallion layout)

| Bucket                | Layer  | Purpose                                                          |
| --------------------- | ------ | ---------------------------------------------------------------- |
| `datafabrik-raw`      | bronze | Untouched data as it arrives from source systems                 |
| `datafabrik-staging`  | silver | Cleaned, deduped, schema-validated intermediate data             |
| `datafabrik-curated`  | gold   | Analytics-ready data consumed by dashboards / ML / external APIs |

Buckets are created automatically by the `minio-init` one-shot container on `up`.

### Presto catalogs

| Catalog  | Connector | Purpose                                                  |
| -------- | --------- | -------------------------------------------------------- |
| `tpch`   | `tpch`    | Built-in TPC-H benchmark data (good for smoke tests)     |
| `memory` | `memory`  | Ephemeral in-memory tables for ad-hoc experimentation    |

A Hive catalog backed by MinIO is **not** configured yet — adding one requires a separate Hive metastore service.

### dbt

- Project: [`dbt/datafabrik_models/`](../dbt/datafabrik_models/)
- Profile: writes to Postgres `datafabrik.analytics` schema
- One example model: `models/example/example.sql` (`SELECT 1 AS one`)

### Airflow

- Admin user: `admin` / `admin`
- DAGs folder: [`orchestration/airflow/dags/`](../orchestration/airflow/dags/) (currently empty)
- Plugins folder: [`orchestration/airflow/plugins/`](../orchestration/airflow/plugins/)
- Executor: `LocalExecutor` (no Celery or Redis — simpler for local dev)
- AWS env vars are pre-wired to point at MinIO, so any `S3Hook` / `boto3` call inside a DAG hits MinIO instead of real AWS.

### FastAPI

- Entrypoint: [`backend/app/main.py`](../backend/app/main.py)
- Endpoints: `GET /` and `GET /health`
- Hot reload is enabled — edits to files under `backend/` take effect immediately.
- Pre-wired env: `DATABASE_URL`, `S3_ENDPOINT_URL`, `PRESTO_HOST`, `PRESTO_PORT`.

## Common commands

### Lifecycle

```bash
docker compose up -d                       # boot (idempotent)
docker compose up -d --build               # boot + rebuild local images
docker compose ps                          # list services + health
docker compose down                        # stop, KEEP data volumes
docker compose down -v                     # stop + WIPE all data
docker compose restart <service>           # restart one service
```

### Logs and shells

```bash
docker compose logs -f <service>           # tail logs
docker compose logs --tail=100 <service>   # last 100 lines
docker compose exec <service> sh           # shell into a container
docker compose exec <service> bash         # (some images have bash)
```

### Smoke test

```bash
./scripts/smoke-test.sh
```

Verifies Postgres, MinIO buckets, Airflow, Presto (TPC-H query), and FastAPI. Non-zero exit on any failure.

### dbt

```bash
docker compose exec dbt dbt debug          # verify connection
docker compose exec dbt dbt run            # materialize all models
docker compose exec dbt dbt test           # run tests
docker compose exec dbt dbt build          # run + test in DAG order
docker compose exec dbt dbt run --select example   # run one model
```

### Presto

```bash
# Interactive SQL shell
docker compose exec presto presto-cli --catalog tpch --schema tiny

# One-shot query
docker compose exec presto presto-cli \
  --catalog tpch --schema tiny \
  --execute 'SELECT count(*) FROM nation'
```

### Postgres

```bash
# From host
psql -h localhost -p 5433 -U datafabrik -d datafabrik

# From inside the container
docker compose exec postgres psql -U datafabrik datafabrik
```

### MinIO

Use the web console at http://localhost:9001 for drag-and-drop uploads, or the `mc` CLI:

```bash
docker compose run --rm --no-deps --entrypoint sh \
  -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD minio-init \
  -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls local/'
```

From host code, use boto3 against the local endpoint:

```python
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)
s3.list_buckets()
```

### Airflow CLI

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow tasks list <dag_id>
docker compose exec airflow-webserver airflow dags trigger <dag_id>
```

## File layout

```
docker-compose.yml                    Service definitions
.env.example                          Env var template (copy to .env)
backend/app/main.py                   FastAPI entrypoint
dbt/datafabrik_models/                dbt project (profile + models)
infra/docker/
  fastapi/                            FastAPI image
  postgres/init.sql                   Bootstrap databases + users
  minio/create-buckets.sh             Bootstrap MinIO buckets
  presto/catalog/                     Presto catalog config (.properties)
orchestration/airflow/
  dags/                               Airflow DAGs (mounted into container)
  plugins/                            Airflow plugins
  configs/                            Airflow extra configs
scripts/smoke-test.sh                 End-to-end verifier
```

## Configuration knobs

Set these in `.env` (created by copying `.env.example`):

| Variable                 | Default        | What it does                                                              |
| ------------------------ | -------------- | ------------------------------------------------------------------------- |
| `POSTGRES_HOST_PORT`     | `5433`         | Host port that Postgres listens on                                        |
| `MINIO_ROOT_USER`        | `minioadmin`   | MinIO admin username                                                      |
| `MINIO_ROOT_PASSWORD`    | `minioadmin`   | MinIO admin password                                                      |
| `AIRFLOW_SECRET_KEY`     | `please-change-me` | Flask signing key for Airflow web sessions                            |
| `AIRFLOW_UID`            | `50000`        | **Linux only** — set to `$(id -u)` to avoid log volume permission issues. **Do not set on macOS** — it breaks Airflow because the host UID has no `/etc/passwd` entry inside the container. |

## Troubleshooting

**Port already in use** (e.g. `5432`, `8080`): another service on your machine is using the port. Either stop it or override the host port in `.env` / `docker-compose.yml`.

**Airflow webserver keeps restarting**: check `docker compose logs airflow-init`. The most common cause is `AIRFLOW_UID` being set to a host UID that doesn't exist in the container — remove that line from `.env` on macOS.

**MinIO unhealthy**: the MinIO image dropped `curl`, so the healthcheck uses bash's `/dev/tcp`. If you see healthcheck failures, the container is probably still booting — give it 10-15s.

**Presto query fails with "Catalog X does not exist"**: catalogs are defined in [infra/docker/presto/catalog/](../infra/docker/presto/catalog/). Add a `.properties` file there and recreate the Presto container.

**dbt image is slow on Apple Silicon**: `ghcr.io/dbt-labs/dbt-postgres` is amd64-only and runs under emulation on M-series Macs. Fine for development, noticeably slower than native.

**Full reset**: `docker compose down -v && docker compose up -d` wipes all data and starts fresh. Useful if Airflow's metadata DB gets into a weird state.

## What is *not* set up

Components from the high-level stack that are intentionally out of scope for the local environment:

- EMR / Spark — heavy, runs on AWS in production
- Looker — SaaS, paid
- Terraform / AWS infra — separate workstream, see [infra/terraform/](../infra/terraform/)
- Claude on Bedrock — requires real AWS credentials
- Hive metastore — would let Presto query MinIO data; deferred until needed
- Production-grade Airflow (Celery, Redis, multi-worker) — we use `LocalExecutor` for simplicity

## Cost

The local stack is **100% free**. Every service is open source (Postgres, MinIO, Airflow, dbt-core, Presto, FastAPI), and Docker Desktop is free for personal use and small companies. Cloud costs only kick in when you deploy to AWS.
