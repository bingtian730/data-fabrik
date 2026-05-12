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

## Dynamic Pipeline Factory

DataFabrik pipelines are config-driven. Adding a new customer pipeline is a YAML file under [orchestration/airflow/configs/pipelines/](../orchestration/airflow/configs/pipelines/), not new Python. The Airflow scheduler picks it up automatically.

### Anatomy

| Component                         | Purpose                                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------------------- |
| `BasePipeline` (ABC)              | Four-stage template (ingestion → transformation → validation → delivery), handles DAG wiring |
| `YamlPipeline`                    | Concrete subclass that resolves builders via the global registry                         |
| Task-builder registry             | `@register("ingestion", "s3_csv")` decorator pattern                                     |
| Typed schema ([pipelines/shared/schema/](../pipelines/shared/schema/)) | Pydantic models with discriminated unions per stage type. `extra="forbid"` rejects unknown YAML keys. JSON Schema in [pipeline_config.schema.json](../pipelines/shared/schema/pipeline_config.schema.json). |
| `ScheduleConfig`                  | Cron/preset, start_date, timezone, catchup, retries, max_active_runs — fed into the Airflow DAG |
| `dynamic_pipelines.py` DAG file   | Iterates `configs/pipelines/*.yaml` and registers one DAG per file                       |

### Built-in task builders

| Stage          | Available `type:` values                |
| -------------- | --------------------------------------- |
| ingestion      | `s3_csv`, `http_api`, `jdbc`            |
| transformation | `dbt`, `sql`, `spark`                   |
| validation     | `row_count`, `schema`, `freshness`      |
| delivery       | `s3_publish`, `slack_notify`, `webhook` |

All builders are currently print-stubs — real implementations come in follow-up tickets. See [pipelines/shared/builders/](../pipelines/shared/builders/) for their config contracts.

### Adding a new pipeline

```bash
# 1. Drop a YAML in this directory
cp orchestration/airflow/configs/pipelines/example_customer.yaml \
   orchestration/airflow/configs/pipelines/acme_daily.yaml
vim orchestration/airflow/configs/pipelines/acme_daily.yaml

# 2. Wait ~30s for the scheduler to pick it up, then verify
docker compose exec airflow-scheduler airflow dags list | grep acme
```

The schema is enforced by [PipelineConfig](../pipelines/shared/config.py); invalid YAML fails fast at DAG parse time with a clear error.

### Pipeline commands

```bash
# List every DAG (built-in + YAML-driven)
docker compose exec airflow-scheduler airflow dags list

# Show task graph for one pipeline
docker compose exec airflow-scheduler airflow tasks list example_customer_daily

# Run end-to-end without scheduling (great for smoke-checking)
docker compose exec airflow-scheduler \
  airflow dags test example_customer_daily 2026-05-12

# Trigger via the scheduled path
docker compose exec airflow-scheduler \
  airflow dags trigger example_customer_daily

# See which task builders are registered
docker compose exec airflow-scheduler python -c \
  "from pipelines.shared import all_builders; print(sorted(all_builders().keys()))"

# Run the pipeline-factory smoke test suite
docker compose exec -T airflow-scheduler python < tests/test_pipeline_factory.py

# Run the schema validation tests
docker compose exec -T airflow-scheduler python < tests/test_pipeline_schema.py

# Regenerate the JSON Schema file (after changing any pydantic model)
docker compose exec -T airflow-scheduler python -m pipelines.shared.schema \
  -o /opt/airflow/pipelines/shared/schema/pipeline_config.schema.json
```

### Pipeline YAML structure

```yaml
pipeline_id: my_pipeline           # alphanumeric / _ / - only; also the DAG id
description: Optional description
owner: data-platform
tags: [example]

schedule:
  cron: "0 6 * * *"                # or preset: "@daily" (exclusive)
  timezone: UTC
  start_date: 2026-01-01T00:00:00
  catchup: false
  max_active_runs: 1
  retries: 3
  retry_delay_minutes: 5

stages:
  ingestion:                       # required, single source
    type: s3_csv                   # discriminator field; chooses the schema
    source_bucket: ...
    source_key: ...
  transformation:                  # optional, single transform
    type: dbt
    select: example
  validation:                      # optional, list of rules (run in parallel)
    - type: row_count
      connection_id: postgres_default
      table: analytics.example
      min_rows: 1
    - type: freshness
      ...
  delivery:                        # optional, single destination
    type: s3_publish
    ...
```

Unknown fields are rejected by the schema, so typos fail fast at DAG parse time.

### Writing a custom task builder

Add a function under [pipelines/shared/builders/](../pipelines/shared/builders/) (or anywhere imported at startup) and decorate it:

```python
from airflow.operators.python import PythonOperator
from pipelines.shared.registry import register

@register("ingestion", "kafka")  # stage, type
def kafka_ingest(*, stage, stage_config, pipeline, dag):
    def _run(**_):
        # real work goes here
        pass
    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)
```

Any pipeline YAML can now use `type: kafka` for its ingestion stage.

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
