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

| Service              | Local URL                          | Default credentials             | Container name                 |
| -------------------- | ---------------------------------- | ------------------------------- | ------------------------------ |
| Airflow UI           | http://localhost:8080              | `admin` / `admin`               | `datafabrik-airflow-webserver` |
| Pipeline dashboard   | http://localhost:8000/dashboard    | —                               | `datafabrik-fastapi`           |
| FastAPI docs         | http://localhost:8000/docs         | —                               | `datafabrik-fastapi`           |
| FastAPI health       | http://localhost:8000/health       | —                               | `datafabrik-fastapi`           |
| MinIO console        | http://localhost:9001              | `minioadmin` / `minioadmin`     | `datafabrik-minio`             |
| MinIO S3 API         | http://localhost:9000              | same as console                 | `datafabrik-minio`             |
| Presto UI            | http://localhost:8081              | —                               | `datafabrik-presto`            |
| Postgres             | `localhost:5433`                   | `postgres` / `postgres`         | `datafabrik-postgres`          |
| dbt                  | (no exposed port — use `exec`)     | —                               | `datafabrik-dbt`               |

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
- Hot reload is enabled — edits to files under `backend/` take effect immediately.
- Pre-wired env: `DATABASE_URL`, `S3_ENDPOINT_URL`, `PRESTO_HOST`, `PRESTO_PORT`, `AIRFLOW_URL`.

| Endpoint              | Description                                              |
| --------------------- | -------------------------------------------------------- |
| `GET /`               | Service info                                             |
| `GET /health`         | Health check (used by Docker healthcheck)                |
| `GET /dashboard`      | Pipeline health dashboard (HTML, auto-refreshes 30s)     |
| `GET /api/pipelines`  | Pipeline health + 30-day success rate as JSON            |
| `GET /api/runs`       | Recent DAG-level run records from `pipeline_runs`        |
| `GET /api/lineage`    | Source → transform → delivery topology per pipeline      |

## Pipeline Health Dashboard

Open **http://localhost:8000/dashboard** in your browser to see the health of all pipelines at a glance.

The dashboard auto-refreshes every 30 seconds and combines two data sources:

| Data source | What it provides |
| --- | --- |
| Airflow REST API | Current run state, last run time, duration |
| `pipeline_metadata.ingestion_log` | Rows extracted, watermark (JDBC pipelines only) |

### What each column shows

| Column | Description |
| --- | --- |
| **Pipeline** | DAG id with a direct link to the Airflow grid view |
| **State** | Most recent run: ✓ success (green), ✗ failed (red), ⏸ paused (yellow), ↻ running (blue) |
| **Last Run (UTC)** | Start time of the most recent run |
| **Duration** | Wall-clock time of the last run |
| **30-day Success Rate** | Percentage of finished runs (success + failed) that succeeded over the past 30 days, with a colour-coded progress bar and raw count (e.g. `28/30 runs`) |
| **Rows** | Rows extracted on the last JDBC ingestion run (`—` for non-JDBC pipelines) |
| **Watermark** | Last high-water mark written to `pipeline_metadata.watermarks` (JDBC only) |

### Success rate colour coding

| Colour | Threshold | Meaning |
| --- | --- | --- |
| Green | ≥ 90% | Pipeline is healthy |
| Yellow | 70–89% | Intermittent failures — worth investigating |
| Red | < 70% | Pipeline is unreliable — investigate immediately |

### CLI equivalent

If you prefer the terminal, `./scripts/pipeline-status.sh` prints the same information as a colour-coded table with no browser required.

```bash
./scripts/pipeline-status.sh
```

### Raw JSON

All dashboard data is available as JSON:

```bash
curl http://localhost:8000/api/pipelines | jq .   # pipeline health + 30d success rate
curl http://localhost:8000/api/runs      | jq .   # last 25 DAG-level run records
curl http://localhost:8000/api/lineage   | jq .   # source → transform → delivery topology
```

## Pipeline Metadata Tracking

Every pipeline run automatically writes structured metadata to `pipeline_metadata` schema in Postgres. This gives you a queryable audit trail independent of the Airflow UI.

### Tables

| Table | What it stores | Populated by |
| --- | --- | --- |
| `pipeline_runs` | One row per DAG run — state, start/end time, duration, error message | Airflow DAG-level `on_success_callback` / `on_failure_callback` |
| `task_runs` | One row per task attempt — stage, state, duration, retry number, error | Airflow task-level `on_success_callback` / `on_failure_callback` |
| `pipeline_lineage` | Static topology: source type/location → transform → delivery | Written at DAG parse time from the YAML config |
| `ingestion_log` | JDBC-specific detail — rows extracted, watermark range, S3 path | JDBC ingestion builder (existing) |
| `watermarks` | Last watermark per pipeline+table for incremental extraction | JDBC ingestion builder (existing) |

### Useful queries

```sql
-- Last 10 runs across all pipelines
SELECT pipeline_id, state, started_at, duration_seconds, error_message
FROM pipeline_metadata.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;

-- Failed tasks with error messages
SELECT pipeline_id, task_id, stage, try_number, error_message, started_at
FROM pipeline_metadata.task_runs
WHERE state = 'failed'
ORDER BY started_at DESC
LIMIT 20;

-- Retry counts per pipeline (runs that needed more than one attempt)
SELECT pipeline_id, task_id, MAX(try_number) AS max_tries, COUNT(*) AS total_attempts
FROM pipeline_metadata.task_runs
GROUP BY pipeline_id, task_id
HAVING MAX(try_number) > 1
ORDER BY max_tries DESC;

-- Pipeline lineage (source → transform → delivery)
SELECT pipeline_id, source_type, source_location,
       transform_type, transform_target,
       delivery_type, delivery_location
FROM pipeline_metadata.pipeline_lineage
ORDER BY pipeline_id;

-- 7-day success rate per pipeline
SELECT pipeline_id,
       COUNT(*) FILTER (WHERE state = 'success') AS ok,
       COUNT(*) FILTER (WHERE state = 'failed')  AS failed,
       ROUND(
         COUNT(*) FILTER (WHERE state = 'success')::numeric /
         NULLIF(COUNT(*) FILTER (WHERE state IN ('success','failed')), 0) * 100, 1
       ) AS success_pct
FROM pipeline_metadata.pipeline_runs
WHERE started_at >= NOW() - INTERVAL '7 days'
GROUP BY pipeline_id
ORDER BY success_pct ASC NULLS LAST;
```

### How it works

Metadata is written automatically — no changes needed to pipeline YAML files.

- **Lineage** is captured at DAG parse time. Every time Airflow loads a YAML config, the pipeline's source, transform, and delivery topology is upserted into `pipeline_lineage`.
- **Run records** are written by Airflow callbacks registered on the DAG object in `BasePipeline.build_dag()`. Success and failure are both captured; failures include the exception message.
- **Task records** are written by callbacks attached to every operator in `BasePipeline._build_stage()`, including individual validation rules inside the validation TaskGroup. `try_number` increments on each Airflow retry so you can see exactly how many attempts a task needed.
- All writes are **best-effort** — a metadata write failure never blocks a pipeline run.

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

### Developer scripts

Four scripts in [`scripts/`](../scripts/) automate common pipeline tasks:

| Script | What it does |
| --- | --- |
| `./scripts/new-pipeline.sh` | Interactive scaffold — prompts for source type, schedule, transform, delivery; writes the YAML config and dbt model stubs |
| `./scripts/upload-data.sh <file> <bucket>/<prefix>/` | Upload a local file or directory to MinIO; auto-detects `aws-cli` or falls back to `mc` inside Docker |
| `./scripts/run-pipeline.sh <pipeline_id>` | Trigger a DAG run, poll until done, print a per-task pass/fail table, exit 1 on failure |
| `./scripts/pipeline-status.sh` | Print a colour-coded health summary of all pipelines (state, last run, duration, success rate) |

Typical workflow for a new pipeline:

```bash
./scripts/upload-data.sh data/my_feed.csv customer-landing/my_feed/
./scripts/new-pipeline.sh          # answer the prompts
./scripts/run-pipeline.sh my_feed_daily
./scripts/pipeline-status.sh      # confirm it's green
```

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

All builders are real implementations backed by boto3, requests, PostgresHook, and Docker SDK (for dbt). See [pipelines/shared/builders/](../pipelines/shared/builders/) for their config contracts.

### Adding a new pipeline — step by step

This is the complete workflow for shipping a new data pipeline from raw source data to a queryable Postgres table. The Stripe pipeline (`stripe_daily`) was built following exactly these steps.

---

#### Step 1 — Upload raw data to MinIO

Put the source file into the `customer-landing` bucket under a prefix that matches your pipeline name.

```bash
# Using boto3 from inside the Airflow container
docker compose exec airflow-scheduler python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
with open('/path/to/your/data.csv', 'rb') as f:
    s3.put_object(Bucket='customer-landing', Key='your_pipeline/data.csv', Body=f.read())
print('Uploaded')
"
```

Or use the **MinIO console** at http://localhost:9001 (`minioadmin` / `minioadmin`) for drag-and-drop uploads.

Bucket layout to follow:

| Bucket | Stage | What goes here |
| --- | --- | --- |
| `customer-landing` | source | Raw files from the customer / API |
| `datafabrik-raw` | bronze | Copied here by the ingestion task |
| `datafabrik-curated` | gold | Delivered here by the delivery task |

---

#### Step 2 — Create dbt models

Create a folder under `dbt/datafabrik_models/models/<your_pipeline>/` with at least one `.sql` file. Models are materialised as **tables** in the `analytics` Postgres schema.

```
dbt/datafabrik_models/models/
└── your_pipeline/
    ├── stg_your_pipeline.sql          # staging — clean/rename raw fields
    └── your_pipeline_summary.sql      # analytics — aggregate or join
```

**Staging model** (`stg_your_pipeline.sql`) — selects and cleans the raw data:

```sql
SELECT
    id,
    customer_id,
    amount / 100.0  AS amount_usd,
    status,
    created_at::DATE AS event_date
FROM (VALUES
    (1, 'cus_001', 4999, 'succeeded', '2026-05-13'::timestamp),
    (2, 'cus_002',  999, 'failed',    '2026-05-13'::timestamp)
) AS t(id, customer_id, amount, status, created_at)
```

**Analytics model** (`your_pipeline_summary.sql`) — references the staging model using `{{ ref(...) }}`:

```sql
SELECT
    event_date,
    status,
    COUNT(*)          AS total,
    SUM(amount_usd)   AS revenue_usd
FROM {{ ref('stg_your_pipeline') }}
GROUP BY event_date, status
```

Test the models run cleanly before wiring them into the pipeline:

```bash
docker compose exec dbt dbt run --select your_pipeline
```

Check the tables appeared in Postgres:

```bash
docker compose exec postgres psql -U datafabrik -d datafabrik \
  -c "\dt analytics.*"
```

---

#### Step 3 — Create the pipeline YAML

Create `orchestration/airflow/configs/pipelines/your_pipeline_daily.yaml`:

```yaml
pipeline_id: your_pipeline_daily        # must be unique; becomes the Airflow DAG id
description: One-line description.
owner: data-platform
tags:
  - your_pipeline

schedule:
  preset: "@daily"                       # or cron: "0 6 * * *"
  timezone: UTC
  start_date: 2026-01-01T00:00:00
  catchup: false
  max_active_runs: 1
  retries: 1
  retry_delay_minutes: 5

stages:
  ingestion:
    type: s3_csv
    source_bucket: customer-landing
    source_key: your_pipeline/*.csv      # glob — matches all CSVs in the prefix
    dest_bucket: datafabrik-raw
    dest_prefix: your_pipeline/{{ ds }}/

  transformation:
    type: dbt
    project_dir: /usr/app/dbt
    profiles_dir: /usr/app/dbt
    select: your_pipeline                # matches the dbt model folder name
    target: dev

  validation:
    - type: row_count
      connection_id: postgres_default
      table: analytics.stg_your_pipeline
      min_rows: 1
    - type: row_count
      connection_id: postgres_default
      table: analytics.your_pipeline_summary
      min_rows: 1

  delivery:
    type: s3_publish
    source_bucket: datafabrik-raw
    source_prefix: your_pipeline/{{ ds }}/
    dest_bucket: datafabrik-curated
    dest_prefix: your_pipeline/{{ ds }}/
```

The schema validates your YAML at parse time — typos and unknown fields fail immediately.

---

#### Step 4 — Verify the DAG appears in Airflow

The scheduler polls for new YAML files every ~30 seconds. Check it registered:

```bash
docker compose exec airflow-scheduler airflow dags list | grep your_pipeline
```

Or open http://localhost:8080 — the DAG will appear in the list automatically.

---

#### Step 5 — Trigger a manual run

```bash
docker compose exec airflow-scheduler airflow dags trigger \
  your_pipeline_daily \
  --run-id "test_1" \
  --exec-date 2026-05-13T00:00:00
```

Watch task progress:

```bash
docker compose exec airflow-scheduler \
  airflow tasks states-for-dag-run your_pipeline_daily test_1
```

Or watch it in the Airflow UI — click the DAG name → Graph view to see tasks turn green in real time.

---

#### Step 6 — Check results in Postgres

Connect with any Postgres client (see credentials above) or via the terminal:

```bash
docker compose exec postgres psql -U datafabrik -d datafabrik
```

Then query your models:

```sql
-- check the staging table
SELECT * FROM analytics.stg_your_pipeline LIMIT 5;

-- check the analytics summary
SELECT * FROM analytics.your_pipeline_summary;
```

If validation passed, both tables will have rows. If a validation task failed, check its log in the Airflow UI — the error message will tell you the row count that was found vs the minimum required.

---

#### Step 7 — Commit and push

```bash
git add \
  dbt/datafabrik_models/models/your_pipeline/ \
  orchestration/airflow/configs/pipelines/your_pipeline_daily.yaml

git commit -m "add your_pipeline pipeline"
git push origin main
```

The pipeline is now in version control and will be picked up automatically on any machine that runs `docker compose up`.

---

#### Quick reference — full checklist

```
[ ] Upload raw data to customer-landing/<pipeline>/ in MinIO
[ ] Create dbt/datafabrik_models/models/<pipeline>/stg_<pipeline>.sql
[ ] Create dbt/datafabrik_models/models/<pipeline>/<pipeline>_summary.sql
[ ] Run: docker compose exec dbt dbt run --select <pipeline>  (verify locally)
[ ] Create orchestration/airflow/configs/pipelines/<pipeline>_daily.yaml
[ ] Run: airflow dags list | grep <pipeline>  (confirm DAG registered)
[ ] Trigger a manual run and watch tasks go green in the UI
[ ] Query analytics.<pipeline>_summary in Postgres
[ ] Commit and push
```

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
