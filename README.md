# DataFabrik

A local data platform that runs entirely in Docker. Upload a CSV, build a cleaning pipeline, and query results in Postgres — all from a single browser tab.

## What's included

| Service | Purpose |
|---|---|
| **Portal** (FastAPI) | Central UI — pipeline wizard, monitoring, guide |
| **Airflow** | Pipeline orchestration and scheduling |
| **MinIO** | Local S3-compatible object storage |
| **Postgres** | Data warehouse (`raw`, `clean`, `analytics` schemas) |

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop) 4.30+ (Mac or Windows with WSL2, or Linux with Docker Engine 24+)
- ~4 GB free RAM

> **No other installs needed.** Postgres, Airflow, and MinIO all run as Docker containers — nothing is installed on your machine outside of Docker.

## Download & run

**Option A — one-liner (Mac / Linux)**

```bash
git clone https://github.com/bingtian730/data-fabrik.git
cd data-fabrik
./install.sh
```

**Option B — no Git (download ZIP)**

1. Download: [⬇ data-fabrik-main.zip](https://github.com/bingtian730/data-fabrik/archive/refs/heads/main.zip)
2. Unzip it and open a terminal inside the folder
3. Run:

```bash
cp .env.example .env
docker compose up -d --build
```

4. Open **http://localhost:8000** once containers are up (~3–5 min on first run)

## How it works

```
📄 Upload CSV
      ↓
🗄️  MinIO  (datafabrik-raw)      ← raw CSV stored here
      ↓
✈️  Airflow Pipeline              ← DAG triggered automatically
      ↓              ↓
🐘  Postgres        🗄️  MinIO (datafabrik-clean)
   clean schema        ← transformed CSV snapshot exported here
   (SQL view)
```

1. **Upload** a CSV in the Workflow Wizard — rows are sampled into `raw.<table>` in Postgres and the file is stored in the `datafabrik-raw` MinIO bucket.
2. **Build** a pipeline — choose columns, types, filters, joins, and aggregations. A SQL transformation is generated automatically.
3. **Run** — Airflow executes the pipeline: it creates a `clean.<table>` view in Postgres and exports a CSV snapshot to the `datafabrik-clean` MinIO bucket.

The full step-by-step is available in the **Pipeline Guide** tab inside the portal.

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Portal** | http://localhost:8000 | — |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Postgres | `localhost:5433` | see below |

### Postgres databases

Connect with any SQL client (TablePlus, DBeaver, psql):

| Database | User | Password | What it's for |
|---|---|---|---|
| **`datafabrik`** | `datafabrik` | `datafabrik` | Your data — `raw` and `clean` schemas |
| `airflow` | `airflow` | `airflow` | Airflow internal metadata |
| `postgres` | `postgres` | `postgres` | Default system DB |

**The database you want is `datafabrik`** — uploaded CSVs land in `raw.<table>` and cleaned views are created in `clean.<table>`.

### MinIO buckets

| Bucket | What it contains |
|---|---|
| `datafabrik-raw` | Original CSV uploads (`wizard/<table>/`) |
| `datafabrik-clean` | Transformed CSV snapshots after each pipeline run |

## Stop / reset

```bash
# Stop containers (data is preserved)
docker compose down

# Stop and delete all data volumes (fresh start)
docker compose down -v
```

## Troubleshooting

**Services not starting?**
```bash
docker compose logs fastapi
docker compose logs airflow-webserver
```

**Port conflict?**  
Edit `.env` and change `POSTGRES_HOST_PORT` (default `5433`), then restart with `docker compose down && docker compose up -d`.

**Airflow log permission errors on Linux?**  
The `install.sh` script handles this automatically. If setting up manually:
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose down && docker compose up -d
```
