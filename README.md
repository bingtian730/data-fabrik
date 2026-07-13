# DataFabrik

A local data platform that runs entirely in Docker. Upload a CSV, build a cleaning pipeline, and query results in Postgres — all from a single browser tab.

## What's included

| Service | Purpose |
|---|---|
| **App** (React + nginx) | Browser UI — workflow wizard, pipeline monitor, guide |
| **API** (FastAPI) | Backend — upload, pipeline management, health checks |
| **Airflow** | Pipeline orchestration and scheduling |
| **MinIO** | Local S3-compatible object storage |
| **Postgres** | Data warehouse (`raw`, `clean`, `analytics` schemas) |

## Requirements

- [Docker Desktop](https://www.docker.com/products/docker-desktop) 4.30+ (Mac or Windows with WSL2, or Linux with Docker Engine 24+)
- ~6 GB free RAM

> **No other installs needed.** Postgres, Airflow, MinIO, and Metabase all run as Docker containers — nothing is installed on your machine outside of Docker.

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

4. Open **http://localhost:3000** once containers are up (~3–5 min on first run)

## How it works

```
📄 Upload CSV           →    🗄️ MinIO Storage
                                    ↓
                             ✈️ Airflow Pipeline
                                    ↓
                             🐘 Postgres  (clean · analytics schemas)
```

The full step-by-step is available in the **Pipeline Guide** tab inside the portal.

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| **App** | http://localhost:3000 | — |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Postgres | `localhost:5433` | see below |

### Postgres databases

Connect with any SQL client (TablePlus, DBeaver, psql):

| Database | User | Password | What it's for |
|---|---|---|---|
| **`datafabrik`** | `datafabrik` | `datafabrik` | Your data — `raw`, `clean`, `analytics` schemas |
| `airflow` | `airflow` | `airflow` | Airflow internal metadata |
| `postgres` | `postgres` | `postgres` | Default system DB |

**The database you want is `datafabrik`** — this is where uploaded CSVs land in `raw`, cleaned data goes into `clean`, and aggregations go into `analytics`.

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
