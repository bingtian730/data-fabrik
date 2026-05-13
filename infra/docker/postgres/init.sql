CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE USER datafabrik WITH PASSWORD 'datafabrik';
CREATE DATABASE datafabrik OWNER datafabrik;
GRANT ALL PRIVILEGES ON DATABASE datafabrik TO datafabrik;

\connect datafabrik datafabrik

CREATE SCHEMA IF NOT EXISTS pipeline_metadata;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Incremental watermark state per pipeline+table
CREATE TABLE IF NOT EXISTS pipeline_metadata.watermarks (
    pipeline_id    VARCHAR(255) NOT NULL,
    table_name     VARCHAR(255) NOT NULL,
    last_watermark TIMESTAMP    NOT NULL,
    updated_at     TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, table_name)
);

-- Per-run audit log for JDBC ingestion (rows, duration, s3 path)
CREATE TABLE IF NOT EXISTS pipeline_metadata.ingestion_log (
    id               SERIAL PRIMARY KEY,
    pipeline_id      VARCHAR(255),
    table_name       VARCHAR(255),
    extracted_at     TIMESTAMP DEFAULT NOW(),
    rows_extracted   INTEGER,
    watermark_from   TIMESTAMP,
    watermark_to     TIMESTAMP,
    duration_seconds DOUBLE PRECISION,
    s3_path          TEXT,
    status           VARCHAR(50),
    error_message    TEXT
);

-- DAG-level run record (all pipeline types)
CREATE TABLE IF NOT EXISTS pipeline_metadata.pipeline_runs (
    id               SERIAL PRIMARY KEY,
    pipeline_id      TEXT NOT NULL,
    dag_run_id       TEXT NOT NULL,
    logical_date     TIMESTAMPTZ,
    state            TEXT NOT NULL,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    error_message    TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (pipeline_id, dag_run_id)
);

-- Task-level run record (per stage, captures retries and errors)
CREATE TABLE IF NOT EXISTS pipeline_metadata.task_runs (
    id               SERIAL PRIMARY KEY,
    pipeline_id      TEXT NOT NULL,
    dag_run_id       TEXT NOT NULL,
    task_id          TEXT NOT NULL,
    stage            TEXT,
    state            TEXT NOT NULL,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    duration_seconds NUMERIC(10,3),
    try_number       INT DEFAULT 1,
    error_message    TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Static pipeline topology (source → transform → delivery)
CREATE TABLE IF NOT EXISTS pipeline_metadata.pipeline_lineage (
    id                SERIAL PRIMARY KEY,
    pipeline_id       TEXT NOT NULL UNIQUE,
    source_type       TEXT,
    source_location   TEXT,
    transform_type    TEXT,
    transform_target  TEXT,
    delivery_type     TEXT,
    delivery_location TEXT,
    registered_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);
