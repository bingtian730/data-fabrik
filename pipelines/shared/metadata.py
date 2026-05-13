from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pipelines.shared.schema import PipelineConfig

log = logging.getLogger(__name__)


def _conn():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id="postgres_default").get_conn()


def _secs(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)
    return round((end - start).total_seconds(), 3)


# ── DAG-level (pipeline_runs) ─────────────────────────────────────────────────

def upsert_pipeline_run(context: dict[str, Any], state: str, error: str | None = None) -> None:
    """Write or update the DAG-level run record."""
    dag_run   = context["dag_run"]
    pipeline_id = context["dag"].dag_id
    now       = datetime.now(timezone.utc)
    finished  = dag_run.end_date or now
    duration  = _secs(dag_run.start_date, finished)

    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metadata.pipeline_runs
                  (pipeline_id, dag_run_id, logical_date, state,
                   started_at, finished_at, duration_seconds, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_id, dag_run_id) DO UPDATE SET
                  state            = EXCLUDED.state,
                  finished_at      = EXCLUDED.finished_at,
                  duration_seconds = EXCLUDED.duration_seconds,
                  error_message    = EXCLUDED.error_message
                """,
                (
                    pipeline_id,
                    dag_run.run_id,
                    getattr(dag_run, "logical_date", None) or getattr(dag_run, "execution_date", None),
                    state,
                    dag_run.start_date,
                    finished,
                    duration,
                    error,
                ),
            )
        conn.commit()
        conn.close()
        log.info("[metadata] pipeline_run upserted: %s %s → %s", pipeline_id, dag_run.run_id, state)
    except Exception as exc:
        log.warning("[metadata] upsert_pipeline_run failed: %s", exc)


# ── Task-level (task_runs) ────────────────────────────────────────────────────

def write_task_run(context: dict[str, Any], state: str, error: str | None = None) -> None:
    """Append a task-level run record (one row per attempt)."""
    ti          = context["task_instance"]
    dag_run     = context["dag_run"]
    pipeline_id = context["dag"].dag_id
    now         = datetime.now(timezone.utc)
    finished    = ti.end_date or now
    duration    = _secs(ti.start_date, finished)

    # Map task_id to stage name (validation tasks live inside a group: "validation.row_count")
    stage = ti.task_id.split(".")[0].replace("_skipped", "")

    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metadata.task_runs
                  (pipeline_id, dag_run_id, task_id, stage, state,
                   started_at, finished_at, duration_seconds, try_number, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_id,
                    dag_run.run_id,
                    ti.task_id,
                    stage,
                    state,
                    ti.start_date,
                    finished,
                    duration,
                    ti.try_number,
                    error,
                ),
            )
        conn.commit()
        conn.close()
    except Exception as exc:
        log.warning("[metadata] write_task_run failed: %s", exc)


# ── Lineage (pipeline_lineage) ────────────────────────────────────────────────

def upsert_lineage(config: PipelineConfig) -> None:
    """Record the static source→transform→delivery topology for a pipeline."""
    stages = config.stages

    ingestion = stages.ingestion
    src_type  = ingestion.type if ingestion else None
    if src_type == "s3_csv":
        src_loc = f"s3://{ingestion.source_bucket}/{ingestion.source_key}"
    elif src_type == "http_api":
        src_loc = ingestion.url
    elif src_type == "jdbc":
        src_loc = ingestion.table
    else:
        src_loc = None

    transform    = stages.transformation
    tx_type      = transform.type if transform else None
    tx_target    = (
        transform.select    if tx_type == "dbt" else
        transform.sql_file  if tx_type == "sql" else
        None
    )

    delivery  = stages.delivery
    del_type  = delivery.type if delivery else None
    if del_type == "s3_publish":
        del_loc = f"s3://{delivery.dest_bucket}/{delivery.dest_prefix}"
    elif del_type == "slack_notify":
        del_loc = delivery.channel
    elif del_type == "webhook":
        del_loc = delivery.url
    else:
        del_loc = None

    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metadata.pipeline_lineage
                  (pipeline_id, source_type, source_location,
                   transform_type, transform_target,
                   delivery_type, delivery_location)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_id) DO UPDATE SET
                  source_type       = EXCLUDED.source_type,
                  source_location   = EXCLUDED.source_location,
                  transform_type    = EXCLUDED.transform_type,
                  transform_target  = EXCLUDED.transform_target,
                  delivery_type     = EXCLUDED.delivery_type,
                  delivery_location = EXCLUDED.delivery_location,
                  updated_at        = NOW()
                """,
                (
                    config.pipeline_id,
                    src_type, src_loc,
                    tx_type, tx_target,
                    del_type, del_loc,
                ),
            )
        conn.commit()
        conn.close()
        log.info("[metadata] lineage upserted for %s", config.pipeline_id)
    except Exception as exc:
        log.warning("[metadata] upsert_lineage failed: %s", exc)
