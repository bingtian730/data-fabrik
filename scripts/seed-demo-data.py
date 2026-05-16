#!/usr/bin/env python3
"""Generate and print SQL to seed realistic demo data into pipeline_metadata."""
import random
from datetime import datetime, timedelta, timezone

random.seed(42)

NOW = datetime.now(timezone.utc).replace(microsecond=0)

PIPELINES = [
    {
        "id":              "stripe_charges_daily",
        "source_type":     "jdbc",
        "source_loc":      "public.stripe_charges_raw",
        "transform_type":  "dbt",
        "transform_tgt":   "stg_stripe_charges stripe_daily_revenue",
        "delivery_type":   "s3_publish",
        "delivery_loc":    "datafabrik-curated/stripe/",
        "schedule":        "daily",
        "success_rate":    0.93,
        "avg_dur":         95,
        "avg_rows":        12_500,
    },
    {
        "id":              "acme_orders_daily",
        "source_type":     "jdbc",
        "source_loc":      "public.orders",
        "transform_type":  "dbt",
        "transform_tgt":   "stg_acme_orders acme_orders_summary",
        "delivery_type":   "s3_publish",
        "delivery_loc":    "datafabrik-curated/acme/orders/",
        "schedule":        "daily",
        "success_rate":    0.80,
        "avg_dur":         78,
        "avg_rows":        3_200,
    },
    {
        "id":              "weather_api_hourly",
        "source_type":     "http_api",
        "source_loc":      "https://api.openweather.example.com/v1/current",
        "transform_type":  "dbt",
        "transform_tgt":   "stg_weather_hourly",
        "delivery_type":   None,
        "delivery_loc":    None,
        "schedule":        "hourly",
        "success_rate":    0.94,
        "avg_dur":         22,
        "avg_rows":        None,
    },
    {
        "id":              "customer_csv_weekly",
        "source_type":     "s3_csv",
        "source_loc":      "customer-landing/weekly/*.csv",
        "transform_type":  "dbt",
        "transform_tgt":   "stg_customer_csv customer_summary",
        "delivery_type":   "s3_publish",
        "delivery_loc":    "datafabrik-curated/customers/",
        "schedule":        "weekly",
        "success_rate":    1.0,
        "avg_dur":         145,
        "avg_rows":        8_700,
    },
]

ERRORS = [
    "psycopg2.OperationalError: server closed the connection unexpectedly",
    "Connection refused: upstream service unavailable after 3 retries",
    "boto3.exceptions.S3UploadFailedError: NoSuchBucket — datafabrik-raw",
    "dbt.exceptions.DbtRuntimeError: relation 'analytics.stg_acme_orders' does not exist",
    "requests.exceptions.ReadTimeout: HTTPSConnectionPool timed out after 30s",
    "Validation failed: row_count returned 0 rows (min_rows=1)",
    "AirflowException: Task exited with return code 1",
]


def q(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def ts(dt):
    return q(dt.strftime("%Y-%m-%d %H:%M:%S+00"))


out = ["BEGIN;", ""]

# ── Truncate ──────────────────────────────────────────────────────────────────
out.append("TRUNCATE pipeline_metadata.task_runs,")
out.append("        pipeline_metadata.ingestion_log,")
out.append("        pipeline_metadata.pipeline_runs,")
out.append("        pipeline_metadata.pipeline_lineage,")
out.append("        pipeline_metadata.watermarks")
out.append("  RESTART IDENTITY CASCADE;")
out.append("")

# ── Lineage ───────────────────────────────────────────────────────────────────
out.append("INSERT INTO pipeline_metadata.pipeline_lineage")
out.append("  (pipeline_id, source_type, source_location, transform_type,")
out.append("   transform_target, delivery_type, delivery_location)")
out.append("VALUES")
lineage_rows = []
for p in PIPELINES:
    lineage_rows.append(
        f"  ({q(p['id'])}, {q(p['source_type'])}, {q(p['source_loc'])}, "
        f"{q(p['transform_type'])}, {q(p['transform_tgt'])}, "
        f"{q(p['delivery_type'])}, {q(p['delivery_loc'])})"
    )
out.append(",\n".join(lineage_rows) + ";")
out.append("")

# ── Watermarks (JDBC only) ────────────────────────────────────────────────────
out.append("INSERT INTO pipeline_metadata.watermarks")
out.append("  (pipeline_id, table_name, last_watermark, updated_at)")
out.append("VALUES")
out.append(f"  ('stripe_charges_daily', 'public.stripe_charges_raw', "
           f"{ts(NOW - timedelta(days=1))}, {ts(NOW)}),")
out.append(f"  ('acme_orders_daily', 'public.orders', "
           f"{ts(NOW - timedelta(days=1))}, {ts(NOW)});")
out.append("")

# ── Runs + tasks + ingestion log ──────────────────────────────────────────────
run_rows, task_rows, ingest_rows = [], [], []

for p in PIPELINES:
    pid = p["id"]

    if p["schedule"] == "daily":
        anchors = [
            NOW.replace(hour=0, minute=5, second=0, microsecond=0) - timedelta(days=d)
            for d in range(29, -1, -1)
        ]
    elif p["schedule"] == "hourly":
        anchors = [
            NOW.replace(minute=5, second=0, microsecond=0) - timedelta(hours=h)
            for h in range(71, -1, -1)
        ]
    else:  # weekly
        anchors = [
            NOW.replace(hour=1, minute=0, second=0, microsecond=0) - timedelta(weeks=w)
            for w in range(3, -1, -1)
        ]

    for run_start in anchors:
        ok = random.random() < p["success_rate"]
        dur = int(p["avg_dur"] * (0.7 + random.random() * 0.6))
        state = "success" if ok else "failed"
        run_end = run_start + timedelta(seconds=dur if ok else int(dur * 0.35))
        err = None if ok else random.choice(ERRORS)
        dag_run_id = "scheduled__" + run_start.strftime("%Y-%m-%dT%H:%M:00+00:00")

        run_rows.append(
            f"  ({q(pid)}, {q(dag_run_id)}, {ts(run_start)}, {q(state)}, "
            f"{ts(run_start)}, {ts(run_end)}, "
            f"{dur if ok else int(dur * 0.35)}, {q(err)})"
        )

        # Tasks — ingestion always runs; others only if ingestion succeeded
        stages = ["ingestion", "transformation"]
        if p["delivery_type"]:
            stages += ["validation", "delivery"]

        offset = 0
        for stage in stages:
            frac = {"ingestion": 0.35, "transformation": 0.40,
                    "validation": 0.10, "delivery": 0.15}.get(stage, 0.25)
            sdur = max(3, int(dur * frac * (0.8 + random.random() * 0.4)))
            t_start = run_start + timedelta(seconds=offset)
            t_end   = t_start + timedelta(seconds=sdur)
            # Failed runs: ingestion or transformation fails, later stages skipped
            if not ok and stage == "ingestion" and random.random() < 0.65:
                t_state = "failed"
                t_err = err
            elif not ok and stage == "transformation" and t_state != "failed" and random.random() < 0.5:
                t_state = "failed"
                t_err = err
            else:
                t_state = "success"
                t_err = None

            task_rows.append(
                f"  ({q(pid)}, {q(dag_run_id)}, {q(stage + '_task')}, {q(stage)}, "
                f"{q(t_state)}, {ts(t_start)}, {ts(t_end)}, {sdur}, 1, {q(t_err)})"
            )
            offset += sdur
            if t_state == "failed":
                break

        # Ingestion log (JDBC successful runs only)
        if p["source_type"] == "jdbc" and ok:
            rows = int(p["avg_rows"] * (0.75 + random.random() * 0.5))
            s3 = f"s3://datafabrik-raw/{pid}/{run_start.strftime('%Y-%m-%d')}.parquet"
            ingest_rows.append(
                f"  ({q(pid)}, {q(pid.split('_')[0])}, {ts(run_end)}, "
                f"{rows}, {ts(run_start - timedelta(days=1))}, {ts(run_start)}, "
                f"{dur * 0.3:.1f}, {q(s3)}, 'success', NULL)"
            )

out.append("INSERT INTO pipeline_metadata.pipeline_runs")
out.append("  (pipeline_id, dag_run_id, logical_date, state,")
out.append("   started_at, finished_at, duration_seconds, error_message)")
out.append("VALUES")
out.append(",\n".join(run_rows) + ";")
out.append("")

out.append("INSERT INTO pipeline_metadata.task_runs")
out.append("  (pipeline_id, dag_run_id, task_id, stage, state,")
out.append("   started_at, finished_at, duration_seconds, try_number, error_message)")
out.append("VALUES")
out.append(",\n".join(task_rows) + ";")
out.append("")

if ingest_rows:
    out.append("INSERT INTO pipeline_metadata.ingestion_log")
    out.append("  (pipeline_id, table_name, extracted_at, rows_extracted,")
    out.append("   watermark_from, watermark_to, duration_seconds, s3_path, status, error_message)")
    out.append("VALUES")
    out.append(",\n".join(ingest_rows) + ";")
    out.append("")

out.append("COMMIT;")
print("\n".join(out))
