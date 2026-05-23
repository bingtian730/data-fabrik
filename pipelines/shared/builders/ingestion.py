from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

import boto3
import requests as _requests
from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.sources import (
    HttpApiSourceConfig,
    JdbcSourceConfig,
    MinioCsvSourceConfig,
    S3CsvSourceConfig,
)

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.schema import PipelineConfig


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


@register("ingestion", "s3_csv")
def s3_csv(
    *,
    stage: str,
    stage_config: S3CsvSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        s3 = _s3_client()
        prefix = stage_config.source_key.split("*")[0]
        paginator = s3.get_paginator("list_objects_v2")
        copied = 0
        for page in paginator.paginate(Bucket=stage_config.source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                filename = src_key.rsplit("/", 1)[-1]
                dest_prefix = (stage_config.dest_prefix or "").replace(
                    "{{ ds }}", context["ds"]
                )
                dest_key = f"{dest_prefix}{filename}"
                s3.copy_object(
                    CopySource={"Bucket": stage_config.source_bucket, "Key": src_key},
                    Bucket=stage_config.dest_bucket,
                    Key=dest_key,
                )
                print(
                    f"Copied s3://{stage_config.source_bucket}/{src_key}"
                    f" → s3://{stage_config.dest_bucket}/{dest_key}"
                )
                copied += 1
        if copied == 0:
            print(f"Warning: no objects found under s3://{stage_config.source_bucket}/{prefix}")
        print(f"[s3_csv] done — {copied} file(s) copied")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)


@register("ingestion", "http_api")
def http_api(
    *,
    stage: str,
    stage_config: HttpApiSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        resp = _requests.request(
            stage_config.method,
            stage_config.url,
            headers=stage_config.headers,
            timeout=30,
        )
        resp.raise_for_status()

        dest_key = stage_config.dest_key.replace("{{ ds }}", context["ds"])
        s3 = _s3_client()
        s3.put_object(Bucket=stage_config.dest_bucket, Key=dest_key, Body=resp.content)
        print(
            f"Fetched {stage_config.url} ({len(resp.content)} bytes)"
            f" → s3://{stage_config.dest_bucket}/{dest_key}"
        )

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)


def _jdbc_read_watermark(conn, pipeline_id, table, watermark_init):
    from datetime import datetime
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_watermark FROM pipeline_metadata.watermarks"
            " WHERE pipeline_id = %s AND table_name = %s",
            (pipeline_id, table),
        )
        row = cur.fetchone()
    return row[0] if row else datetime.fromisoformat(watermark_init)


def _jdbc_write_parquet(df, stage_config, ds):
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    raw_prefix = stage_config.dest_prefix or stage_config.table.replace(".", "/")
    prefix = raw_prefix.rstrip("/")
    table_name = stage_config.table.split(".")[-1]
    dest_key = f"{prefix}/{ds}/{table_name}_{ds}.parquet"
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    _s3_client().put_object(
        Bucket=stage_config.dest_bucket,
        Key=dest_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return f"s3://{stage_config.dest_bucket}/{dest_key}"


def _jdbc_update_watermark(conn, pipeline_id, table, watermark_to):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metadata.watermarks
              (pipeline_id, table_name, last_watermark, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (pipeline_id, table_name)
            DO UPDATE SET last_watermark = EXCLUDED.last_watermark,
                          updated_at = NOW()
            """,
            (pipeline_id, table, watermark_to),
        )
    conn.commit()


def _jdbc_log(conn, pipeline_id, table, rows, wm_from, wm_to, duration, s3_path, error):
    status = "failed" if error else "success"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metadata.ingestion_log
              (pipeline_id, table_name, extracted_at, rows_extracted,
               watermark_from, watermark_to, duration_seconds,
               s3_path, status, error_message)
            VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
            """,
            (pipeline_id, table, rows, wm_from, wm_to, duration,
             s3_path, status, error),
        )
    conn.commit()
    print(f"[jdbc] logged run — status={status}, duration={duration}s")


@register("ingestion", "minio_csv")
def minio_csv(
    *,
    stage: str,
    stage_config: MinioCsvSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        import io
        import csv
        import os
        from sqlalchemy import create_engine, text

        s3 = _s3_client()
        obj = s3.get_object(Bucket=stage_config.bucket, Key=stage_config.key)
        content = obj["Body"].read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            print(f"[minio_csv] warning: no data rows in s3://{stage_config.bucket}/{stage_config.key}")
            return

        fieldnames = list(reader.fieldnames or [])
        table = stage_config.table

        def _safe(s: str) -> str:
            return re.sub(r"[^a-z0-9_]", "_", s.strip().lower())

        safe_cols = [_safe(c) for c in fieldnames]
        col_defs = ", ".join(f'"{c}" TEXT' for c in safe_cols)
        col_list = ", ".join(f'"{c}"' for c in safe_cols)
        placeholders = ", ".join(f":{c}" for c in safe_cols)

        db_url = os.environ["DATABASE_URL"]
        eng = create_engine(db_url, pool_pre_ping=True)
        with eng.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{table}"'))
            conn.execute(text(f'CREATE TABLE raw."{table}" ({col_defs}, uploaded_at TIMESTAMPTZ DEFAULT now())'))
            for row in rows:
                data = {_safe(k): (v or "").strip() or None for k, v in row.items()}
                conn.execute(text(f'INSERT INTO raw."{table}" ({col_list}) VALUES ({placeholders})'), data)

        print(f"[minio_csv] loaded {len(rows)} rows → raw.\"{table}\"")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("ingestion", "jdbc")
def jdbc(
    *,
    stage: str,
    stage_config: JdbcSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        import time
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        conn = PostgresHook(postgres_conn_id=stage_config.connection_id).get_conn()
        ds = context["ds"]
        start = time.time()
        error_msg = None
        rows_extracted = 0
        watermark_to = None
        s3_path = None

        watermark_from = _jdbc_read_watermark(
            conn, pipeline.pipeline_id, stage_config.table, stage_config.watermark_init
        )
        try:
            query = (
                f"SELECT * FROM {stage_config.table}"
                f" WHERE {stage_config.watermark_column} > %s"
                f" ORDER BY {stage_config.watermark_column}"
            )
            df = pd.read_sql(query, conn, params=(watermark_from,))
            rows_extracted = len(df)
            print(
                f"[jdbc] extracted {rows_extracted} rows from {stage_config.table}"
                f" (watermark > {watermark_from})"
            )
            if rows_extracted > 0:
                watermark_to = df[stage_config.watermark_column].max()
                s3_path = _jdbc_write_parquet(df, stage_config, ds)
                print(f"[jdbc] wrote {rows_extracted} rows → {s3_path}")
                _jdbc_update_watermark(
                    conn, pipeline.pipeline_id, stage_config.table, watermark_to
                )
        except Exception as exc:
            error_msg = str(exc)
            conn.rollback()
            raise
        finally:
            duration = round(time.time() - start, 3)
            _jdbc_log(
                conn, pipeline.pipeline_id, stage_config.table,
                rows_extracted, watermark_from, watermark_to,
                duration, s3_path, error_msg,
            )
            conn.close()

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)
