from __future__ import annotations

import os
from typing import TYPE_CHECKING

import boto3
import requests as _requests
from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.sources import (
    HttpApiSourceConfig,
    JdbcSourceConfig,
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
                print(f"Copied s3://{stage_config.source_bucket}/{src_key} → s3://{stage_config.dest_bucket}/{dest_key}")
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
        print(f"Fetched {stage_config.url} ({len(resp.content)} bytes) → s3://{stage_config.dest_bucket}/{dest_key}")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)


@register("ingestion", "jdbc")
def jdbc(
    *,
    stage: str,
    stage_config: JdbcSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=stage_config.connection_id)
        records = hook.get_records(stage_config.query)
        dest_key = stage_config.dest_key.replace("{{ ds }}", context["ds"])
        import json
        s3 = _s3_client()
        s3.put_object(
            Bucket=stage_config.dest_bucket,
            Key=dest_key,
            Body=json.dumps(records).encode(),
        )
        print(f"JDBC query returned {len(records)} rows → s3://{stage_config.dest_bucket}/{dest_key}")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)
