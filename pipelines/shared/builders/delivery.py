from __future__ import annotations

import os
from typing import TYPE_CHECKING

import boto3
from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.destinations import (
    PostgresTableDestinationConfig,
    S3PublishDestinationConfig,
    SlackNotifyDestinationConfig,
    WebhookDestinationConfig,
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


@register("delivery", "s3_publish")
def s3_publish(
    *,
    stage: str,
    stage_config: S3PublishDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        s3 = _s3_client()
        src_prefix = stage_config.source_prefix.replace("{{ ds }}", context["ds"])
        dest_prefix = stage_config.dest_prefix.replace("{{ ds }}", context["ds"])
        paginator = s3.get_paginator("list_objects_v2")
        copied = 0
        for page in paginator.paginate(Bucket=stage_config.source_bucket, Prefix=src_prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                dest_key = dest_prefix + src_key[len(src_prefix):]
                s3.copy_object(
                    CopySource={"Bucket": stage_config.source_bucket, "Key": src_key},
                    Bucket=stage_config.dest_bucket,
                    Key=dest_key,
                )
                print(f"Published s3://{stage_config.source_bucket}/{src_key} → s3://{stage_config.dest_bucket}/{dest_key}")
                copied += 1
        print(f"[s3_publish] done — {copied} file(s) published")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)


@register("delivery", "slack_notify")
def slack_notify(
    *,
    stage: str,
    stage_config: SlackNotifyDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        ds = context["ds"]
        message = stage_config.message.replace("{{ ds }}", ds)
        hook = SlackWebhookHook(slack_webhook_conn_id=stage_config.connection_id)
        hook.send(text=message, channel=stage_config.channel)
        print(f"[slack_notify] sent to {stage_config.channel}: {message}")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)


@register("delivery", "postgres_table")
def postgres_table(
    *,
    stage: str,
    stage_config: PostgresTableDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        import os
        from sqlalchemy import create_engine, text

        eng = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
        with eng.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {stage_config.table}")).scalar()
            conn.execute(text(f"ANALYZE {stage_config.table}"))
        print(f"[postgres_table] delivered {count} rows → {stage_config.table}")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("delivery", "webhook")
def webhook(
    *,
    stage: str,
    stage_config: WebhookDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**context):
        import requests
        import json
        payload = {
            k: (v.replace("{{ ds }}", context["ds"]) if isinstance(v, str) else v)
            for k, v in stage_config.payload.items()
        }
        resp = requests.request(
            stage_config.method,
            stage_config.url,
            headers=stage_config.headers,
            data=json.dumps(payload),
            timeout=30,
        )
        resp.raise_for_status()
        print(f"[webhook] {stage_config.method} {stage_config.url} → {resp.status_code}")

    return PythonOperator(task_id=stage, python_callable=_run, provide_context=True, dag=dag)
