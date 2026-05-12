from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.destinations import (
    S3PublishDestinationConfig,
    SlackNotifyDestinationConfig,
    WebhookDestinationConfig,
)

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.schema import PipelineConfig


def _stub(label: str, **fields: Any):
    def _run(**_):
        print(f"[{label}] {fields}")
    return _run


@register("delivery", "s3_publish")
def s3_publish(
    *,
    stage: str,
    stage_config: S3PublishDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.s3_publish",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("delivery", "slack_notify")
def slack_notify(
    *,
    stage: str,
    stage_config: SlackNotifyDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.slack_notify",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("delivery", "webhook")
def webhook(
    *,
    stage: str,
    stage_config: WebhookDestinationConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.webhook",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )
