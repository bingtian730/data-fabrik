from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.config import PipelineConfig, StageConfig


def _stub(label: str, **kwargs):
    def _run(**_):
        print(f"[{label}] config={kwargs}")
    return _run


@register("delivery", "s3_publish")
def s3_publish(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Copy curated data to a downstream-readable S3 location.

    config:
      source_bucket: str (default: datafabrik-curated)
      source_prefix: str (required)
      dest_bucket:   str (required)
      dest_prefix:   str (required)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.s3_publish", **stage_config.config),
        dag=dag,
    )


@register("delivery", "slack_notify")
def slack_notify(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Post a completion message to a Slack channel.

    config:
      connection_id: str (default: slack_default)
      channel:       str (required)
      message:       str (required, supports Jinja)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.slack_notify", **stage_config.config),
        dag=dag,
    )


@register("delivery", "webhook")
def webhook(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Call an HTTP webhook to notify a downstream system.

    config:
      url:     str (required)
      method:  str (default: POST)
      headers: dict (optional)
      payload: dict (optional)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.webhook", **stage_config.config),
        dag=dag,
    )
