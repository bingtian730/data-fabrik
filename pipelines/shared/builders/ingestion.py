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


@register("ingestion", "s3_csv")
def s3_csv(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Copy CSV files from a source S3 location to the raw bucket.

    config:
      source_bucket: str (required)
      source_key:    str (required, supports glob)
      dest_bucket:   str (default: datafabrik-raw)
      dest_prefix:   str (default: <pipeline_id>/{{ ds }}/)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.s3_csv", **stage_config.config),
        dag=dag,
    )


@register("ingestion", "http_api")
def http_api(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Fetch from an HTTP endpoint and land in the raw bucket.

    config:
      url:          str (required)
      method:       str (default: GET)
      headers:      dict[str, str] (optional)
      dest_bucket:  str (default: datafabrik-raw)
      dest_key:     str (required)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.http_api", **stage_config.config),
        dag=dag,
    )


@register("ingestion", "jdbc")
def jdbc(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Pull from a JDBC source via a connection_id and write to the raw bucket.

    config:
      connection_id: str (required, Airflow connection)
      query:         str (required)
      dest_bucket:   str (default: datafabrik-raw)
      dest_key:      str (required)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.jdbc", **stage_config.config),
        dag=dag,
    )
