from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def _stub(label: str, **fields: Any):
    def _run(**_):
        print(f"[{label}] {fields}")
    return _run


@register("ingestion", "s3_csv")
def s3_csv(
    *,
    stage: str,
    stage_config: S3CsvSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.s3_csv",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("ingestion", "http_api")
def http_api(
    *,
    stage: str,
    stage_config: HttpApiSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.http_api",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("ingestion", "jdbc")
def jdbc(
    *,
    stage: str,
    stage_config: JdbcSourceConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.jdbc",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )
