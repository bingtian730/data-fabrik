from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.transformations import (
    DbtTransformConfig,
    SparkTransformConfig,
    SqlTransformConfig,
)

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.schema import PipelineConfig


def _stub(label: str, **fields: Any):
    def _run(**_):
        print(f"[{label}] {fields}")
    return _run


@register("transformation", "dbt")
def dbt(
    *,
    stage: str,
    stage_config: DbtTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.dbt",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("transformation", "sql")
def sql(
    *,
    stage: str,
    stage_config: SqlTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.sql",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("transformation", "spark")
def spark(
    *,
    stage: str,
    stage_config: SparkTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.{stage}.spark",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )
