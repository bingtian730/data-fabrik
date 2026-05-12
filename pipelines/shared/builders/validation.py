from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.validations import (
    FreshnessValidation,
    RowCountValidation,
    SchemaValidation,
)

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.schema import PipelineConfig


def _stub(label: str, **fields: Any):
    def _run(**_):
        print(f"[{label}] {fields}")
    return _run


@register("validation", "row_count")
def row_count(
    *,
    stage: str,
    stage_config: RowCountValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.validation.row_count",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("validation", "schema")
def schema(
    *,
    stage: str,
    stage_config: SchemaValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.validation.schema",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )


@register("validation", "freshness")
def freshness(
    *,
    stage: str,
    stage_config: FreshnessValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(
            f"{pipeline.pipeline_id}.validation.freshness",
            **stage_config.model_dump(),
        ),
        dag=dag,
    )
