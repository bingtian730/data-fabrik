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


@register("validation", "row_count")
def row_count(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Assert a table has at least `min_rows` rows.

    config:
      connection_id: str (required)
      table:         str (required, schema-qualified)
      min_rows:      int (default: 1)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.row_count", **stage_config.config),
        dag=dag,
    )


@register("validation", "schema")
def schema(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Validate that a table's columns match an expected contract.

    config:
      connection_id:  str (required)
      table:          str (required)
      expected_columns: list[str] (required)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.schema", **stage_config.config),
        dag=dag,
    )


@register("validation", "freshness")
def freshness(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Check that a timestamp column has data newer than `max_lag_minutes`.

    config:
      connection_id:   str (required)
      table:           str (required)
      timestamp_column: str (required)
      max_lag_minutes:  int (default: 1440)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.freshness", **stage_config.config),
        dag=dag,
    )
