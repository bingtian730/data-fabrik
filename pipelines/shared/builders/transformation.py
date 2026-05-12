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


@register("transformation", "dbt")
def dbt(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Run dbt models for this pipeline.

    config:
      project_dir:  str (default: /usr/app/dbt)
      profiles_dir: str (default: /usr/app/dbt)
      select:       str (optional, dbt --select selector)
      target:       str (default: dev)

    Currently a stub. Real execution requires either installing dbt inside
    the Airflow image or using a DockerOperator to exec into the `dbt`
    service; tracked as a follow-up.
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.dbt", **stage_config.config),
        dag=dag,
    )


@register("transformation", "sql")
def sql(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Execute a SQL script against a configured connection.

    config:
      connection_id: str (required)
      sql_file:      str (required)
      parameters:    dict (optional)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.sql", **stage_config.config),
        dag=dag,
    )


@register("transformation", "spark")
def spark(
    *,
    stage: str,
    stage_config: StageConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    """Submit a Spark job (e.g. to EMR).

    config:
      cluster_id:   str (required if using existing EMR cluster)
      script:       str (s3 path to the Spark script)
      arguments:    list[str] (optional)
    """
    return PythonOperator(
        task_id=stage,
        python_callable=_stub(f"{pipeline.pipeline_id}.{stage}.spark", **stage_config.config),
        dag=dag,
    )
