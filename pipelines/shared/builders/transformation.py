from __future__ import annotations

from typing import TYPE_CHECKING

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


@register("transformation", "dbt")
def dbt(
    *,
    stage: str,
    stage_config: DbtTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        import docker
        client = docker.from_env()
        container = client.containers.get("datafabrik-dbt")
        cmd = f"dbt run --select {stage_config.select} --target {stage_config.target} --profiles-dir {stage_config.profiles_dir}"
        exit_code, output = container.exec_run(cmd, workdir=stage_config.project_dir)
        print(output.decode())
        if exit_code != 0:
            raise RuntimeError(f"dbt run failed (exit {exit_code})")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("transformation", "sql")
def sql(
    *,
    stage: str,
    stage_config: SqlTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=stage_config.connection_id)
        hook.run(stage_config.sql)
        print(f"[sql] executed successfully")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("transformation", "spark")
def spark(
    *,
    stage: str,
    stage_config: SparkTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        print(f"[spark] would submit job_path={stage_config.job_path} to master={stage_config.master}")
        print("[spark] EMR/Spark operator not yet wired — stub retained until AWS is deployed")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)
