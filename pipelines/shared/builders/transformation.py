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


def _dbt_exec(container, cmd: str, workdir: str) -> None:
    """Run a dbt command inside the container; raise on non-zero exit."""
    exit_code, output = container.exec_run(cmd, workdir=workdir)
    print(output.decode())
    if exit_code != 0:
        raise RuntimeError(f"`{cmd}` failed (exit {exit_code})")


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
        base = (
            f"--target {stage_config.target}"
            f" --profiles-dir {stage_config.profiles_dir}"
        )
        selector = f"--select {stage_config.select}" if stage_config.select else ""

        _dbt_exec(container, f"dbt run {selector} {base}".strip(), stage_config.project_dir)

        if stage_config.run_tests:
            _dbt_exec(container, f"dbt test {selector} {base}".strip(), stage_config.project_dir)

        if stage_config.generate_docs:
            _dbt_exec(container, f"dbt docs generate {base}".strip(), stage_config.project_dir)

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
