from __future__ import annotations

from typing import TYPE_CHECKING

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


@register("validation", "row_count")
def row_count(
    *,
    stage: str,
    stage_config: RowCountValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=stage_config.connection_id)
        count = hook.get_first(f"SELECT COUNT(*) FROM {stage_config.table}")[0]
        print(f"[row_count] {stage_config.table}: {count} rows")
        if count < stage_config.min_rows:
            raise ValueError(f"{stage_config.table} has {count} rows, minimum is {stage_config.min_rows}")
        if stage_config.max_rows is not None and count > stage_config.max_rows:
            raise ValueError(f"{stage_config.table} has {count} rows, maximum is {stage_config.max_rows}")
        print(f"[row_count] PASSED")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("validation", "schema")
def schema(
    *,
    stage: str,
    stage_config: SchemaValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=stage_config.connection_id)
        schema_name, table_name = (stage_config.table.split(".", 1) + [""])[:2]
        rows = hook.get_records(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            parameters=(schema_name, table_name),
        )
        actual = [r[0] for r in rows]
        expected = stage_config.expected_columns
        if actual != expected:
            raise ValueError(f"{stage_config.table} columns mismatch.\n  expected: {expected}\n  actual:   {actual}")
        print(f"[schema] PASSED — {actual}")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)


@register("validation", "freshness")
def freshness(
    *,
    stage: str,
    stage_config: FreshnessValidation,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=stage_config.connection_id)
        lag_minutes = hook.get_first(
            f"""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX({stage_config.timestamp_column}))) / 60
            FROM {stage_config.table}
            """
        )[0]
        print(f"[freshness] {stage_config.table}.{stage_config.timestamp_column}: lag = {lag_minutes:.1f} min")
        if lag_minutes is None or lag_minutes > stage_config.max_lag_minutes:
            raise ValueError(
                f"{stage_config.table} freshness lag {lag_minutes:.1f} min exceeds max {stage_config.max_lag_minutes} min"
            )
        print(f"[freshness] PASSED")

    return PythonOperator(task_id=stage, python_callable=_run, dag=dag)
