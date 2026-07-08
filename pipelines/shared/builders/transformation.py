from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.operators.python import PythonOperator

from pipelines.shared.registry import register
from pipelines.shared.schema.transformations import (
    SparkTransformConfig,
    SqlTransformConfig,
)

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.schema import PipelineConfig


@register("transformation", "sql")
def sql(
    *,
    stage: str,
    stage_config: SqlTransformConfig,
    pipeline: PipelineConfig,
    dag: DAG,
) -> BaseOperator:
    def _run(**_):
        import os
        import re
        from pathlib import Path
        from sqlalchemy import create_engine, text as sa_text

        if stage_config.sql:
            sql_str = stage_config.sql
        elif stage_config.sql_file:
            sql_str = Path(stage_config.sql_file).read_text()
        else:
            raise ValueError("SqlTransformConfig requires either 'sql' or 'sql_file'")

        db_url = os.environ["DATABASE_URL"]
        eng = create_engine(db_url, pool_pre_ping=True)
        with eng.begin() as conn:
            for statement in sql_str.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(sa_text(stmt))

        view_re = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"?(\w+)"?\."?(\w+)"?',
            re.IGNORECASE,
        )
        outputs = [
            {"database": "datafabrik", "schema": schema, "view": view}
            for schema, view in view_re.findall(sql_str)
        ]
        for o in outputs:
            print(f"[sql] output → {o['database']}.{o['schema']}.{o['view']}")
        print(f"[sql] executed successfully — {len(outputs)} view(s) created")
        return {"outputs": outputs}

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
