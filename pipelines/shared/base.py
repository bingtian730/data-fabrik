from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.registry import TaskBuilder
    from pipelines.shared.schema import PipelineConfig


STAGES: tuple[str, ...] = ("ingestion", "transformation")


# ── Airflow callback helpers ───────────────────────────────────────────────────

def _dag_success(context: dict) -> None:
    from pipelines.shared.metadata import upsert_pipeline_run
    upsert_pipeline_run(context, "success")


def _dag_failure(context: dict) -> None:
    from pipelines.shared.metadata import upsert_pipeline_run
    exc = context.get("exception")
    upsert_pipeline_run(context, "failed", error=str(exc) if exc else None)


def _task_success(context: dict) -> None:
    from pipelines.shared.metadata import write_task_run
    write_task_run(context, "success")


def _task_failure(context: dict) -> None:
    from pipelines.shared.metadata import write_task_run
    exc = context.get("exception")
    write_task_run(context, "failed", error=str(exc) if exc else None)


class BasePipeline(ABC):
    """Four-stage data pipeline template.

    Subclasses implement `_resolve_task_builder` to decide how to map a
    `(stage, type)` pair to a callable returning an Airflow operator.
    Stage wiring (including the parallel `validation` rule fan-out) is
    handled here so adding a new pipeline is a config change rather
    than new Python.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    @property
    def pipeline_id(self) -> str:
        return self.config.pipeline_id

    @abstractmethod
    def _resolve_task_builder(self, stage: str, type_: str) -> TaskBuilder:
        """Return the builder for a stage type, or raise KeyError."""

    def build_dag(self, **dag_overrides: Any) -> DAG:
        from pipelines.shared.metadata import upsert_lineage

        schedule = self.config.schedule
        dag_kwargs: dict[str, Any] = dict(
            dag_id=self.config.dag_id,
            description=self.config.description,
            schedule=schedule.to_airflow_schedule(),
            start_date=schedule.start_date,
            end_date=schedule.end_date,
            catchup=schedule.catchup,
            max_active_runs=schedule.max_active_runs,
            tags=self.config.tags,
            on_success_callback=_dag_success,
            on_failure_callback=_dag_failure,
            default_args={
                "owner": self.config.owner,
                **schedule.to_airflow_default_args(),
                **self.config.default_args,
            },
        )
        dag_kwargs.update(dag_overrides)
        dag = DAG(**dag_kwargs)

        # Persist topology so lineage is queryable without a run.
        try:
            upsert_lineage(self.config)
        except Exception:
            pass  # non-fatal — DAG must still register

        previous: BaseOperator | None = None
        for stage in STAGES:
            node = self._build_stage(stage, dag)
            if previous is not None:
                previous >> node
            previous = node

        return dag

    def _build_stage(self, stage: str, dag: DAG) -> BaseOperator:
        stage_value = getattr(self.config.stages, stage)

        if stage_value is None:
            return EmptyOperator(task_id=f"{stage}_skipped", dag=dag)

        builder = self._resolve_task_builder(stage, stage_value.type)
        op = builder(
            stage=stage,
            stage_config=stage_value,
            pipeline=self.config,
            dag=dag,
        )
        op.on_success_callback = _task_success
        op.on_failure_callback = _task_failure
        return op
