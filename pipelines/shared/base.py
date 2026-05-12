from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.registry import TaskBuilder
    from pipelines.shared.schema import PipelineConfig

STAGES: tuple[str, ...] = ("ingestion", "transformation", "validation", "delivery")


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
            default_args={
                "owner": self.config.owner,
                **schedule.to_airflow_default_args(),
                **self.config.default_args,
            },
        )
        dag_kwargs.update(dag_overrides)
        dag = DAG(**dag_kwargs)

        previous: BaseOperator | TaskGroup | None = None
        for stage in STAGES:
            node = self._build_stage(stage, dag)
            if previous is not None:
                previous >> node
            previous = node

        return dag

    def _build_stage(self, stage: str, dag: DAG) -> BaseOperator | TaskGroup:
        stage_value = getattr(self.config.stages, stage)

        # Validation: parallel TaskGroup over N rules
        if stage == "validation":
            rules = stage_value
            if not rules:
                return EmptyOperator(task_id="validation_skipped", dag=dag)

            # Suffix duplicate types so task ids stay unique within the group.
            type_totals: dict[str, int] = {}
            for r in rules:
                type_totals[r.type] = type_totals.get(r.type, 0) + 1
            seen: dict[str, int] = {}

            with TaskGroup(group_id="validation", dag=dag) as tg:
                for rule in rules:
                    seen[rule.type] = seen.get(rule.type, 0) + 1
                    task_id = (
                        f"{rule.type}_{seen[rule.type]}"
                        if type_totals[rule.type] > 1
                        else rule.type
                    )
                    builder = self._resolve_task_builder(stage, rule.type)
                    builder(
                        stage=task_id,
                        stage_config=rule,
                        pipeline=self.config,
                        dag=dag,
                    )
            return tg

        # Other stages: optional single operator
        if stage_value is None:
            return EmptyOperator(task_id=f"{stage}_skipped", dag=dag)

        builder = self._resolve_task_builder(stage, stage_value.type)
        return builder(
            stage=stage,
            stage_config=stage_value,
            pipeline=self.config,
            dag=dag,
        )
