from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

    from pipelines.shared.config import PipelineConfig, StageConfig
    from pipelines.shared.registry import TaskBuilder

STAGES: tuple[str, ...] = ("ingestion", "transformation", "validation", "delivery")


class BasePipeline(ABC):
    """Four-stage data pipeline template.

    Subclasses implement `_resolve_task_builder` to decide how to map a
    `(stage, type)` pair to a callable that returns an Airflow operator.
    Everything else — DAG construction, stage wiring, skipping missing
    stages — is handled here so adding a new pipeline is a config change
    rather than new Python.
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
        dag_kwargs: dict[str, Any] = dict(
            dag_id=self.config.dag_id,
            description=self.config.description,
            schedule=self.config.schedule,
            start_date=self.config.start_date,
            catchup=self.config.catchup,
            tags=self.config.tags,
            default_args={"owner": self.config.owner, **self.config.default_args},
        )
        dag_kwargs.update(dag_overrides)

        dag = DAG(**dag_kwargs)

        previous: BaseOperator | None = None
        for stage in STAGES:
            op = self._build_stage(stage, dag)
            if previous is not None:
                previous >> op
            previous = op

        return dag

    def _build_stage(self, stage: str, dag: DAG) -> BaseOperator:
        stage_cfg: StageConfig | None = self.config.stages.get(stage)
        if stage_cfg is None:
            return EmptyOperator(task_id=f"{stage}_skipped", dag=dag)

        builder = self._resolve_task_builder(stage, stage_cfg.type)
        return builder(
            stage=stage,
            stage_config=stage_cfg,
            pipeline=self.config,
            dag=dag,
        )
