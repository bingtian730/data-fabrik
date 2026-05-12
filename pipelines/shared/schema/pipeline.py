from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from pipelines.shared.schema.destinations import DestinationConfig
from pipelines.shared.schema.scheduling import ScheduleConfig
from pipelines.shared.schema.sources import SourceConfig
from pipelines.shared.schema.transformations import TransformationConfig
from pipelines.shared.schema.validations import ValidationRuleConfig


class PipelineStages(BaseModel):
    """The four stages of a DataFabrik pipeline.

    - `ingestion` is required (every pipeline has a source).
    - `transformation` and `delivery` are optional; a missing stage is
      rendered as an EmptyOperator in the DAG so downstream stages still
      depend on its completion.
    - `validation` is a list — each rule runs in parallel after
      transformation and gates the delivery stage.
    """

    model_config = ConfigDict(extra="forbid")

    ingestion: SourceConfig
    transformation: TransformationConfig | None = None
    validation: list[ValidationRuleConfig] = Field(default_factory=list)
    delivery: DestinationConfig | None = None


class PipelineConfig(BaseModel):
    """Customer pipeline schema.

    A pipeline YAML is loaded with `PipelineConfig.from_yaml(path)`. The
    factory then builds an Airflow DAG by wiring the four stages in
    order: ingestion -> transformation -> [validation rules] -> delivery.
    """

    model_config = ConfigDict(extra="forbid")

    pipeline_id: str = Field(
        description="Unique identifier; also used as the Airflow DAG id."
    )
    description: str | None = None
    owner: str = "data-platform"
    tags: list[str] = Field(default_factory=list)
    default_args: dict[str, Any] = Field(default_factory=dict)
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    stages: PipelineStages

    @field_validator("pipeline_id")
    @classmethod
    def _valid_dag_id(cls, v: str) -> str:
        if not v or not all(c.isalnum() or c in "_-" for c in v):
            raise ValueError(
                "pipeline_id must be alphanumeric (plus '_' or '-') and non-empty"
            )
        return v

    @property
    def dag_id(self) -> str:
        return self.pipeline_id

    @classmethod
    def from_yaml(cls, path: str | Path) -> PipelineConfig:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        return cls.model_validate(data)
