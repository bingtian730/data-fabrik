from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class StageConfig(BaseModel):
    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    pipeline_id: str
    description: str | None = None
    owner: str = "data-platform"
    schedule: str | None = "@daily"
    start_date: datetime = Field(default_factory=lambda: datetime(2026, 1, 1))
    catchup: bool = False
    tags: list[str] = Field(default_factory=list)
    default_args: dict[str, Any] = Field(default_factory=dict)
    stages: dict[str, StageConfig]

    @field_validator("pipeline_id")
    @classmethod
    def _pipeline_id_is_valid_dag_id(cls, v: str) -> str:
        if not v or not all(c.isalnum() or c in "_-" for c in v):
            raise ValueError(
                "pipeline_id must be alphanumeric (plus '_' or '-') and non-empty"
            )
        return v

    @field_validator("stages")
    @classmethod
    def _stages_known(cls, v: dict[str, StageConfig]) -> dict[str, StageConfig]:
        from pipelines.shared.base import STAGES

        unknown = set(v) - set(STAGES)
        if unknown:
            raise ValueError(
                f"Unknown stage(s): {sorted(unknown)}. Allowed: {list(STAGES)}"
            )
        return v

    @property
    def dag_id(self) -> str:
        return self.pipeline_id

    @classmethod
    def from_yaml(cls, path: str | Path) -> PipelineConfig:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        return cls(**data)
