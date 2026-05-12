from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _TransformBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DbtTransformConfig(_TransformBase):
    """Run dbt models for this pipeline."""

    type: Literal["dbt"]
    project_dir: str = "/usr/app/dbt"
    profiles_dir: str = "/usr/app/dbt"
    select: str | None = Field(default=None, description="dbt --select selector.")
    target: str = "dev"


class SqlTransformConfig(_TransformBase):
    """Execute a SQL script against a configured connection."""

    type: Literal["sql"]
    connection_id: str
    sql_file: str
    parameters: dict[str, Any] = Field(default_factory=dict)


class SparkTransformConfig(_TransformBase):
    """Submit a Spark job (e.g. to an EMR cluster)."""

    type: Literal["spark"]
    cluster_id: str | None = None
    script: str
    arguments: list[str] = Field(default_factory=list)


TransformationConfig = Annotated[
    Union[DbtTransformConfig, SqlTransformConfig, SparkTransformConfig],
    Field(discriminator="type"),
]
