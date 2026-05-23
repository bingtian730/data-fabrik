from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _TransformBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SqlTransformConfig(_TransformBase):
    """Execute SQL against Postgres — either inline or from a file."""

    type: Literal["sql"]
    sql: str | None = Field(default=None, description="Inline SQL to execute.")
    sql_file: str | None = Field(default=None, description="Path to a .sql file (used when sql is not set).")
    parameters: dict[str, Any] = Field(default_factory=dict)


class SparkTransformConfig(_TransformBase):
    """Submit a Spark job (e.g. to an EMR cluster)."""

    type: Literal["spark"]
    cluster_id: str | None = None
    script: str
    arguments: list[str] = Field(default_factory=list)


TransformationConfig = Annotated[
    Union[SqlTransformConfig, SparkTransformConfig],
    Field(discriminator="type"),
]
