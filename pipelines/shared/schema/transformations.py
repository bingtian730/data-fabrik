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
    run_tests: bool = Field(default=True, description="Run dbt test after dbt run.")
    generate_docs: bool = Field(default=False, description="Run dbt docs generate after tests.")


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
    Union[DbtTransformConfig, SqlTransformConfig, SparkTransformConfig],
    Field(discriminator="type"),
]
