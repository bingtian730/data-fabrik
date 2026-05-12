from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _ValidationBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RowCountValidation(_ValidationBase):
    """Assert a table has a row count within [min_rows, max_rows]."""

    type: Literal["row_count"]
    connection_id: str
    table: str
    min_rows: int = Field(default=1, ge=0)
    max_rows: int | None = Field(default=None, ge=0)


class SchemaValidation(_ValidationBase):
    """Assert a table's columns exactly match an expected list."""

    type: Literal["schema"]
    connection_id: str
    table: str
    expected_columns: list[str] = Field(min_length=1)


class FreshnessValidation(_ValidationBase):
    """Assert a timestamp column has data newer than max_lag_minutes."""

    type: Literal["freshness"]
    connection_id: str
    table: str
    timestamp_column: str
    max_lag_minutes: int = Field(default=1440, gt=0)


ValidationRuleConfig = Annotated[
    Union[RowCountValidation, SchemaValidation, FreshnessValidation],
    Field(discriminator="type"),
]
