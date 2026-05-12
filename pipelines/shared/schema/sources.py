from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _SourceBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class S3CsvSourceConfig(_SourceBase):
    """Copy CSV files from a source S3 location to the raw bucket."""

    type: Literal["s3_csv"]
    source_bucket: str
    source_key: str = Field(description="Object key, supports glob (e.g. 'daily/*.csv').")
    dest_bucket: str = "datafabrik-raw"
    dest_prefix: str | None = None


class HttpApiSourceConfig(_SourceBase):
    """Fetch from an HTTP endpoint and land in the raw bucket."""

    type: Literal["http_api"]
    url: str
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    auth_connection_id: str | None = None
    dest_bucket: str = "datafabrik-raw"
    dest_key: str


class JdbcSourceConfig(_SourceBase):
    """Pull from a JDBC source via an Airflow connection."""

    type: Literal["jdbc"]
    connection_id: str
    query: str
    dest_bucket: str = "datafabrik-raw"
    dest_key: str


SourceConfig = Annotated[
    Union[S3CsvSourceConfig, HttpApiSourceConfig, JdbcSourceConfig],
    Field(discriminator="type"),
]
