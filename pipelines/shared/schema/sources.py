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
    """Incrementally extract a Postgres table into S3 as Parquet.

    On each run the builder reads the last watermark from
    pipeline_metadata.watermarks, extracts rows where
    `watermark_column > last_watermark`, writes a Parquet file to S3,
    updates the watermark, and logs a row to pipeline_metadata.ingestion_log.
    """

    type: Literal["jdbc"]
    connection_id: str
    table: str = Field(description="Fully-qualified source table, e.g. 'public.stripe_charges_raw'.")
    watermark_column: str = Field(default="updated_at", description="Column used for incremental extraction.")
    watermark_init: str = Field(default="1970-01-01 00:00:00", description="Watermark used on the very first run.")
    dest_bucket: str = "datafabrik-raw"
    dest_prefix: str | None = None


class MinioCsvSourceConfig(_SourceBase):
    """Load a CSV file from MinIO into a Postgres raw schema table."""

    type: Literal["minio_csv"]
    bucket: str = "datafabrik-raw"
    key: str = Field(description="Object key of the CSV in MinIO, e.g. 'wizard/orders/orders_20260522.csv'.")
    table: str = Field(description="Target table name in the raw schema, e.g. 'orders'.")


SourceConfig = Annotated[
    Union[S3CsvSourceConfig, HttpApiSourceConfig, JdbcSourceConfig, MinioCsvSourceConfig],
    Field(discriminator="type"),
]
