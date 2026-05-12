from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PRESETS = ("@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@once")


class ScheduleConfig(BaseModel):
    """When and how a pipeline runs.

    Specify exactly one of `cron` or `preset` (or omit both for an
    unscheduled DAG that only runs on manual trigger).
    """

    model_config = ConfigDict(extra="forbid")

    cron: str | None = Field(
        default=None,
        description="Standard 5-field cron expression, e.g. '0 6 * * *'.",
    )
    preset: Literal["@hourly", "@daily", "@weekly", "@monthly", "@yearly", "@once"] | None = Field(
        default=None,
        description="Airflow preset shortcut.",
    )
    start_date: datetime = Field(
        default_factory=lambda: datetime(2026, 1, 1),
        description="Earliest logical date for a DAG run.",
    )
    end_date: datetime | None = Field(
        default=None,
        description="Stop scheduling new runs after this date.",
    )
    timezone: str = Field(default="UTC")
    catchup: bool = Field(
        default=False,
        description="Backfill missed runs between start_date and now.",
    )
    max_active_runs: int = Field(default=1, ge=1)
    retries: int = Field(default=3, ge=0)
    retry_delay_minutes: int = Field(default=5, ge=0)

    @model_validator(mode="after")
    def _exactly_one_schedule_spec(self) -> ScheduleConfig:
        if self.cron and self.preset:
            raise ValueError("Specify exactly one of `cron` or `preset`, not both.")
        return self

    def to_airflow_schedule(self) -> str | None:
        return self.cron or self.preset

    def to_airflow_default_args(self) -> dict[str, Any]:
        return {
            "retries": self.retries,
            "retry_delay": timedelta(minutes=self.retry_delay_minutes),
        }
