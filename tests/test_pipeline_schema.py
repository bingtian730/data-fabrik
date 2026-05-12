"""Schema-level validation tests.

Cover both the happy path (valid YAML loads) and the negative cases that
prove invalid configs fail fast with a clear error.

    docker compose exec -T airflow-scheduler python < tests/test_pipeline_schema.py
"""
from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from pipelines.shared.schema import (
    DbtTransformConfig,
    PipelineConfig,
    PipelineStages,
    RowCountValidation,
    S3CsvSourceConfig,
    S3PublishDestinationConfig,
    ScheduleConfig,
)


_SCHEMA_PATH = Path("/opt/airflow/pipelines/shared/schema/pipeline_config.schema.json")


def _minimal_config() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="ok_pipeline",
        stages=PipelineStages(
            ingestion=S3CsvSourceConfig(
                type="s3_csv", source_bucket="b", source_key="k"
            ),
        ),
    )


def test_minimal_config_is_valid() -> None:
    cfg = _minimal_config()
    assert cfg.pipeline_id == "ok_pipeline"
    assert cfg.stages.ingestion.type == "s3_csv"
    assert cfg.schedule.preset is None
    assert cfg.schedule.cron is None


def test_pipeline_id_rejects_invalid_characters() -> None:
    try:
        PipelineConfig(
            pipeline_id="bad id with spaces",
            stages=PipelineStages(
                ingestion=S3CsvSourceConfig(type="s3_csv", source_bucket="b", source_key="k")
            ),
        )
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for invalid pipeline_id")


def test_unknown_top_level_field_rejected() -> None:
    try:
        PipelineConfig.model_validate(
            {
                "pipeline_id": "ok",
                "stages": {"ingestion": {"type": "s3_csv", "source_bucket": "b", "source_key": "k"}},
                "this_field_does_not_exist": True,
            }
        )
    except ValidationError as exc:
        assert "extra_forbidden" in str(exc).lower() or "extra inputs are not permitted" in str(exc).lower()
        return
    raise AssertionError("Expected ValidationError for unknown field")


def test_source_discriminator_rejects_unknown_type() -> None:
    try:
        PipelineConfig.model_validate(
            {
                "pipeline_id": "ok",
                "stages": {"ingestion": {"type": "ftp", "host": "x"}},
            }
        )
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for unknown source type")


def test_source_rejects_extra_fields() -> None:
    try:
        S3CsvSourceConfig(
            type="s3_csv",
            source_bucket="b",
            source_key="k",
            mystery_field="surprise",  # type: ignore[call-arg]
        )
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for extra source field")


def test_schedule_rejects_both_cron_and_preset() -> None:
    try:
        ScheduleConfig(cron="0 6 * * *", preset="@daily")
    except ValidationError as exc:
        assert "cron" in str(exc).lower() or "preset" in str(exc).lower()
        return
    raise AssertionError("Expected ValidationError when both cron and preset set")


def test_validation_rules_are_a_list() -> None:
    cfg = PipelineConfig(
        pipeline_id="ok",
        stages=PipelineStages(
            ingestion=S3CsvSourceConfig(type="s3_csv", source_bucket="b", source_key="k"),
            validation=[
                RowCountValidation(type="row_count", connection_id="c", table="t1"),
                RowCountValidation(type="row_count", connection_id="c", table="t2", min_rows=10),
            ],
        ),
    )
    assert len(cfg.stages.validation) == 2
    assert cfg.stages.validation[1].min_rows == 10


def test_row_count_min_rows_must_be_non_negative() -> None:
    try:
        RowCountValidation(type="row_count", connection_id="c", table="t", min_rows=-1)
    except ValidationError:
        return
    raise AssertionError("Expected ValidationError for negative min_rows")


def test_schedule_retries_default_propagates_to_default_args() -> None:
    schedule = ScheduleConfig(retries=7, retry_delay_minutes=10)
    args = schedule.to_airflow_default_args()
    assert args["retries"] == 7
    assert args["retry_delay"].total_seconds() == 600


def test_schema_export_matches_committed_file() -> None:
    """The committed JSON Schema must match what the models currently emit."""
    if not _SCHEMA_PATH.exists():
        # Not a hard failure - allow running outside the container
        print(f"  (skipped: {_SCHEMA_PATH} not found)")
        return
    on_disk = json.loads(_SCHEMA_PATH.read_text())
    fresh = PipelineConfig.model_json_schema()
    # The exported file has $schema and $id added by the CLI; strip for comparison
    on_disk.pop("$schema", None)
    on_disk.pop("$id", None)
    assert on_disk == fresh, (
        "Committed JSON Schema is stale. Regenerate:\n"
        "  python -m pipelines.shared.schema -o pipelines/shared/schema/pipeline_config.schema.json"
    )


def test_example_yaml_loads_against_schema() -> None:
    """The shipped example must parse cleanly through PipelineConfig."""
    path = Path("/opt/airflow/configs/pipelines/example_customer.yaml")
    cfg = PipelineConfig.from_yaml(path)
    assert cfg.pipeline_id == "example_customer_daily"
    assert cfg.stages.ingestion.type == "s3_csv"
    assert len(cfg.stages.validation) == 2
    assert cfg.schedule.cron == "0 6 * * *"


def smoke() -> None:
    test_minimal_config_is_valid()
    test_pipeline_id_rejects_invalid_characters()
    test_unknown_top_level_field_rejected()
    test_source_discriminator_rejects_unknown_type()
    test_source_rejects_extra_fields()
    test_schedule_rejects_both_cron_and_preset()
    test_validation_rules_are_a_list()
    test_row_count_min_rows_must_be_non_negative()
    test_schedule_retries_default_propagates_to_default_args()
    test_schema_export_matches_committed_file()
    test_example_yaml_loads_against_schema()
    print("All pipeline schema tests passed.")


if __name__ == "__main__":
    smoke()
