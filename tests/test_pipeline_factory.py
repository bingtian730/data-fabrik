"""Smoke tests for the dynamic pipeline factory.

Run inside the Airflow container so Airflow + pydantic + PyYAML are available:

    docker compose exec -T airflow-scheduler python < tests/test_pipeline_factory.py
"""
from __future__ import annotations

from pathlib import Path

from airflow import DAG

from pipelines.shared import (
    BasePipeline,
    PipelineConfig,
    YamlPipeline,
    all_builders,
    build_dag_from_yaml,
    build_dags_from_directory,
)
from pipelines.shared.base import STAGES
from pipelines.shared.schema import (
    DbtTransformConfig,
    PipelineStages,
    RowCountValidation,
    S3CsvSourceConfig,
    S3PublishDestinationConfig,
    ScheduleConfig,
)


def _example_config_path() -> Path:
    return Path("/opt/airflow/configs/pipelines/example_customer.yaml")


def _make_config(**overrides) -> PipelineConfig:
    stages = PipelineStages(
        ingestion=S3CsvSourceConfig(
            type="s3_csv", source_bucket="x", source_key="y/*.csv"
        ),
        transformation=DbtTransformConfig(type="dbt", select="example"),
        validation=[
            RowCountValidation(
                type="row_count", connection_id="c", table="t", min_rows=1
            ),
        ],
        delivery=S3PublishDestinationConfig(
            type="s3_publish",
            source_prefix="p",
            dest_bucket="b",
            dest_prefix="p",
        ),
    )
    return PipelineConfig(
        pipeline_id=overrides.pop("pipeline_id", "unit_test_pipeline"),
        schedule=overrides.pop("schedule", ScheduleConfig()),
        stages=overrides.pop("stages", stages),
        **overrides,
    )


def test_base_pipeline_is_abstract() -> None:
    try:
        BasePipeline(config=_make_config())  # type: ignore[abstract]
    except TypeError as exc:
        assert "abstract" in str(exc).lower()
    else:
        raise AssertionError("BasePipeline should be abstract")


def test_registry_has_at_least_one_builder_per_stage() -> None:
    builders = all_builders()
    covered = {stage for stage, _ in builders}
    for stage in STAGES:
        assert stage in covered, f"No builders registered for stage={stage!r}"


def test_yaml_pipeline_builds_dag_with_all_four_stages() -> None:
    config = _make_config()
    dag = YamlPipeline(config).build_dag()

    assert isinstance(dag, DAG)
    assert dag.dag_id == config.pipeline_id
    top_level = {t.task_id for t in dag.tasks if "." not in t.task_id}
    assert "ingestion" in top_level
    assert "transformation" in top_level
    assert "delivery" in top_level


def test_validation_runs_as_parallel_task_group() -> None:
    stages = PipelineStages(
        ingestion=S3CsvSourceConfig(type="s3_csv", source_bucket="x", source_key="y"),
        validation=[
            RowCountValidation(type="row_count", connection_id="c", table="t1"),
            RowCountValidation(type="row_count", connection_id="c", table="t2", min_rows=10),
        ],
    )
    config = _make_config(stages=stages)
    dag = YamlPipeline(config).build_dag()

    group_ids = [g.group_id for g in dag.task_group_dict.values() if g.group_id]
    assert "validation" in group_ids


def test_missing_optional_stage_becomes_empty_operator() -> None:
    stages = PipelineStages(
        ingestion=S3CsvSourceConfig(type="s3_csv", source_bucket="x", source_key="y"),
    )
    config = _make_config(stages=stages)
    dag = YamlPipeline(config).build_dag()

    task_ids = {t.task_id for t in dag.tasks}
    assert "transformation_skipped" in task_ids
    assert "delivery_skipped" in task_ids
    assert "validation_skipped" in task_ids


def test_schedule_config_passed_to_dag() -> None:
    config = _make_config(
        schedule=ScheduleConfig(cron="0 12 * * *", retries=5, max_active_runs=3)
    )
    dag = YamlPipeline(config).build_dag()
    assert dag.schedule_interval == "0 12 * * *"
    assert dag.max_active_runs == 3
    assert dag.default_args["retries"] == 5


def test_factory_builds_dag_from_yaml_file() -> None:
    path = _example_config_path()
    assert path.exists(), f"Example config not found at {path}"
    dag = build_dag_from_yaml(path)
    assert dag.dag_id == "example_customer_daily"


def test_factory_discovers_all_yaml_in_directory() -> None:
    dags = build_dags_from_directory(_example_config_path().parent)
    assert "example_customer_daily" in dags


def smoke() -> None:
    test_base_pipeline_is_abstract()
    test_registry_has_at_least_one_builder_per_stage()
    test_yaml_pipeline_builds_dag_with_all_four_stages()
    test_validation_runs_as_parallel_task_group()
    test_missing_optional_stage_becomes_empty_operator()
    test_schedule_config_passed_to_dag()
    test_factory_builds_dag_from_yaml_file()
    test_factory_discovers_all_yaml_in_directory()
    print("All pipeline factory smoke tests passed.")


if __name__ == "__main__":
    smoke()
