"""Smoke tests for the dynamic pipeline factory.

Run inside the Airflow container so Airflow + pydantic + PyYAML are available:

    docker compose exec airflow-webserver \
        python -m pytest /opt/airflow/pipelines/tests/test_pipeline_factory.py -v

Or as a one-liner that skips pytest by just importing and asserting:

    docker compose exec airflow-webserver python -c \
        "from pipelines.tests.test_pipeline_factory import smoke; smoke()"
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG

from pipelines.shared import (
    BasePipeline,
    PipelineConfig,
    StageConfig,
    YamlPipeline,
    all_builders,
    build_dag_from_yaml,
    build_dags_from_directory,
)
from pipelines.shared.base import STAGES


def _example_config_path() -> Path:
    return Path("/opt/airflow/configs/pipelines/example_customer.yaml")


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
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == set(STAGES), f"Expected {STAGES}, got {sorted(task_ids)}"

    for upstream, downstream in zip(STAGES, STAGES[1:]):
        up = dag.get_task(upstream)
        down = dag.get_task(downstream)
        assert down in up.downstream_list, (
            f"{upstream} -> {downstream} edge missing"
        )


def test_missing_stage_becomes_empty_operator() -> None:
    config = _make_config()
    config.stages.pop("validation")
    dag = YamlPipeline(config).build_dag()

    task_ids = {t.task_id for t in dag.tasks}
    assert "validation_skipped" in task_ids
    assert "validation" not in task_ids


def test_factory_builds_dag_from_yaml_file() -> None:
    path = _example_config_path()
    assert path.exists(), f"Example config not found at {path}"
    dag = build_dag_from_yaml(path)
    assert dag.dag_id == "example_customer_daily"


def test_factory_discovers_all_yaml_in_directory() -> None:
    dags = build_dags_from_directory(_example_config_path().parent)
    assert "example_customer_daily" in dags


def _make_config() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="unit_test_pipeline",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        stages={
            "ingestion": StageConfig(type="s3_csv", config={"source_bucket": "x", "source_key": "y"}),
            "transformation": StageConfig(type="sql", config={"connection_id": "c", "sql_file": "f"}),
            "validation": StageConfig(type="row_count", config={"connection_id": "c", "table": "t"}),
            "delivery": StageConfig(type="s3_publish", config={"source_prefix": "p", "dest_bucket": "b", "dest_prefix": "p"}),
        },
    )


def smoke() -> None:
    """Run every test sequentially without pytest."""
    test_base_pipeline_is_abstract()
    test_registry_has_at_least_one_builder_per_stage()
    test_yaml_pipeline_builds_dag_with_all_four_stages()
    test_missing_stage_becomes_empty_operator()
    test_factory_builds_dag_from_yaml_file()
    test_factory_discovers_all_yaml_in_directory()
    print("All pipeline factory smoke tests passed.")


if __name__ == "__main__":
    smoke()
