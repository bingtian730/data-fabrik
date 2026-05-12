from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pipelines.shared.builders  # noqa: F401  - register builders on import

from pipelines.shared.config import PipelineConfig
from pipelines.shared.yaml_pipeline import YamlPipeline

if TYPE_CHECKING:
    from airflow import DAG


def build_dag_from_yaml(path: str | Path) -> DAG:
    """Load one YAML config and return the Airflow DAG it describes."""
    config = PipelineConfig.from_yaml(path)
    return YamlPipeline(config).build_dag()


def build_dags_from_directory(directory: str | Path) -> dict[str, DAG]:
    """Build a DAG per `*.yaml` / `*.yml` config in `directory`."""
    directory = Path(directory)
    dags: dict[str, DAG] = {}
    if not directory.is_dir():
        return dags

    for path in sorted([*directory.glob("*.yaml"), *directory.glob("*.yml")]):
        dag = build_dag_from_yaml(path)
        dags[dag.dag_id] = dag
    return dags
