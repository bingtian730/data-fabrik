"""Discover pipeline YAML configs under /opt/airflow/configs/pipelines/ and
register one Airflow DAG per file via `globals()`.

Adding a new pipeline = adding a YAML file. No Python changes required.
"""
from __future__ import annotations

from pathlib import Path

from pipelines.shared.factory import build_dags_from_directory

CONFIG_DIR = Path("/opt/airflow/configs/pipelines")

for dag_id, dag in build_dags_from_directory(CONFIG_DIR).items():
    globals()[dag_id] = dag
