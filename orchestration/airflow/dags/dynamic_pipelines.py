"""Register Airflow DAGs from pipeline YAML configs.

Hand-crafted pipelines live in /opt/airflow/configs/pipelines.yaml (multi-doc).
Pipelines submitted via the onboarding UI are written as individual files under
/opt/airflow/configs/pipelines/ and are picked up automatically from there.
"""
from __future__ import annotations

from pathlib import Path

from pipelines.shared.factory import build_dags_from_directory, build_dags_from_yaml

CONFIG_FILE = Path("/opt/airflow/configs/pipelines.yaml")
CONFIG_DIR  = Path("/opt/airflow/configs/pipelines")

for dag_id, dag in build_dags_from_yaml(CONFIG_FILE).items():
    globals()[dag_id] = dag

for dag_id, dag in build_dags_from_directory(CONFIG_DIR).items():
    globals()[dag_id] = dag
