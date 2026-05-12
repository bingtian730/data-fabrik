from __future__ import annotations

from pipelines.shared.base import BasePipeline
from pipelines.shared.registry import TaskBuilder, get_builder


class YamlPipeline(BasePipeline):
    """Concrete pipeline resolved entirely from the global task-builder registry.

    This is the default config-driven implementation: a YAML file produces a
    `PipelineConfig`, which is then handed to `YamlPipeline.build_dag()`.
    """

    def _resolve_task_builder(self, stage: str, type_: str) -> TaskBuilder:
        return get_builder(stage, type_)
