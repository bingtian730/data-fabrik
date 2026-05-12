"""Backwards-compatible alias for the strongly-typed schema package.

Existing code can keep importing `PipelineConfig` from
`pipelines.shared.config`; the canonical definitions live in
`pipelines.shared.schema`.
"""
from pipelines.shared.schema import PipelineConfig, PipelineStages, ScheduleConfig

__all__ = ["PipelineConfig", "PipelineStages", "ScheduleConfig"]
