from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class _DestinationBase(BaseModel):
    model_config = ConfigDict(extra="forbid")


class S3PublishDestinationConfig(_DestinationBase):
    """Copy curated data to a downstream-readable S3 location."""

    type: Literal["s3_publish"]
    source_bucket: str = "datafabrik-curated"
    source_prefix: str
    dest_bucket: str
    dest_prefix: str


class SlackNotifyDestinationConfig(_DestinationBase):
    """Post a completion notification to a Slack channel."""

    type: Literal["slack_notify"]
    connection_id: str = "slack_default"
    channel: str
    message: str = Field(description="Supports Jinja templating.")


class WebhookDestinationConfig(_DestinationBase):
    """Call an HTTP webhook to notify a downstream system."""

    type: Literal["webhook"]
    url: str
    method: Literal["POST", "PUT"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)


DestinationConfig = Annotated[
    Union[S3PublishDestinationConfig, SlackNotifyDestinationConfig, WebhookDestinationConfig],
    Field(discriminator="type"),
]
