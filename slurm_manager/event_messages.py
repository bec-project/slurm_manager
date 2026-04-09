"""Models for messages sent between the slurm jobs from the nodes and the slurm manager."""

from __future__ import annotations

import json
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, Field, TypeAdapter


class SlurmBaseMessage(BaseModel):
    """Base class for all messages sent by the job wrapper. It defines the common structure of the messages."""

    msg_type: str = Field(description="Discriminator used to parse incoming job messages.")
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Metadata dictionary that contains additional information abou the message, e.g. msg_received, slurm_job_id, job_id.",
    )


class HeartBeatMessage(SlurmBaseMessage):
    """Heartbeat message sent by the job wrapper to indicate that the job is still alive."""

    msg_type: Literal["heartbeat"] = "heartbeat"

    timestamp: float


class StatusMessage(SlurmBaseMessage):
    """Status message sent by the job wrapper to indicate the current status of the job."""

    msg_type: Literal["status"] = "status"

    status: str


class LogMessage(SlurmBaseMessage):
    """Log message sent by the job wrapper to forward stdout and stderr."""

    msg_type: Literal["log"] = "log"

    log: str


SlurmMessage: TypeAlias = Annotated[
    HeartBeatMessage | StatusMessage | LogMessage, Field(discriminator="msg_type")
]

SLURM_MESSAGE_ADAPTER = TypeAdapter(SlurmMessage)


def parse_slurm_message(payload: str | dict) -> SlurmMessage:
    """Parse a JSON payload into the appropriate slurm message model."""
    if isinstance(payload, str):
        payload = json.loads(payload)
    return SLURM_MESSAGE_ADAPTER.validate_python(payload)
