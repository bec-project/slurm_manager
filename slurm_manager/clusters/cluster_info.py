"""Non-secret connection settings for a Slurm cluster."""

from pydantic import BaseModel, ConfigDict


class ClusterInfo(BaseModel):
    """REST and Redis connection settings for a cluster."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    url: str
    api_version: str
    redis_host: str = "bec-slurm.psi.ch"
    redis_port: int = 6379
    redis_tls: bool = True
