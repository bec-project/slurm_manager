"""RA connection settings and typed job options."""

from typing import Literal

from pydantic import Field

from slurm_manager.clusters.cluster_info import ClusterInfo
from slurm_manager.clusters.job_options import JobOptions

CLUSTER_INFO: ClusterInfo = ClusterInfo(url="https://ra-slurmapi-001.psi.ch", api_version="v0.0.44")


RAPartition = Literal["hour", "day", "week", "gpu"] | None


class RAJobOptions(JobOptions):
    """Job options for the RA cluster, including its typed partition choices."""

    partition: RAPartition = Field(  # pyright: ignore[reportIncompatibleVariableOverride]
        default=None,
        description=(
            "RA partition: hour (default/max 1 hour), day (default 4 hours, max 24 hours), "
            "week (default 2 days, max 8 days), or gpu (default 1 hour, max 7 days). "
            "Omit to use the cluster default, currently hour."
        ),
    )
