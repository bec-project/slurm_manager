"""SFOnline connection settings and typed job options."""

from typing import Literal

from slurm_manager.clusters.cluster_info import ClusterInfo
from slurm_manager.clusters.job_options import JobOptions

CLUSTER_INFO: ClusterInfo = ClusterInfo(url="https://sf-slurmrest.psi.ch", api_version="v0.0.44")


SFOnlinePartition = Literal["prod", "prod-aramis", "prod-athos", "prod-gpu"]


class SFOnlineJobOptions(JobOptions):
    """Job options for the SFOnline cluster."""

    partition: SFOnlinePartition = "prod"  # pyright: ignore[reportIncompatibleVariableOverride]
