"""SLSOnline connection settings and typed job options."""

from typing import Literal

from slurm_manager.clusters.cluster_info import ClusterInfo
from slurm_manager.clusters.job_options import JobOptions

CLUSTER_INFO: ClusterInfo = ClusterInfo(url="https://sls-slurm-001.psi.ch", api_version="v0.0.44")


SLSOnlinePartition = Literal["hour", "day", "week"]


class SLSOnlineJobOptions(JobOptions):
    """Job options for the SLSOnline cluster."""

    partition: SLSOnlinePartition = "hour"  # pyright: ignore[reportIncompatibleVariableOverride]
