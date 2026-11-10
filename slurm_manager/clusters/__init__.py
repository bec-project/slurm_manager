"""Built-in Slurm cluster definitions."""

from enum import StrEnum


class Cluster(StrEnum):
    """Built-in clusters accepted by the Slurm manager factory."""

    RA = "ra"
    SFONLINE = "sfonline"
    SLSONLINE = "slsonline"
