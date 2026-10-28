"""Validated user-facing Slurm job options."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class JobOptions(BaseModel):
    """Resources and output settings for one job.

    An omitted partition uses the cluster default; an omitted time limit uses
    the selected partition's default. Time limits
    are wall-clock minutes, independently of JobFuture.wait()'s timeout in seconds.
    Output paths refer to the compute node and may use Slurm's %j job-ID pattern.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: NonEmptyString | None = Field(
        default=None, description="Job name shown in Slurm. Omit to use the scheduler default."
    )
    partition: Literal["hour", "day", "week", "gpu"] | None = Field(
        default=None,
        description=(
            "RA partition: hour (default/max 1 hour), day (default 4 hours, max 24 hours), "
            "week (default 2 days, max 8 days), or gpu (default 1 hour, max 7 days). "
            "Omit to use the cluster default, currently hour."
        ),
    )
    account: NonEmptyString | None = Field(
        default=None,
        description=(
            "Slurm accounting/project account; omit to use your default account association. "
            "The account must also be allowed by the selected partition: hour/day allow "
            "user and proprietary; gpu allows user and gpu-csaxs; week denies gpu-csaxs "
            "and tomcat. These partition rules do not establish your account membership."
        ),
    )
    qos: NonEmptyString | None = Field(
        default=None,
        description=(
            "Quality of Service controlling scheduling priority and resource limits. "
            "Must be allowed by both your association and the selected partition. "
            "Omit to use your association's default job QoS. The gpu partition allows "
            "gpu-default, gpu-day, gpu-hour, and gpu-csaxs, and additionally applies "
            "partition QoS gpu-default. Partition QoS is separate from requested job QoS."
        ),
    )
    tasks: int = Field(
        default=1,
        ge=1,
        strict=True,
        description=(
            "Number of Slurm tasks requested for the allocation; defaults to one. "
            "The worker executes the workload script once, without launching MPI ranks. "
            "This does not specify CPUs per task."
        ),
    )
    time_limit_minutes: int | None = Field(
        default=None,
        ge=1,
        strict=True,
        description=(
            "Maximum job wall-clock runtime in minutes. Slurm terminates the job at this limit. "
            "Omit to use the selected partition's default: hour 60, day 240, week 2880, "
            "gpu 60 minutes. Partition maxima are respectively 60, 1440, 11520, and "
            "10080 minutes; account/QoS limits may impose additional restrictions. "
            "Separate from JobFuture.wait(timeout), which is measured in seconds."
        ),
    )
    standard_output: NonEmptyString | None = Field(
        default=None,
        description=(
            "Compute-node file path for stdout, in addition to Redis forwarding. "
            "Supports Slurm patterns such as %j for the job ID. Relative paths use the job's "
            "working directory; the parent directory must exist. Omit for Slurm's default output."
        ),
    )
    standard_error: NonEmptyString | None = Field(
        default=None,
        description=(
            "Compute-node file path for stderr, in addition to Redis forwarding. "
            "Supports %j for the job ID. Relative paths use the job's working directory; "
            "the parent directory must exist. Omit to use Slurm's default stderr handling."
        ),
    )

    ################################################
    #  Public Serialization                        #
    ################################################

    def to_rest(self) -> dict[str, Any]:
        """Serialize validated options into slurmrestd job-description fields.

        Returns:
            dict[str, Any]: REST options with unset values omitted and runtime expressed
                as an optional integer.
        """
        options = self.model_dump(exclude={"time_limit_minutes"}, exclude_none=True)
        if self.time_limit_minutes is not None:
            options["time_limit"] = {"set": True, "number": self.time_limit_minutes}
        return options
