"""Validated user-facing Slurm job options."""

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

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
    partition: NonEmptyString | None = Field(
        default=None, description="Slurm partition name. Omit to use the scheduler default."
    )
    account: NonEmptyString | None = Field(
        default=None,
        description=(
            "Slurm accounting/project account; omit to use your default account association. "
            "The account must also be allowed by the selected partition."
        ),
    )
    qos: NonEmptyString | None = Field(
        default=None,
        description=(
            "Quality of Service controlling scheduling priority and resource limits. "
            "Must be allowed by both your association and the selected partition. "
            "Omit to use your association's default job QoS."
        ),
    )
    reservation: NonEmptyString | None = Field(
        default=None, description="Name of an existing Slurm reservation to use."
    )
    constraints: NonEmptyString | None = Field(
        default=None, description="Required scheduler-defined node features."
    )
    dependency: NonEmptyString | None = Field(
        default=None, description="Slurm dependency expression, such as afterok:12345."
    )
    nodes: NonEmptyString | None = Field(
        default=None, description='Slurm node count or range, such as "2" or "2-4".'
    )
    tasks: int = Field(
        default=1,
        ge=1,
        strict=True,
        description=(
            "Total Slurm tasks. Defaults to one unless tasks_per_node is supplied. "
            "The worker executes the workload script once, without launching MPI ranks."
        ),
    )
    tasks_per_node: int | None = Field(
        default=None,
        ge=1,
        le=65533,
        strict=True,
        description="Number of tasks to request per node.",
    )
    tasks_per_core: int | None = Field(
        default=None,
        ge=1,
        le=65535,
        strict=True,
        description="Maximum tasks per core. Use 65535 for no per-core task limit.",
    )
    cpus_per_task: int | None = Field(
        default=None,
        ge=1,
        le=65533,
        strict=True,
        description="Number of CPUs requested per task. Omit to use Slurm's default.",
    )
    memory_per_cpu: int | None = Field(
        default=None, ge=1, lt=2**63, strict=True, description="Memory in MiB per allocated CPU."
    )
    memory_per_node: int | None = Field(
        default=None,
        ge=0,
        lt=2**63,
        strict=True,
        description="Memory in MiB per node. Use 0 to request all memory on each node.",
    )
    tres_per_node: NonEmptyString | None = Field(
        default=None, description="Slurm trackable resource requests per node, such as GPUs."
    )
    tres_per_task: NonEmptyString | None = Field(
        default=None, description="Slurm trackable resource requests per task, such as GPUs."
    )
    exclusive: bool = Field(
        default=False, strict=True, description="Request exclusive access to the allocated nodes."
    )
    shared: list[Literal["none", "oversubscribe", "user", "mcs", "topo"]] | None = Field(
        default=None,
        min_length=1,
        max_length=1,
        description='Job resource sharing mode. Use ["none"] to request exclusive nodes.',
    )
    time_limit: int | None = Field(
        default=None,
        ge=1,
        strict=True,
        description=(
            "Maximum job wall-clock runtime in minutes. Slurm terminates the job at this limit. "
            "Omit to use the selected partition's default. "
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

    @model_validator(mode="after")
    def _validate_resource_options(self) -> Self:
        if self.memory_per_cpu is not None and self.memory_per_node is not None:
            raise ValueError("Specify either memory_per_cpu or memory_per_node")
        if self.exclusive and self.shared is not None:
            raise ValueError("Specify either exclusive=True or shared")
        return self

    ################################################
    #  Public Serialization                        #
    ################################################

    def to_rest(self) -> dict[str, Any]:
        """Serialize validated options into slurmrestd job-description fields.

        Returns:
            dict[str, Any]: REST options with unset values omitted and memory and runtime
                encoded as Slurm optional integers.
        """
        options = self.model_dump(exclude={"exclusive"}, exclude_none=True)
        if self.tasks_per_node is not None and "tasks" not in self.model_fields_set:
            options.pop("tasks", None)
        for field in ("time_limit", "memory_per_cpu", "memory_per_node"):
            if field in options:
                options[field] = {"set": True, "number": options[field]}
        if self.exclusive:
            options["shared"] = ["none"]
        return options
