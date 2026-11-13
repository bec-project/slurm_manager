<p align="center">
  <img src="assets/slurm-manager.png" alt="Slurm Manager" width="180">
</p>

# BEC Slurm Manager

Submit Slurm jobs through slurmrestd and receive live logs, status, and heartbeats through Redis.

## Setup

```bash
pip install bec-slurm-manager
cp .env.example .env
```

Set `SLURM_JWT` and, if needed, `SLURM_USER` in your local `.env` file.
`SlurmManager.for_cluster()` uses the named cluster’s REST and Redis settings.
Pass a cluster name or a `Cluster` enum member from `slurm_manager.clusters`. The credential file is ignored by Git and stays local.
The submitter needs access to slurmrestd and Redis.
For development from a checkout, install with `pip install -e '.[dev]'`.

## Redis TLS

Redis connections use TLS by default, including the worker on compute nodes.
Certificates and hostnames are verified against each machine's system trust store.
For built-in clusters, `ClusterInfo` defines the Redis host, port, and TLS setting.
When constructing a manager directly, pass `redis_port` for a non-default
listener port or `redis_tls=False` for a local plaintext Redis server.
The standalone worker likewise defaults to TLS; use `--redis-tls=false` for plaintext.

## Simple job

Run the included ten-second Bash script using the cluster's default resources:

```python
from slurm_manager.clusters import Cluster
from slurm_manager.slurm_manager import SlurmManager

manager = SlurmManager.for_cluster(Cluster.RA, env_file=".env")
try:
    job = manager.submit_job(
        script_path="examples/print_for_10s.sh", working_directory="/das/home/your_username"
    )
    job.wait()
finally:
    manager.shutdown()
```

When calling `submit_job()`, pass exactly one of `command`, `script_path`,
or `remote_script_path`. A local Bash file passed as
`script_path` is uploaded with the job. Use `remote_script_path`
for a Bash file accessible from the compute node. Relative paths start at
`working_directory`. Use `command` for Bash command text, such as
`command="python -m my_analysis"`. The worker executes command text directly
in Bash.
For optional module loads or environment activation, pass `env_path="activate.sh"`
with a local Bash activation script. Additional files used by the workload are not uploaded.
Replace `working_directory` with an existing directory accessible on the compute node.

## Customized job

Choose resources and log files, and receive live output and status updates:

```python
from slurm_manager.clusters import Cluster
from slurm_manager.clusters.ra import RAJobOptions
from slurm_manager.slurm_manager import SlurmManager

manager = SlurmManager.for_cluster(Cluster.RA, env_file=".env")
try:
    job = manager.submit_job(
        script_path="examples/print_for_10s.sh",
        working_directory="/das/home/your_username",
        job_options=RAJobOptions(
            name="my-analysis",
            partition="day",
            tasks=1,
            cpus_per_task=4,
            memory_per_node=8192,
            time_limit=120,
            standard_output="/das/home/your_username/slurm_logs/%j.out",
            standard_error="/das/home/your_username/slurm_logs/%j.err",
        ),
        callbacks={
            "log": lambda message: print(message.log, flush=True),
            "status": lambda message: print(f"Job status: {message.status}"),
        },
    )
    try:
        job.wait(timeout=180)
    except TimeoutError:
        job.cancel()
        raise
finally:
    manager.shutdown()
```

The log directory must already exist on the compute node; `%j` expands to the Slurm
job ID. Output is also forwarded through Redis. Workloads must flush their output
buffers for live feedback.

`RAJobOptions` validates the partition against `hour`, `day`, `week`, and `gpu`.
Omit `job_options` to use one task and the cluster's default partition and time
limit. `RAJobOptions` also
accepts `account` and `qos`. Requesting tasks reserves a Slurm allocation; the
workload script runs once and must launch any parallel processes itself.

Use `reservation` to name an existing Slurm reservation and `cpus_per_task` to
request CPUs per task. Choose either `memory_per_cpu` or
`memory_per_node`, both measured in MiB. A per-node value of `0` requests all
memory on each node. Use `exclusive=True` to request exclusive nodes or
`shared` to set a Slurm sharing mode explicitly. Slurm still applies the
requested memory amount or its default.

Use `nodes` for a node count or range such as `"2-4"`, and `tasks_per_node` for
task placement. With `tasks_per_node` set, omit `tasks` to let Slurm infer the
total. Otherwise, omitted `tasks` defaults to one. Use `tasks_per_core` to
limit tasks per CPU core, or set it to `65535` for no per-core limit. Use
`tres_per_node` or `tres_per_task` to request resources such as GPUs,
`constraints` to select nodes by feature, and `dependency` to wait for other
jobs, for example `"afterok:12345"`.

To submit a job that runs after earlier jobs succeed, pass their returned
futures to `submit_job(..., after_ok=prepare)` or
`submit_job(..., after_ok=[prepare, calibrate])`. Use `after_any` to wait until
all of those jobs finish, regardless of outcome. You can combine `after_ok`
and `after_any` to require both conditions. Use futures returned by the
same manager, including completed jobs. For external jobs or other dependency
types, supply a raw expression in `JobOptions.dependency` by itself. The
manager asks Slurm to cancel a dependent job if its dependency can never
be satisfied.

`time_limit` limits job runtime in minutes. `job.wait(timeout=...)` limits
how long the client waits, in seconds; it raises on failure or timeout. A timeout
does not cancel the job automatically, so the example explicitly calls
`job.cancel()`. `manager.shutdown()` disconnects without cancelling jobs.
Callbacks support `log`, `status`, and `heartbeat` and are attached before execution.

Failure status messages start with `[ERROR]: `. If the worker can report the
failure, it includes up to the last 8 KiB of workload stderr under
`stderr (tail):` when stderr is available. Status callbacks receive the full
message, and the exception raised by `job.wait()` includes the failure detail
and stderr tail. If the worker dies before it can report, the manager can
report the failure from Slurm's job record.

`manager.listen("log", callback)` registers a callback for **all current and future
jobs**, even when called before the first submission. Pass the returned IDs to
`manager.unlisten(callback_ids)` to stop listening. Use `job.listen()` or pass
`job_id` to `manager.listen()` to monitor one job only.
