<p align="center">
  <img src="assets/slurm-manager.png" alt="Slurm Manager" width="180">
</p>

# slurm_manager

Submit Slurm jobs through slurmrestd and receive live logs, status, and heartbeats through Redis.

## Setup

```bash
pip install -e .
cp .env.example .env
```

Set `SLURM_JWT` in your local `.env` file. The example contains the RA REST URL
and API version. This credential file is ignored by Git and never uploaded.
The submitter needs access to slurmrestd and Redis.

## Simple job

Run the included ten-second Bash script using the cluster's default resources:

```python
from slurm_manager.slurm_manager import SlurmManager

manager = SlurmManager(redis_host="bec-slurm.psi.ch")
try:
    job = manager.submit_job(
        script_path="examples/print_for_10s.sh", working_directory="/das/home/your_username"
    )
    job.wait()
finally:
    manager.shutdown()
```

`script_path` refers to a local Bash file whose contents are uploaded with the job.
For optional module loads or environment activation, pass `env_path="activate.sh"`
with a local Bash activation script. Additional files used by the workload are not uploaded.
Replace `working_directory` with an existing directory accessible on the compute node.

## Customized job

Choose resources and log files, and receive live output and status updates:

```python
from slurm_manager.job_options import JobOptions
from slurm_manager.slurm_manager import SlurmManager

manager = SlurmManager(redis_host="bec-slurm.psi.ch")
try:
    job = manager.submit_job(
        script_path="examples/print_for_10s.sh",
        working_directory="/das/home/your_username",
        job_options=JobOptions(
            name="my-analysis",
            partition="day",
            tasks=1,
            time_limit_minutes=120,
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

Supported partitions are `hour`, `day`, `week`, and `gpu`. Omit `job_options` to use
one task and the cluster's default partition and time limit. `JobOptions` also
accepts `account` and `qos`. Requesting tasks reserves a Slurm allocation; the
workload script runs once and must launch any parallel processes itself.

`time_limit_minutes` limits job runtime in minutes. `job.wait(timeout=...)` limits
how long the client waits, in seconds; it raises on failure or timeout. A timeout
does not cancel the job automatically, so the example explicitly calls
`job.cancel()`. `manager.shutdown()` disconnects without cancelling jobs.
Callbacks support `log`, `status`, and `heartbeat` and are attached before execution.
