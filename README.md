# slurm_manager

Submit Slurm jobs from Python and receive live status, logs, and heartbeats through Redis.

## Setup

```bash
pip install -e .
```

The submitter needs Python and the Slurm client commands `sbatch`, `scontrol`,
`scancel`, `squeue`, and `sacct`. Compute nodes use the Bash wrapper and Redis worker;
they do not need this Python package or the submitter's Python environment.

Compute nodes need `redis-cli`, `jq`, and `stdbuf`, with the site's Redis module
available. The wrapper, Redis worker, workload, and activation script must be
accessible at the paths passed to Slurm. The Redis worker currently publishes to
`bec-slurm.psi.ch`.

## Submit a job

```python
from slurm_manager.slurm_manager import SlurmManager

manager = SlurmManager(redis_host="bec-slurm.psi.ch")
try:
    job = manager.submit_job(
        script_path="/shared/run_task.sh", env_path="/shared/activate_workload.sh"
    )
    job.listen("log", lambda message: print(message.log))
    job.wait(timeout=60)
finally:
    manager.shutdown()
```

The activation script selects the workload's environment on the compute node.
Jobs are submitted on hold and released after Redis acknowledges the monitoring
subscriptions. Missing heartbeats trigger Slurm queries; queued jobs remain pending.

`job.wait()` raises on failure or timeout. A timeout does not cancel the job;
use `job.cancel()` for that. `job.listen()` accepts `status`, `log`, and `heartbeat`;
remove a callback with `job.unlisten(callback_id)`. Manager shutdown disconnects the
client without cancelling remote jobs.

## Development

```bash
pip install -e '.[dev]'
ruff check .
ruff format --check .
python -m pytest --random-order -q test
```

Unit tests run on Python 3.11 and 3.14. Other CI checks use Python 3.13.
