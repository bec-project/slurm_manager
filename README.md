# slurm_manager

The slurm manager is a tool for managing slurm jobs. It provides a simple interface for submitting, monitoring, and canceling slurm jobs. It also provides a way to view the status of all slurm jobs in a cluster.

 
## SlurmManager Manager (Python)
* submits jobs using the slurm client 
* sets up the configuration for slurm jobs 
* provides an overview to query the status of all slurm jobs that are currently running or have recently finished
 
## SlurmManager Job Future (Python)
* wrapper around a slurm submission command
* provides an interface to query the status of a slurm job and to cancel a slurm job using a future-like interface
* provides an interface to listen to the stream of stdout and stderr of a slurm job through redis subscriptions
 
## SlurmManager Job Wrapper (Bash script + ?)
* Wraps around the user's command to be run as a slurm job
* streams stdout and stderr of the job to redis 
* emits events to redis when the job starts and finishes and when it fails
* Looking forward, this may also start a local server that listens to commands from the user to emit custom events to redis (bec-slurm) to stream out data.

## Open Points
* We want to allow that user scripts can be written in a way that they emit custom events to redis (bec-slurm). 
* Proper procedure to cancel/kill a slurm job, and clean up with proper event emission to redis. Alternatively, client side will implement also a monitoring of the slurm job status, and if the job vanishes from the slurm job list it will be considered as finished respectively failed if no finished event was emitted. 

### Example Code
 
```python
 
manager = SlurmManager(redis_host="localhost", redis_port=6379)
manager.set_default_job_config(
    time="00:30:00",
    partition="short",
    nodes=1,
    ntasks_per_node=1,
    cpus_per_task=4,
    mem="16G",
    environments={"default": "/path/to/env/script.sh", "env2": "/path/to/env2/script.sh"},
    default_environment="default",
)
job = manager.submit_job(
    job_name="my_job",
    command="python my_script.py",
)
job_status = job.status()
print(f"Job status: {job_status}")
try:
    job.wait(timeout=20)
except TimeoutError:
    print("Job did not finish within the timeout period.")
    job.cancel()
 
job = manager.submit_job(
    job_name="my_job",
    command="python my_script.py",
    environment="env2",
)
 
# Listen to job events
job.listen(event_type="status", callback=lambda event: print(f"Received event: {event}"))
job.listen(event_type="stdout", callback=lambda event: print(f"Received stdout: {event}"))
job.listen(event_type="stderr", callback=lambda event: print(f"Received stderr: {event}"))
 
 
# Listen to all job events
manager.listen(event_type="status", callback=lambda event: print(f"Job started: {event}"))
```


