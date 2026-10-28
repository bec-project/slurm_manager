"""Submit local Bash files and verify ten stdout lines through Redis.

Run from the repository root with credentials in .env:
    python examples/submit_job.py

Replace your_username in the paths below before running.
The remote working and log directories must already exist. One job is submitted;
there are no automatic submission retries.
"""

from pathlib import Path

from slurm_manager.event_messages import LogMessage
from slurm_manager.job_options import JobOptions
from slurm_manager.slurm_manager import SlurmManager


def main(
    *, script_path: Path, working_directory: str, log_directory: str, worker_path: str
) -> None:
    """Upload local Bash files and verify ten stdout lines through Redis.

    Args:
        script_path (Path): Local Bash script that prints once per second.
        working_directory (str): Existing absolute compute-node working directory.
        log_directory (str): Existing compute-node directory for Slurm output files.
        worker_path (str): Absolute compute-node path to the Go worker executable.

    Raises:
        TimeoutError: The job exceeds the 90-second caller wait and is cancelled.
        Exception: Submission, execution, or callback verification fails.
    """
    lines = []

    def log(message: LogMessage) -> None:
        """Record and print a message delivered through the job's Redis log channel.

        Args:
            message (LogMessage): Parsed worker stdout or stderr message.
        """
        lines.append(message.log)
        print(message.log, flush=True)

    manager = SlurmManager(redis_host="bec-slurm.psi.ch", worker_path=worker_path)
    try:
        job = manager.submit_job(
            script_path=script_path,
            working_directory=working_directory,
            callbacks={"log": log},
            job_options=JobOptions(
                name="slurm-manager-redis-stdout-smoke",
                tasks=1,
                time_limit_minutes=1,
                standard_output=f"{log_directory}/redis-stdout-%j.out",
                standard_error=f"{log_directory}/redis-stdout-%j.err",
            ),
        )
        print(f"Submitted job {job.slurm_job_id}", flush=True)
        try:
            job.wait(timeout=90)
        except TimeoutError:
            job.cancel()
            raise
    finally:
        manager.shutdown(timeout=65)
    assert sorted(lines) == [f"stdout-test {second:02d}/10" for second in range(1, 11)]
    print("All ten stdout lines received through Redis.")


if __name__ == "__main__":
    main(
        script_path=Path(__file__).with_name("print_for_10s.sh"),
        working_directory="/das/home/your_username",
        log_directory="/das/home/your_username/slurm_logs",
        worker_path="/das/home/your_username/slurm-worker",
    )
