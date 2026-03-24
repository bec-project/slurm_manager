"""Minimal Slurm manager draft for sbatch + JobFuture creation."""

from __future__ import annotations

from pathlib import Path
import subprocess
from uuid import uuid4

from slurm_manager.job_future import JobFuture
from slurm_manager.redis_pubsub_client import RedisPubSubClient


class SlurmManager:
    """Small manager that submits wrapper jobs and returns JobFuture handles."""

    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379) -> None:
        self.pubsub_client = RedisPubSubClient(bootstrap=f"{redis_host}:{redis_port}")
        self.registry: dict[str, JobFuture] = {}

        self._wrapper_path = Path(__file__).resolve().parent / "job_submission" / "wrapper.sh"
        self._wrapper_cwd = str(self._wrapper_path.parent)

    def submit_job(self, script_path: str, env_path: str) -> JobFuture:
        """Create a temporary script from command and submit via sbatch wrapper."""
        if not self._wrapper_path.exists():
            raise FileNotFoundError(f"wrapper script not found: {self._wrapper_path}")

        job_id = str(uuid4())

        sbatch_cmd = ["sbatch", "wrapper.sh", env_path, script_path, job_id, "1", "0"]

        result = subprocess.run(
            sbatch_cmd, cwd=self._wrapper_cwd, capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"sbatch failed: {stderr}")

        job_future = JobFuture(job_id=job_id, manager=self, registry=self.registry)
        self.registry[job_id] = job_future
        return job_future
