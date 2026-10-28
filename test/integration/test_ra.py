"""Opt-in production smoke tests; jobs are submitted strictly sequentially.

Run from the checkout with a valid JWT in .env::

    SLURM_RA_HOME=/remote/home SLURM_RA_WORKER=/remote/home/slurm-worker \
        python -m pytest -s test/integration/test_ra.py

The remote home and its slurm_logs subdirectory must already exist. No retries
are made. Each allocation is one task in hour with a one-minute wall-clock limit.
"""

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import pytest

from slurm_manager.event_messages import SlurmMessage
from slurm_manager.job_future import JobFuture
from slurm_manager.job_options import JobOptions
from slurm_manager.slurm_manager import SlurmManager

pytestmark = pytest.mark.skipif(
    not os.environ.get("SLURM_RA_HOME"), reason="Explicit SLURM_RA_HOME required for live jobs"
)


def test_ra_modules(tmp_path: Path) -> None:
    """Verify module activation and module commands inside the remote workload.

    Args:
        tmp_path (Path): Local directory for uploaded activation and workload files.
    """
    remote_home = os.environ["SLURM_RA_HOME"]
    activation = tmp_path / "activate.sh"
    activation.write_text("module load gcc/12.3.0 || return $?\n")
    workload = tmp_path / "modules.sh"
    workload.write_text(
        "#!/bin/bash\nset -eu\n"
        "module is-loaded gcc/12.3.0\n"
        "echo activation-module-ok\n"
        "command -v gcc\n"
        "gcc --version\n"
        "gcc -dumpfullversion\n"
        "module unload gcc/12.3.0\n"
        "if module is-loaded gcc/12.3.0; then exit 1; fi\n"
        "module load gcc/12.3.0\n"
        "module is-loaded gcc/12.3.0\n"
        "echo workload-module-ok\n"
        "gcc -dumpfullversion\n"
    )
    lines: list[str] = []
    manager = SlurmManager(
        redis_host=os.environ.get("SLURM_RA_REDIS", "bec-slurm.psi.ch"),
        worker_path=os.environ.get("SLURM_RA_WORKER", f"{remote_home}/slurm-worker"),
    )
    job = None
    try:
        job = manager.submit_job(
            script_path=workload,
            env_path=activation,
            working_directory=remote_home,
            callbacks={"log": lambda message: lines.append(message.log)},
            job_options=JobOptions(
                name="smoke-modules",
                partition="hour",
                tasks=1,
                time_limit_minutes=1,
                standard_output=f"{remote_home}/slurm_logs/smoke-modules-%j.out",
                standard_error=f"{remote_home}/slurm_logs/smoke-modules-%j.err",
            ),
        )
        print(f"Submitted modules: {job.slurm_job_id}", flush=True)
        job.wait(timeout=90)
    finally:
        try:
            if job is not None and not job.done():
                job.cancel()
        finally:
            manager.shutdown(timeout=65)
    print("Redis output:", lines, flush=True)
    assert lines.count("12.3.0") == 2
    assert any(line.startswith("/") and line.endswith("/gcc") for line in lines)
    assert any("gcc" in line.lower() and "12.3.0" in line for line in lines)
    assert "activation-module-ok" in lines
    assert "workload-module-ok" in lines


def test_ra_job_lifecycle(tmp_path: Path) -> None:
    """Verify live feedback, future-job listeners, failure, cancellation and unlisten.

    Args:
        tmp_path (Path): Pytest directory for local scripts uploaded by the manager.
    """
    remote_home = os.environ["SLURM_RA_HOME"]
    manager = SlurmManager(
        redis_host=os.environ.get("SLURM_RA_REDIS", "bec-slurm.psi.ch"),
        worker_path=os.environ.get("SLURM_RA_WORKER", f"{remote_home}/slurm-worker"),
    )
    events: list[SlurmMessage] = []
    lock = threading.Lock()
    jobs: list[JobFuture] = []

    def record(message: SlurmMessage) -> None:
        """Collect global feedback safely from callback threads.

        Args:
            message (SlurmMessage): Feedback received through Redis or the manager.
        """
        with lock:
            events.append(message)

    def received(job: JobFuture, kind: str) -> list[SlurmMessage]:
        """Return a snapshot of messages for the scheduler job.

        Args:
            job (JobFuture): Job whose feedback is selected.
            kind (str): Message discriminator to select.

        Returns:
            list[SlurmMessage]: Matching feedback collected so far.
        """
        with lock:
            return [
                message
                for message in events
                if message.metadata.get("slurm_job_id") == job.slurm_job_id
                and message.msg_type == kind
            ]

    def submit(
        name: str,
        script: str,
        callbacks: dict[Literal["log", "status", "heartbeat"], Callable[[SlurmMessage], None]]
        | None = None,
    ) -> JobFuture:
        """Upload a local workload with a bounded production allocation.

        Args:
            name (str): Short workload name used in log filenames.
            script (str): Bash workload body uploaded from a temporary local file.
            callbacks (dict[str, Callable] | None): Optional job-specific callbacks.

        Returns:
            JobFuture: Submitted job tracked for cleanup on test failure.
        """
        path = tmp_path / f"{name}.sh"
        path.write_text("#!/bin/bash\nset -eu\n" + script)
        job = manager.submit_job(
            script_path=path,
            working_directory=remote_home,
            callbacks=callbacks,
            job_options=JobOptions(
                name=f"smoke-{name}",
                partition="hour",
                tasks=1,
                time_limit_minutes=1,
                standard_output=f"{remote_home}/slurm_logs/smoke-{name}-%j.out",
                standard_error=f"{remote_home}/slurm_logs/smoke-{name}-%j.err",
            ),
        )
        jobs.append(job)
        print(f"Submitted {name}: {job.slurm_job_id}", flush=True)
        return job

    registrations = [
        registration
        for kind in ("log", "status", "heartbeat")
        for registration in manager.listen(kind, record)
    ]
    try:
        first_line = threading.Event()
        live = submit(
            "output",
            'for i in {1..10}; do echo "stdout-$i"; sleep 1; done\necho stderr-ok >&2\n',
            {"log": lambda message: first_line.set()},
        )
        assert first_line.wait(90), "No live Redis output within 90 seconds"
        assert not live.done(), "Output was only received after completion"
        live.wait(timeout=90)
        assert live.success()

        failed = submit("failure", "echo failure-visible >&2\nexit 7\n")
        with pytest.raises(Exception, match="failed with status error"):
            failed.wait(timeout=90)
        assert not failed.success()

        ready = threading.Event()
        cancelled = submit(
            "cancel",
            "echo cancel-ready\nwhile true; do sleep 1; done\n",
            {"log": lambda message: ready.set()},
        )
        assert ready.wait(90), "Cancellation workload never started"
        cancelled.cancel()
        assert cancelled.status == "cancelled"
        # A locally cancelled future alone does not prove scheduler cancellation.
        for _ in range(12):
            time.sleep(5)
            record_from_slurm = manager.rest_client.get_job(cancelled.slurm_job_id)
            state = (record_from_slurm or {}).get("job_state", [])
            if isinstance(state, dict):
                state = state.get("current", [])
            if "CANCELLED" in state and "COMPLETING" not in state:
                break
        else:
            pytest.fail("Slurm did not confirm cancellation within 60 seconds")

        manager.unlisten(registrations)
        local_lines = []
        detached = submit(
            "unlisten",
            "echo local-only\nsleep 2\n",
            {"log": lambda message: local_lines.append(message.log)},
        )
        detached.wait(timeout=90)
    finally:
        try:
            for job in jobs:
                if not job.done():
                    job.cancel()
        finally:
            manager.shutdown(timeout=65)

    assert sorted(message.log for message in received(live, "log")) == sorted(
        [f"stdout-{index}" for index in range(1, 11)] + ["stderr-ok"]
    )
    assert received(live, "heartbeat")
    assert any(message.status == "finished" for message in received(live, "status"))
    assert [message.log for message in received(failed, "log")] == ["failure-visible"]
    assert any(message.status.startswith("error") for message in received(failed, "status"))
    assert received(cancelled, "heartbeat")
    assert any(message.status == "cancelled" for message in received(cancelled, "status"))
    assert not received(detached, "log")
    assert not received(detached, "status")
    assert local_lines == ["local-only"]
