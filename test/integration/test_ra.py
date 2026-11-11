"""Opt-in production smoke tests; jobs are submitted strictly sequentially.

Run from the checkout with a valid JWT in .env::

    SLURM_RA_HOME=/remote/home SLURM_RA_WORKER=/remote/home/slurm-worker \
        python -m pytest -s test/integration/test_ra.py

The remote home and its slurm_logs subdirectory must already exist. No retries
are made. Each allocation is one task in hour with a one-minute wall-clock limit.
"""

import os
import shlex
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from queue import Queue
from typing import Literal, TypeVar
from uuid import uuid4

import pytest

from slurm_manager.clusters import Cluster
from slurm_manager.clusters.job_options import JobOptions
from slurm_manager.clusters.ra import RAJobOptions
from slurm_manager.event_messages import HeartBeatMessage, LogMessage, SlurmMessage, StatusMessage
from slurm_manager.job_future import JobFuture
from slurm_manager.slurm_manager import SlurmManager

_MessageT = TypeVar("_MessageT", bound=SlurmMessage)

pytestmark = pytest.mark.skipif(
    not os.environ.get("SLURM_RA_HOME"), reason="Explicit SLURM_RA_HOME required for live jobs"
)


def test_ra_modules(tmp_path: Path) -> None:
    """Verify module activation and workload commands using the built-in RA profile.

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
    manager = SlurmManager.for_cluster(
        Cluster.RA,
        env_file=".env",
        worker_path=os.environ.get("SLURM_RA_WORKER", f"{remote_home}/slurm-worker"),
    )
    job = None
    try:
        job = manager.submit_job(
            script_path=workload,
            env_path=activation,
            working_directory=remote_home,
            callbacks={
                "log": lambda message: (
                    lines.append(message.log) if isinstance(message, LogMessage) else None
                )
            },
            job_options=RAJobOptions(
                name="smoke-modules",
                partition="hour",
                tasks=1,
                time_limit=1,
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

    def received(job: JobFuture, message_type: type[_MessageT]) -> list[_MessageT]:
        """Return a snapshot of messages for the scheduler job.

        Args:
            job (JobFuture): Job whose feedback is selected.
            message_type: Message model to select.

        Returns:
            Matching feedback collected so far.
        """
        with lock:
            return [
                message
                for message in events
                if message.metadata.get("slurm_job_id") == job.slurm_job_id
                and isinstance(message, message_type)
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
                time_limit=1,
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
            {
                "log": lambda message: (
                    local_lines.append(message.log) if isinstance(message, LogMessage) else None
                )
            },
        )
        detached.wait(timeout=90)
    finally:
        try:
            for job in jobs:
                if not job.done():
                    job.cancel()
        finally:
            manager.shutdown(timeout=65)

    assert sorted(message.log for message in received(live, LogMessage)) == sorted(
        [f"stdout-{index}" for index in range(1, 11)] + ["stderr-ok"]
    )
    assert received(live, HeartBeatMessage)
    assert any(message.status == "finished" for message in received(live, StatusMessage))
    assert [message.log for message in received(failed, LogMessage)] == ["failure-visible"]
    assert any(message.status.startswith("error") for message in received(failed, StatusMessage))
    assert received(cancelled, HeartBeatMessage)
    assert any(message.status == "cancelled" for message in received(cancelled, StatusMessage))
    assert not received(detached, LogMessage)
    assert not received(detached, StatusMessage)
    assert local_lines == ["local-only"]


@pytest.fixture
def ra_home() -> Path:
    """Return the shared home directory on RA."""
    return Path(os.environ["SLURM_RA_HOME"])


@pytest.fixture
def ra_manager(ra_home: Path) -> Iterator[SlurmManager]:
    """Connect to RA with the smoke test worker and Redis settings."""
    manager = SlurmManager(
        redis_host=os.environ.get("SLURM_RA_REDIS", "bec-slurm.psi.ch"),
        worker_path=os.environ.get("SLURM_RA_WORKER", str(ra_home / "slurm-worker")),
    )
    try:
        yield manager
    finally:
        manager.shutdown(timeout=65)


def _wait_for_job(job: JobFuture) -> None:
    """Wait for a short smoke job and cancel it if it is still running."""
    print(f"Submitted RA job {job.slurm_job_id}", flush=True)
    try:
        job.wait(timeout=90)
    finally:
        if not job.done():
            job.cancel()


def test_ra_command(ra_manager: SlurmManager, ra_home: Path, tmp_path: Path) -> None:
    """Run a shell command with the uploaded activation environment."""
    activation = tmp_path / "activate.sh"
    activation.write_text("export BEC_RA_SMOKE=activated\nset -- unused\n")
    output: Queue[str] = Queue()

    def capture_log(message: SlurmMessage) -> None:
        if isinstance(message, LogMessage):
            output.put(message.log)

    job = ra_manager.submit_job(
        command='test "$BEC_RA_SMOKE" = activated && printf "command-ok\\n"',
        env_path=activation,
        working_directory=str(ra_home),
        callbacks={"log": capture_log},
        job_options=JobOptions(name="smoke-command", partition="hour", time_limit=1),
    )
    _wait_for_job(job)
    assert output.get(timeout=5) == "command-ok"


def test_ra_remote_script(ra_manager: SlurmManager, ra_home: Path, tmp_path: Path) -> None:
    """Resolve a relative script path before activation changes directory and arguments."""
    remote_directory = ra_home / f".bec-slurm-smoke-{uuid4().hex}"
    remote_script = remote_directory / "workload.sh"
    directory = shlex.quote(str(remote_directory))
    script = shlex.quote(str(remote_script))
    workload = (
        "set -eu\n"
        'test "$BEC_RA_SMOKE" = activated\n'
        'test "$PWD" = /\n'
        f"rm -- {script}\n"
        f"rmdir -- {directory}\n"
    )
    stage = tmp_path / "stage.sh"
    stage.write_text(
        f"#!/bin/bash\nset -eu\nmkdir -- {directory}\n"
        f"printf '%s' {shlex.quote(workload)} > {script}\n"
    )
    cleanup = tmp_path / "cleanup.sh"
    cleanup.write_text(
        f"#!/bin/bash\nset -eu\nif [ -d {directory} ]; then\n"
        f"    rm -f -- {script}\n    rmdir -- {directory}\nfi\n"
    )
    activation = tmp_path / "activate.sh"
    activation.write_text("export BEC_RA_SMOKE=activated\nset -- unused\ncd /\n")
    options = JobOptions(name="smoke-remote-script", partition="hour", time_limit=1)
    staging_job = ra_manager.submit_job(
        script_path=stage, working_directory=str(ra_home), job_options=options
    )
    try:
        _wait_for_job(staging_job)
        job = ra_manager.submit_job(
            remote_script_path=str(remote_script.relative_to(ra_home)),
            env_path=activation,
            working_directory=str(ra_home),
            job_options=options,
        )
        _wait_for_job(job)
    except Exception:
        cleanup_job = ra_manager.submit_job(
            script_path=cleanup, working_directory=str(ra_home), job_options=options
        )
        _wait_for_job(cleanup_job)
        raise


@pytest.fixture
def ra_noop(tmp_path: Path) -> Path:
    """Provide an uploaded workload for checking scheduler options."""
    script = tmp_path / "noop.sh"
    script.write_text("#!/bin/bash\ntrue\n")
    return script


def test_ra_required_nodes(ra_manager: SlurmManager, ra_home: Path, ra_noop: Path) -> None:
    """Request a node selected by Slurm and verify the next job's allocation."""
    discovery = ra_manager.submit_job(
        script_path=ra_noop,
        working_directory=str(ra_home),
        job_options=JobOptions(name="smoke-discover-node", partition="hour", time_limit=1),
    )
    _wait_for_job(discovery)
    record = ra_manager.rest_client.get_job(discovery.slurm_job_id)
    assert record is not None, "Slurm did not return the discovery job"
    node = record.get("nodes")
    assert isinstance(node, str) and node, (
        f"Slurm returned no allocated node for {discovery.slurm_job_id}"
    )

    job = ra_manager.submit_job(
        script_path=ra_noop,
        working_directory=str(ra_home),
        job_options=JobOptions(
            name="smoke-required-node", partition="hour", time_limit=1, required_nodes=[node]
        ),
    )
    _wait_for_job(job)
    record = ra_manager.rest_client.get_job(job.slurm_job_id)
    assert record is not None, "Slurm did not return the node-constrained job"
    assert record.get("required_nodes") == node
    assert record.get("nodes") == node


def test_ra_nice(ra_manager: SlurmManager, ra_home: Path, ra_noop: Path) -> None:
    """Verify that Slurm records the requested positive niceness."""
    job = ra_manager.submit_job(
        script_path=ra_noop,
        working_directory=str(ra_home),
        job_options=JobOptions(name="smoke-nice", partition="hour", time_limit=1, nice=100),
    )
    _wait_for_job(job)
    record = ra_manager.rest_client.get_job(job.slurm_job_id)
    assert record is not None, "Slurm did not return the nice job"
    assert record.get("nice") == 100


def test_ra_job_dependencies(ra_manager: SlurmManager, ra_home: Path, ra_noop: Path) -> None:
    """Release after_any after a failure and cancel the unsatisfiable after_ok job."""
    jobs: list[JobFuture] = []
    options = JobOptions(name="smoke-dependencies", partition="hour", time_limit=1)
    try:
        upstream = ra_manager.submit_job(
            command="sleep 8; exit 7", working_directory=str(ra_home), job_options=options
        )
        jobs.append(upstream)
        after_ok = ra_manager.submit_job(
            script_path=ra_noop,
            working_directory=str(ra_home),
            job_options=options,
            after_ok=upstream,
        )
        jobs.append(after_ok)
        after_any = ra_manager.submit_job(
            script_path=ra_noop,
            working_directory=str(ra_home),
            job_options=options,
            after_any=upstream,
        )
        jobs.append(after_any)

        with pytest.raises(Exception, match="failed with status error"):
            _wait_for_job(upstream)
        _wait_for_job(after_any)
        with pytest.raises(Exception, match="failed with status cancelled"):
            _wait_for_job(after_ok)

        parent_record = ra_manager.rest_client.get_job(upstream.slurm_job_id)
        child_record = ra_manager.rest_client.get_job(after_any.slurm_job_id)
        assert parent_record is not None and child_record is not None
        parent_end = parent_record["end_time"]["number"]
        child_start = child_record["start_time"]["number"]
        assert 0 < parent_end <= child_start
    finally:
        for job in jobs:
            if not job.done():
                job.cancel()
