"""Submit and monitor remote Slurm jobs through slurmrestd and Redis."""

from __future__ import annotations

import json
import shlex
import threading
from collections.abc import Callable, Sequence
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import Any, Generic, Literal, overload
from uuid import uuid4
from weakref import WeakValueDictionary

from typing_extensions import TypeVar

from slurm_manager.clusters import Cluster, ra, sfonline, slsonline
from slurm_manager.clusters.job_options import JobOptions
from slurm_manager.event_messages import SlurmMessage, StatusMessage
from slurm_manager.job_future import JobFuture, JobStatus
from slurm_manager.job_tracker import JobTracker, SubscriptionInfo
from slurm_manager.redis_sub_client import RedisSubClient
from slurm_manager.slurm_rest_client import SlurmRestClient, SlurmRestError

logger = getLogger(__name__)

_JobOptionsT = TypeVar("_JobOptionsT", bound=JobOptions, default=JobOptions)


def _job_exit_code(exit_code: Any) -> int | str | None:
    """Decode a v0.0.44 process exit status without treating unset codes as success.

    Args:
        exit_code (Any): Integer or v0.0.44 structured process exit status.

    Returns:
        int | str | None: Exit code, a signal description, or None when no code is
            available.
    """
    if not isinstance(exit_code, dict):
        return exit_code
    code = exit_code.get("return_code")
    if isinstance(code, dict):
        code = code.get("number") if code.get("set") else None
    signal = exit_code.get("signal", {}).get("id", {})
    if isinstance(signal, dict):
        signal = signal.get("number", 0) if signal.get("set") else 0
    return code if not signal else f"signal {signal}"


class SlurmManager(Generic[_JobOptionsT]):
    """Submit Slurm jobs and coordinate tracking and scheduler monitoring."""

    @overload
    def __init__(
        self: SlurmManager[JobOptions],
        redis_host: str = "localhost",
        redis_port: int = 6379,
        *,
        redis_tls: bool = True,
        env_file: str | Path = ".env",
        rest_client: SlurmRestClient | None = None,
        worker_path: str = "/usr/bin/bec-slurm-worker",
        job_options_type: None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        *,
        redis_tls: bool = True,
        env_file: str | Path = ".env",
        rest_client: SlurmRestClient | None = None,
        worker_path: str = "/usr/bin/bec-slurm-worker",
        job_options_type: type[_JobOptionsT],
    ) -> None: ...

    def __init__(  # noqa: PLR0913 - connection and worker configuration
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        *,
        redis_tls: bool = True,
        env_file: str | Path = ".env",
        rest_client: SlurmRestClient | None = None,
        worker_path: str = "/usr/bin/bec-slurm-worker",
        job_options_type: type[_JobOptionsT] | None = None,
    ) -> None:
        """Create REST/Redis clients and start background job monitoring.

        Args:
            redis_host (str): Redis hostname reachable by the submitter and compute
                nodes.
            redis_port (int): Redis TCP port shared by the submitter and worker.
            redis_tls (bool): Enable verified TLS for the subscriber and worker.
            env_file (str | Path): Local credential dotenv path. Environment variables
                take precedence.
            rest_client (SlurmRestClient | None): Optional preconfigured REST client;
                None creates one from env_file.
            worker_path (str): Absolute path to the Go executable on compute nodes.
            job_options_type: Model used to validate submissions for this manager.

        Raises:
            ValueError: The worker path or REST connection settings are invalid.
        """
        if not worker_path.startswith("/"):
            raise ValueError("Worker path must be an absolute compute-node path")
        self._job_options_type = JobOptions if job_options_type is None else job_options_type
        self._redis_tls = redis_tls
        self.worker_path = worker_path
        self._redis_address = f"{redis_host}:{redis_port}"
        self.rest_client = rest_client if rest_client is not None else SlurmRestClient(env_file)
        redis_options = {}
        if redis_tls:
            redis_options = {"ssl": True, "ssl_cert_reqs": "required", "ssl_check_hostname": True}
        self.sub_client = RedisSubClient(bootstrap=self._redis_address, **redis_options)
        self._job_tracker = JobTracker(self.sub_client)
        self._submitted_futures: WeakValueDictionary[str, JobFuture] = WeakValueDictionary()
        self._submitted_futures_lock = threading.Lock()
        self._stop_heartbeat_thread = threading.Event()
        # Heartbeat monitoring thread
        self._heartbeat_monitoring_thread = threading.Thread(
            target=self._heartbeat_monitoring_loop, name="SlurmManagerHeartbeatMonitor", daemon=True
        )
        self._start_threads()

    @overload
    @staticmethod
    def for_cluster(
        cluster: Literal["ra", Cluster.RA],
        *,
        env_file: str | Path,
        worker_path: str = "/usr/bin/bec-slurm-worker",
    ) -> SlurmManager[ra.RAJobOptions]: ...

    @overload
    @staticmethod
    def for_cluster(
        *, env_file: str | Path, worker_path: str = "/usr/bin/bec-slurm-worker"
    ) -> SlurmManager[ra.RAJobOptions]: ...

    @overload
    @staticmethod
    def for_cluster(
        cluster: Literal["sfonline", "SFOnline", Cluster.SFONLINE],
        *,
        env_file: str | Path,
        worker_path: str = "/usr/bin/bec-slurm-worker",
    ) -> SlurmManager[sfonline.SFOnlineJobOptions]: ...

    @overload
    @staticmethod
    def for_cluster(
        cluster: Literal["slsonline", "SLSOnline", Cluster.SLSONLINE],
        *,
        env_file: str | Path,
        worker_path: str = "/usr/bin/bec-slurm-worker",
    ) -> SlurmManager[slsonline.SLSOnlineJobOptions]: ...

    @staticmethod
    def for_cluster(
        cluster: str = Cluster.RA,
        *,
        env_file: str | Path,
        worker_path: str = "/usr/bin/bec-slurm-worker",
    ) -> SlurmManager[Any]:
        """Create a manager bound to a built-in cluster using a local credential file."""
        options_type: type[JobOptions]
        match cluster:
            case Cluster.RA:
                info = ra.CLUSTER_INFO
                options_type = ra.RAJobOptions
            case Cluster.SFONLINE | "SFOnline":
                info = sfonline.CLUSTER_INFO
                options_type = sfonline.SFOnlineJobOptions
            case Cluster.SLSONLINE | "SLSOnline":
                info = slsonline.CLUSTER_INFO
                options_type = slsonline.SLSOnlineJobOptions
            case _:
                raise ValueError(
                    f"Unknown Slurm cluster {cluster!r}; supported clusters: ra, sfonline, slsonline"
                )
        rest_client = SlurmRestClient(
            env_file, rest_url=info.url, api_version=info.api_version, use_environment=False
        )
        return SlurmManager(
            info.redis_host,
            info.redis_port,
            redis_tls=info.redis_tls,
            worker_path=worker_path,
            rest_client=rest_client,
            job_options_type=options_type,
        )

    ################################################
    #  Public Job Management                       #
    ################################################

    def submit_job(  # noqa: PLR0913 - submission options
        self,
        script_path: str | Path,
        env_path: str | Path | None = None,
        *,
        working_directory: str,
        job_options: _JobOptionsT | None = None,
        after_ok: JobFuture | Sequence[JobFuture] | None = None,
        after_any: JobFuture | Sequence[JobFuture] | None = None,
        callbacks: dict[Literal["log", "status", "heartbeat"], Callable[[SlurmMessage], None]]
        | None = None,
    ) -> JobFuture:
        """Upload local Bash files and return a monitored, released Slurm job.

        Args:
            script_path (str | Path): Local Bash workload file whose contents are
                uploaded.
            env_path (str | Path | None): Optional local Bash activation file for
                workload-specific environment setup; None skips activation commands.
            working_directory (str): Absolute, existing compute-node directory in which
                the job runs.
            job_options (JobOptions | None): Validated resources and Slurm output
                settings; None uses JobOptions defaults.
            after_ok: One or more jobs from this manager that must succeed before the
                new job starts.
            after_any: One or more jobs from this manager that must finish before the
                new job starts, regardless of outcome. Both after_ok and after_any
                may be supplied. Use job_options.dependency for a raw expression.
            callbacks (dict[str, Callable] | None): Callbacks keyed by log, status, or
                heartbeat, acknowledged before job release.

        Returns:
            JobFuture: Handle for querying, waiting, cancelling, and registering
                callbacks.

        Raises:
            FileNotFoundError: A local workload or activation file does not exist.
            ValueError: The working directory, callback event names, or dependencies
                are invalid.
            ValidationError: Resource or output options fail JobOptions validation.
            SlurmRestError: Submission or release fails. A known held job is cancelled
                on setup failure.
            TimeoutError: Redis does not acknowledge the monitoring subscriptions.

        Notes:
            The credential dotenv file stays local. Callbacks are registered before
            release; only the working directory, worker path, and output paths refer to
            compute nodes.
        """
        if not working_directory.startswith("/"):
            raise ValueError("Working directory must be an absolute compute-node path")
        if callbacks and set(callbacks).difference({"log", "status", "heartbeat"}):
            raise ValueError("Callbacks must use log, status, or heartbeat keys")
        options = job_options
        if isinstance(job_options, JobOptions):
            exclude = {"tasks"} if job_options.tasks_per_node is not None else set()
            options = job_options.model_dump(exclude=exclude - job_options.model_fields_set)
        job = self._job_options_type.model_validate({} if options is None else options).to_rest()
        dependency = self._build_job_dependency(
            after_ok=after_ok, after_any=after_any, has_raw_dependency="dependency" in job
        )
        if dependency is not None:
            job["dependency"] = dependency
        if "dependency" in job:
            job["flags"] = [*job.get("flags", ()), "KILL_INVALID_DEPENDENCY"]
        job_id = str(uuid4())
        script = self._build_job_script(
            script_path,
            env_path,
            job_id,
            worker_path=self.worker_path,
            redis_address=self._redis_address,
            redis_tls=self._redis_tls,
        )
        job.update(
            {
                "current_working_directory": working_directory,
                "environment": [
                    "PATH=/usr/share/Modules/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:"
                    "/usr/sbin:/usr/lpp/mmfs/bin:"
                ],
                "hold": True,
            }
        )
        result = self.rest_client.submit(script, job)
        slurm_job_id = str(result["job_id"])
        job_future = JobFuture(job_id=job_id, manager=self, slurm_job_id=slurm_job_id)
        slurm_job_id = job_future.slurm_job_id
        try:
            self._job_tracker.track(job_future)
            for event, callback in (callbacks or {}).items():
                job_future.listen(event, callback)
            self._job_tracker.wait_until_ready(job_id)
            self.rest_client.release(slurm_job_id)
        except Exception as exc:
            # A setup failure must not leave a held allocation behind.
            try:
                self.rest_client.cancel(slurm_job_id)
            except (SlurmRestError, ValueError):
                logger.error("Could not cancel held job %s", slurm_job_id)
            self._job_tracker.apply_status(
                StatusMessage(status=f"error:Submission setup failed: {exc}"), job_future
            )
            raise
        with self._submitted_futures_lock:
            self._submitted_futures[job_id] = job_future
        return job_future

    def cancel_job(self, slurm_job_id: str, job_id: str) -> None:
        """Request cancellation through REST and mark the tracked job cancelled.

        Args:
            slurm_job_id (str): Numeric scheduler identifier returned by Slurm.
            job_id (str): Opaque manager identifier used in Redis topic names.

        Raises:
            SlurmRestError: Slurm rejects cancellation or the request fails.
        """
        job_future = self._job_tracker.get_future(job_id)
        self.rest_client.cancel(slurm_job_id)

        if job_future is None:
            return

        self._job_tracker.apply_status(
            StatusMessage(
                status="cancelled",
                metadata={
                    "msg_received": datetime.now().isoformat(),
                    "job_id": job_future._job_id,
                    "slurm_job_id": job_future.slurm_job_id,
                },
            ),
            future=job_future,
        )

    ################################################
    #  Public Event Subscriptions                  #
    ################################################

    def listen(
        self,
        event_type: Literal["heartbeat", "log", "status"],
        callback: Callable[[SlurmMessage], None],
        job_id: str | None = None,
    ) -> list[str]:
        """Register a callback for one job or all current and future jobs.

        Args:
            event_type (Literal['heartbeat', 'log', 'status']): Event to receive: log,
                status, or heartbeat.
            callback (Callable[[SlurmMessage], None]): Callable receiving the parsed
                Slurm message.
            job_id (str | None): Opaque manager identifier used in Redis topic names.

        Returns:
            list[str]: A single registration ID accepted by unlisten(). Without a
                job_id, the ID owns the persistent manager-wide registration.

        Raises:
            ValueError: The event type is not log, status, or heartbeat.
            KeyError: The explicitly requested job ID is not tracked.
            RuntimeError: A selected job has already completed.

        Notes:
            With job_id=None, registration applies to all current and future jobs until
            unlisten() or shutdown(). Callbacks run in the tracker executor.
        """
        return self._job_tracker.listen(event_type, callback, job_id)

    def unlisten(self, callback_id: str | list[str]) -> None:
        """Remove one or more callback subscriptions without cancelling the job.

        Args:
            callback_id (str | list[str]): Subscription ID, or IDs where accepted,
                returned during registration.
        """
        self.remove_subscription_by_id(callback_id)

    def remove_subscription_by_id(self, callback_id: str | list[str]) -> None:
        """Remove the selected callbacks while preserving other topic listeners.

        Args:
            callback_id (str | list[str]): Subscription ID, or IDs where accepted,
                returned during registration.
        """
        self._job_tracker.remove_subscription_by_id(callback_id)

    def remove_subscription_by_topic(self, topic: str) -> None:
        """Remove all callbacks for one topic and unsubscribe it from Redis.

        Args:
            topic (str): Redis channel whose callback registrations are selected.
        """
        self._job_tracker.remove_subscription_by_topic(topic)

    def remove_subscriptions_for_job(self, job_id: str, skip_event_callback: bool = False) -> None:
        """Remove job subscriptions and discard the tracked future.

        Args:
            job_id (str): Opaque manager identifier used in Redis topic names.
            skip_event_callback (bool): If true, remove monitoring without synthesizing
                a failure update.

        Notes:
            Unless skip_event_callback is true, an unfinished future is marked failed.
            This only removes local monitoring; it does not cancel the Slurm allocation.
        """
        self._job_tracker.remove_subscriptions_for_job(job_id, skip_event_callback)

    ################################################
    #  Public Lifecycle                            #
    ################################################

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Stop monitoring, close Redis, and drain accepted callback work.

        Args:
            timeout (float | None): Thread-join timeout in seconds; None waits
                indefinitely.

        Raises:
            TimeoutError: The heartbeat monitor is still performing a Slurm query.

        Notes:
            Remote jobs are not cancelled. The timeout applies to thread joins; running
            and queued user callbacks are drained without a callback execution deadline.
        """
        self._stop_heartbeat_thread.set()
        self._heartbeat_monitoring_thread.join(timeout)
        if self._heartbeat_monitoring_thread.is_alive():
            raise TimeoutError(
                "Heartbeat monitor did not stop; retry after its Slurm query finishes"
            )
        self._job_tracker.shutdown(timeout)

    ################################################
    #  Internal Callback Registration              #
    ################################################

    def _add_subscription(
        self,
        job_id: str,
        key: Literal["heartbeat", "log", "status"],
        callback: Callable[[SlurmMessage], None],
        callback_kwargs: dict[str, Any] | None = None,
    ) -> str:
        """Atomically register a Redis callback and its manager ownership.

        Args:
            job_id (str): Opaque manager identifier used in Redis topic names.
            key (Literal['heartbeat', 'log', 'status']): Event key: log, status, or
                heartbeat.
            callback (Callable[[SlurmMessage], None]): Callable receiving the parsed
                Slurm message.
            callback_kwargs (dict[str, Any] | None): Additional keyword arguments passed
                to the user callback.

        Returns:
            str: Unique callback subscription ID.

        Raises:
            RuntimeError: The job is absent or finishes during registration.
        """

        return self._job_tracker.add_subscription(job_id, key, callback, callback_kwargs)

    ################################################
    #  Internal Script Packaging                   #
    ################################################

    def _build_job_dependency(
        self,
        *,
        after_ok: JobFuture | Sequence[JobFuture] | None,
        after_any: JobFuture | Sequence[JobFuture] | None,
        has_raw_dependency: bool,
    ) -> str | None:
        """Validate issued futures and return their Slurm dependency expression."""
        dependencies = []
        for argument, kind, dependency_jobs in (
            ("after_ok", "afterok", after_ok),
            ("after_any", "afterany", after_any),
        ):
            if dependency_jobs is None:
                continue
            if has_raw_dependency:
                raise ValueError(f"Specify either {argument} or job_options.dependency")
            upstream = (
                (dependency_jobs,)
                if isinstance(dependency_jobs, JobFuture)
                else tuple(dependency_jobs)
            )
            with self._submitted_futures_lock:
                if not upstream or any(
                    not isinstance(future, JobFuture)
                    or self._submitted_futures.get(future._job_id) is not future
                    for future in upstream
                ):
                    raise ValueError(f"{argument} must contain futures returned by this manager")
            ids = ":".join(future.slurm_job_id for future in upstream)
            dependencies.append(f"{kind}:{ids}")
        return ",".join(dependencies) if dependencies else None

    @staticmethod
    def _build_job_script(  # noqa: PLR0913 - worker command and TLS configuration
        script_path: str | Path,
        env_path: str | Path | None,
        job_id: str,
        *,
        worker_path: str = "/usr/bin/bec-slurm-worker",
        redis_address: str = "localhost:6379",
        redis_tls: bool = True,
    ) -> str:
        """Package local Bash file contents for the compute-node Go worker.

        Args:
            script_path (str | Path): Local Bash workload file whose contents are uploaded.
            env_path (str | Path | None): Optional local Bash activation file for
                workload-specific environment setup; None skips activation commands.
            job_id (str): Opaque manager identifier used in Redis topic names.
            worker_path (str): Absolute path to the Go executable on compute nodes.
            redis_address (str): Redis host:port passed to the worker; no credentials are
                embedded.
            redis_tls (bool): Enable verified TLS in the worker.

        Returns:
            str: Batch script invoking the configured worker with JSON on standard input.

        Raises:
            FileNotFoundError: A local script file is missing.
            ValueError: The worker path is not an absolute compute-node path.

        Notes:
            The payload contains the job ID and selected workload and activation scripts,
            not the credential dotenv file. The job ID is kept out of process arguments.
            Staging and cleanup belong to the worker.
        """
        if not worker_path.startswith("/"):
            raise ValueError("Worker path must be an absolute compute-node path")
        payload = json.dumps(
            {
                "job_id": job_id,
                "script": Path(script_path).read_text(),
                "activation": Path(env_path).read_text() if env_path is not None else "",
            }
        )
        delimiter = f"SLURM_PAYLOAD_{uuid4().hex}"
        args = [
            worker_path,
            "--redis-address",
            redis_address,
            f"--redis-tls={str(redis_tls).lower()}",
        ]
        command = shlex.join(args)
        return f"#!/bin/bash\nexec {command} <<'{delimiter}'\n{payload}\n{delimiter}\n"

    ################################################
    #  Internal Monitoring                         #
    ################################################

    def _start_threads(self) -> None:
        """Start the manager heartbeat-monitor thread."""
        self._heartbeat_monitoring_thread.start()

    def _heartbeat_monitoring_loop(self) -> None:
        """Check missing heartbeats until shutdown and query Slurm when necessary."""

        while not self._stop_heartbeat_thread.wait(timeout=2):
            for sub_info in self._job_tracker.get_missing_heartbeats():
                self._check_slurm_job(sub_info)

    def _check_slurm_job(self, sub_info: SubscriptionInfo) -> None:
        """Resolve a missed heartbeat from controller or accounting records.

        Args:
            sub_info (SubscriptionInfo): Subscription ownership and heartbeat
                bookkeeping.

        Notes:
            Transport failures, absent records, and incomplete accounting leave the job
            unresolved for a later check.
        """
        future = self._job_tracker.begin_scheduler_check(sub_info.job_id)
        if future is None or future.done():
            return
        try:
            record = self.rest_client.get_job(future.slurm_job_id)
            if record is None:
                record = self.rest_client.get_job(future.slurm_job_id, accounting=True)
        except (SlurmRestError, ValueError):
            logger.warning("Could not query Slurm job %s", future.slurm_job_id)
            return
        if not record:
            return
        state = record.get("job_state", record.get("state", []))
        if isinstance(state, dict):
            state = state.get("current", [])
        states = [state] if isinstance(state, str) else (state or [])
        terminal = {
            "COMPLETED",
            "CANCELLED",
            "FAILED",
            "TIMEOUT",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
            "BOOT_FAIL",
            "DEADLINE",
            "PREEMPTED",
            "REVOKED",
        }
        state = next((value for value in states if value in terminal), None)
        if state is None:
            return
        exit_code = _job_exit_code(record.get("exit_code"))
        if state == "COMPLETED" and exit_code == 0:
            status = JobStatus.FINISHED.value
        elif state == "CANCELLED":
            status = JobStatus.CANCELLED.value
        elif state == "COMPLETED" and exit_code is None:
            # A partial accounting record is not proof of failure.
            return
        else:
            status = f"error:Slurm state {state}, exit code {exit_code}"
        self._job_tracker.apply_status(StatusMessage(status=status), future)
