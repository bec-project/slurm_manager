"""Minimal Slurm manager draft for sbatch + JobFuture creation."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial, wraps
from logging import getLogger
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from slurm_manager.event_messages import HeartBeatMessage, SlurmMessage, StatusMessage
from slurm_manager.job_future import JobFuture, JobStatus
from slurm_manager.redis_sub_client import RedisSubClient

logger = getLogger(__name__)

_STOP = object()


@dataclass(frozen=True, slots=True)
class TopicInfo:
    """Helper for topic specification within Redis bec-slurm pub/sub interface."""

    job_id: str
    prefix: str = "info"

    def topic(self, v: str) -> str:
        """Constructs a new topic string for a given key."""
        return f"{self.prefix}/{self.job_id}/{v}"

    @property
    def log(self) -> str:
        """Topic for log messages."""
        return self.topic("log")

    @property
    def event(self) -> str:
        """Topic for event messages."""
        return self.topic("status")

    @property
    def heartbeat(self) -> str:
        """Topic for heartbeat messages."""
        return self.topic("heartbeat")


class SubscriptionInfo(BaseModel):
    """Helper for slurm job information and future tracking."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    topic: str
    job_id: str
    slurm_job_id: str | None = None
    callback: Callable[[SlurmMessage], None] | None = None
    callback_id: str | None = None
    last_heartbeat: float | None = None
    heartbeat_received: bool = False


HEARTBEAT_TIMEOUT = 10  # seconds


class SlurmManager:
    """Small manager that submits wrapper jobs and returns JobFuture handles."""

    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379) -> None:
        self.sub_client = RedisSubClient(bootstrap=f"{redis_host}:{redis_port}")
        self._wrapper_path = Path(__file__).resolve().parent / "job_submission" / "wrapper.sh"
        self._redis_script_path = (
            Path(__file__).resolve().parent / "job_submission" / "redis_worker.sh"
        )

        # This is a job_id mapping to job futures
        self._future_registry: dict[str, JobFuture] = {}
        # This subscription id sub_info mapping by topics, used for callback handling
        self._subscriptions_by_topic: dict[str, dict[str, SubscriptionInfo]] = defaultdict(dict)
        # This is a topic mapping to subscriptions
        self._topic_by_subscription_id: dict[str, str] = {}
        # This is a job_id mapping to topics
        self._active_subscriptions: dict[str, set[str]] = defaultdict(set)

        # Threading
        self._lock = threading.RLock()
        self._stop_heartbeat_thread = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="SlurmManagerCallback")
        # Heartbeat monitoring thread
        self._heartbeat_monitoring_thread = threading.Thread(
            target=self._heartbeat_monitoring_loop, name="SlurmManagerHeartbeatMonitor", daemon=True
        )
        self._start_threads()

    def _start_threads(self):
        """Starts the heartbeat monitoring thread."""
        self._heartbeat_monitoring_thread.start()

    def _heartbeat_monitoring_loop(self) -> None:
        """Internal loop to check for missing heartbeats. If a heartbeat for a job is missing for
        more than HEARTBEAT_TIMEOUT seconds, we will trigger a check of the slurm job status,
        and potentially trigger a cleanup of the job future and subscriptions if the job is no longer active.
        """

        while not self._stop_heartbeat_thread.wait(timeout=2):
            missing_heartbeats = []
            # Check heartbeats for active jobs
            with self._lock:
                for _, topics in self._active_subscriptions.items():
                    for topic in topics:
                        infos = self._subscriptions_by_topic.get(topic, {})
                        for sub_info in infos.values():
                            if sub_info and sub_info.last_heartbeat:
                                if time.monotonic() - sub_info.last_heartbeat > HEARTBEAT_TIMEOUT:
                                    missing_heartbeats.append(sub_info)

            for sub_info in missing_heartbeats:
                self._check_slurm_job(sub_info)

    def _check_slurm_job(self, sub_info: SubscriptionInfo) -> None:
        """Check if a SLURM job is active"""
        # TODO check if job is Pending through SLURM Rest API..
        # Currently we just trigger a cleanup if heartbea is missing.
        future = self._future_registry.get(sub_info.job_id)
        if not future:
            return
        cancel_msg = f"error:No heartbeat received within {HEARTBEAT_TIMEOUT}s for topic {sub_info.topic} and slurm JobID {sub_info.slurm_job_id}."
        if sub_info.heartbeat_received is True:
            cancel_msg += (
                " Last heartbeat from job at "
                f"{datetime.fromtimestamp(sub_info.last_heartbeat).isoformat()}."
            )

        self._event_callback(
            StatusMessage(
                status=cancel_msg,
                metadata={
                    "msg_received": datetime.now().isoformat(),
                    "job_id": future._job_id,
                    "slurm_job_id": future.slurm_job_id,
                },
            ),
            future=future,
        )

    def submit_job(self, script_path: str, env_path: str) -> JobFuture:
        """Create a temporary script from command and submit via sbatch wrapper."""
        # Check that the wrapper script and redis worker script exist before submitting the job
        if not self._wrapper_path.exists():
            raise FileNotFoundError(f"wrapper script not found: {self._wrapper_path}")
        if not self._redis_script_path.exists():
            raise FileNotFoundError(f"redis worker script not found: {self._redis_script_path}")

        env_path = os.path.abspath(env_path)
        script_path = os.path.abspath(script_path)

        job_id = str(uuid4())  # unique job_id for hash-key in Redis.
        sbatch_cmd = [
            "sbatch",
            "--parsable",
            os.path.abspath(self._wrapper_path),
            env_path,
            script_path,
            job_id,
            "1",
            "1",
            os.path.abspath(self._redis_script_path),
        ]
        logger.debug("Submitting job with command: %s", " ".join(sbatch_cmd))

        result = self._run_subprocess(sbatch_cmd)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"sbatch failed: {stderr}")
        slurm_job_id = result.stdout.strip()  # ID of job from SLURM scheduler
        logger.debug("SBatch job submission id: %s", slurm_job_id)

        job_future = JobFuture(
            job_id=job_id, manager=self, registry=self._future_registry, slurm_job_id=slurm_job_id
        )
        topic_info = TopicInfo(job_id=job_id)
        self._future_registry[job_id] = job_future
        self._subscribe_to_internal_topics(future=job_future, topic_info=topic_info)
        return job_future

    def cancel_job(self, slurm_job_id: str, job_id: str) -> None:
        """Cancel a job via scancel."""
        result = self._run_subprocess(["scancel", slurm_job_id])
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            raise RuntimeError(f"scancel failed: {stderr}")

        job_future = self._future_registry.get(job_id)
        if job_future:
            self._event_callback(
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

    def listen(
        self,
        event_type: Literal["heartbeat", "log", "status"],
        callback: Callable[[StatusMessage], None],
        job_id: str | None = None,
    ) -> list[str]:
        """
        Register a callback to listen for events of the specified type ('heartbeat', 'log', or 'status').
        Additional keyword arguments for the callback can be passed via callback_kwargs dictionary. These
        will be passed to the callback when it is executed. If the job_id is specified, the callback will
        only be registered for the specific job. If job_id is None, the callback will be registered for all jobs
        Returns a callback ID that can be used to unregister.

        Args:
            event_type: The type of event to listen for ('heartbeat', 'log', or 'status').
            callback: A function that takes a Message object as input and is called when an event of the specified type is received for any job.
                      The Message object contains two fields, 'topic', 'data'

        Returns:
            A list of unique callback IDS will be returned which can be used to unregister the callbacks later.
        """
        job_futures: list[JobFuture] = (
            [self._future_registry[job_id]] if job_id else list(self._future_registry.values())
        )
        for future in job_futures:
            future.listen(event_type=event_type, callback=callback)

    def unlisten(self, callback_id: str | list[str]) -> None:
        self.remove_subscription_by_id(callback_id)

    def remove_subscription_by_id(self, callback_id: str | list[str]) -> None:
        """Remove a subscription by its callback ID."""
        if isinstance(callback_id, str):
            callback_id = [callback_id]
        with self._lock:
            for cb_id in callback_id:
                topic = self._topic_by_subscription_id.pop(cb_id, None)
                if topic is not None:
                    callbacks = self._subscriptions_by_topic.get(topic, {})
                    sub_info = callbacks.pop(cb_id, None)
                    if sub_info:
                        job_id = sub_info.job_id
                        self._active_subscriptions[job_id].discard(topic)

                    # If no more callbacks for the topic, remove the topic subscription
                    if len(callbacks) == 0:
                        self._subscriptions_by_topic.pop(topic, None)
                        self.sub_client.unsubscribe_topic(topic)

    def remove_subscription_by_topic(self, topic: str) -> None:
        """Remove a subscription by its topic."""
        with self._lock:
            callbacks = self._subscriptions_by_topic.pop(topic, {})
            for callback_id, sub_info in callbacks.items():
                self._topic_by_subscription_id.pop(callback_id, None)
                job_id = sub_info.job_id
                self._active_subscriptions[job_id].discard(topic)

            # Remove the topic subscription from Redis
            self.sub_client.unsubscribe_topic(topic)

    def remove_subscriptions_for_job(self, job_id: str, skip_event_callback: bool = False) -> None:
        """
        Remove all subscriptions for a specific job. If skip_event_callback is False, this will also trigger
        the event callback with an error status which will update the JobFuture status.
        This can be used to trigger a cleanup of the Job and its subscriptions.

        If it finished in success, it should be called with skip_event_callback=True to avoid overwriting
        the finished status with an error status.

        Args:
            job_id: The ID of the job for which to remove subscriptions.
            skip_event_callback: If True, the event callback will not be triggered with an error status.
                                 This should be used when the job is finishing successfully to avoid overwriting the
                                 finished status with an error status.
        """
        with self._lock:
            topics = self._active_subscriptions.pop(job_id, set())
            for topic in topics:
                callbacks = self._subscriptions_by_topic.pop(topic, {})
                for callback_id in callbacks.keys():
                    self._topic_by_subscription_id.pop(callback_id, None)
                self.sub_client.unsubscribe_topic(topic)
            future = self._future_registry.get(job_id)
            if future:
                # This callback will trigger a cleanup
                if not skip_event_callback:
                    self._event_callback(
                        StatusMessage(
                            status=f"error:Removal requested by user; timestamp {datetime.now().isoformat()}",
                            metadata={
                                "msg_received": datetime.now().isoformat(),
                                "job_id": future._job_id,
                                "slurm_job_id": future.slurm_job_id,
                            },
                        ),
                        future,
                    )
                self._remove_job_future(future._job_id)

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Shuts down the manager, including the heartbeat monitoring thread and Redis subscriptions."""
        self._stop_heartbeat_thread.set()
        with self._lock:
            self._subscriptions_by_topic.clear()
            self._topic_by_subscription_id.clear()

        self.sub_client.shutdown(timeout=timeout)
        self.executor.shutdown(wait=True, cancel_futures=True)

    ###################
    ### Internal Methods
    ###################

    def _add_subscription(
        self,
        job_id: str,
        key: Literal["heartbeat", "log", "status"],
        callback: Callable[[SlurmMessage], None],
        callback_kwargs: dict[str, Any] | None = None,
    ) -> str:
        """
        Register a callback to listen for events of the specified type ('heartbeat', 'log', or 'status') for a specific job.
        Callbacks receive a SlurmMessage object, depending on the type of event they listen to. The callback will be executed
        in the background and errors will be logged but not raised to not jeopardize the main loop of the manager.
        The callback will be executed with additional keyword arguments passed via callback_kwargs dictionary.

        Args:
            job_id: The ID of the job for which to register the callback.
            key: The type of event to listen for ('heartbeat', 'log', or 'status').
            callback: A function that takes a SlurmMessage object as input and is called when an event of the specified type is received for the job.
            callback_kwargs: A dictionary of additional keyword arguments to pass to the callback when it is executed
        """

        topic_str = TopicInfo(job_id=job_id).topic(key)

        @wraps(callback)
        def wrapped_callback(
            message: SlurmMessage,
            executor: ThreadPoolExecutor,
            job_id: str,
            slurm_job_id: str,
            callback_kwargs: dict[str, Any] | None = None,
        ) -> None:
            message.metadata.update(
                {
                    "msg_received": datetime.now().isoformat(),
                    "job_id": job_id,
                    "slurm_job_id": slurm_job_id,
                }
            )
            executor.submit(callback, message, **(callback_kwargs or {}))

        future = self._future_registry.get(job_id)
        if future is None:
            logger.warning(
                "Trying to add subscription for job_id %s which does not exist in future registry.",
                job_id,
            )
            return ""
        cb_id = self.sub_client.subscribe(
            topic_str,
            partial(
                wrapped_callback,
                executor=self.executor,
                callback_kwargs=callback_kwargs,
                job_id=job_id,
                slurm_job_id=future.slurm_job_id,
            ),
        )

        sub_info = SubscriptionInfo(
            job_id=job_id,
            topic=topic_str,
            slurm_job_id=future.slurm_job_id,
            callback=wrapped_callback,
            last_heartbeat=None,
            callback_id=cb_id,
            heartbeat_received=False,
        )
        self._register_subscription_for_job(sub_info=sub_info)
        return cb_id

    def _remove_job_future(self, job_id: str) -> None:
        """Remove a job future from the registry."""
        with self._lock:
            self._future_registry.pop(job_id, None)

    def _run_subprocess(self, cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, capture_output=True, text=True, check=False)

    def _subscribe_to_internal_topics(self, future: JobFuture, topic_info: TopicInfo) -> None:
        self._subscribe_to_heartbeat(topic_info, future)
        self._subscribe_to_event(topic_info, future)

    def _subscribe_to_heartbeat(self, topic_info: TopicInfo, future: JobFuture) -> None:
        sub_info = SubscriptionInfo(
            job_id=topic_info.job_id,
            topic=topic_info.heartbeat,
            slurm_job_id=future.slurm_job_id,
            callback=None,
            last_heartbeat=None,
            callback_id=None,
            heartbeat_received=False,
        )
        topic = topic_info.heartbeat

        def wrapped_callback(message: HeartBeatMessage, sub_info: SubscriptionInfo) -> None:
            try:
                message.metadata.update(
                    {
                        "msg_received": datetime.now().isoformat(),
                        "job_id": sub_info.job_id,
                        "slurm_job_id": sub_info.slurm_job_id,
                    }
                )
                self._heartbeat_callback(message, sub_info)
            except Exception:
                logger.error("Error in callback for topic %s: %s", message.topic, message.payload)

        cb_id = self.sub_client.subscribe(topic, partial(wrapped_callback, sub_info=sub_info))
        sub_info.callback_id = cb_id
        sub_info.last_heartbeat = time.monotonic()
        self._register_subscription_for_job(sub_info=sub_info)

    def _heartbeat_callback(self, message: HeartBeatMessage, sub_info: SubscriptionInfo) -> None:
        sub_info.heartbeat_received = True
        sub_info.last_heartbeat = time.monotonic()

    def _subscribe_to_event(self, topic_info: TopicInfo, future: JobFuture) -> None:
        sub_info = SubscriptionInfo(
            job_id=topic_info.job_id,
            topic=topic_info.event,
            slurm_job_id=future.slurm_job_id,
            callback=None,
            last_heartbeat=None,
            callback_id=None,
            heartbeat_received=False,
        )
        event_topic = topic_info.event

        def wrapped_callback(message: StatusMessage, future: JobFuture) -> None:
            try:
                message.metadata.update(
                    {
                        "msg_received": datetime.now().isoformat(),
                        "job_id": future._job_id,
                        "slurm_job_id": future.slurm_job_id,
                    }
                )
                self._event_callback(message, future=future)
            except Exception:
                logger.error("Error in callback for topic %s: %s", message.topic, message.payload)

        cb_id = self.sub_client.subscribe(event_topic, partial(wrapped_callback, future=future))
        sub_info.callback_id = cb_id
        self._register_subscription_for_job(sub_info=sub_info)

    def _register_subscription_for_job(self, sub_info: SubscriptionInfo) -> None:
        with self._lock:
            if sub_info.callback_id is None:
                raise ValueError("SubscriptionInfo.callback_id must be set before registration")
            self._subscriptions_by_topic[sub_info.topic][sub_info.callback_id] = sub_info
            self._topic_by_subscription_id[sub_info.callback_id] = sub_info.topic
            self._active_subscriptions[sub_info.job_id].add(sub_info.topic)

    def _event_callback(self, message: StatusMessage, future: JobFuture) -> None:
        """
        Callback for handling event message updates. The method should also be used to trigger
        a cleanup of subscriptions and job future when a job is finishing unexpectedly
        (e.g. due to missing heartbeat or user requested removal). If finishing unexpectedly,
        we recommend using a payload format of 'error:reason' to indicate the error and reason
        for the job finishing, which will be reflected in the JobFuture status and exception info.

        Args:
            message: The incoming message from the event topic, expected to have a payload format of 'status:optional_info'.
            future: The JobFuture associated with the job, which will be updated based on the event message.
        """
        info = message.status.split(":", 1)
        status = info[0]
        try:  # Catch irregular formatted status messages to avoid crashing the callback loop.
            status = JobStatus(status)
        except ValueError:
            logger.warning("Received unknown job status '%s' for job %s", status, future.name)
            future._update_status(
                "error",
                f"Received unknown job status '{status}' from message: {message.model_dump()}",
            )
            return
        exc_info = info[1] if len(info) > 1 else None
        future._update_status(status, exc_info)
        # Clean up subscriptions for finished job
        if status in [JobStatus.ERROR, JobStatus.FINISHED, JobStatus.CANCELLED]:
            self.remove_subscriptions_for_job(future._job_id, skip_event_callback=True)
