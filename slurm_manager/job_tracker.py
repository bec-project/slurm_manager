"""Own job futures, Redis subscriptions, and callback dispatch."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial, wraps
from logging import getLogger
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from slurm_manager.event_messages import HeartBeatMessage, SlurmMessage, StatusMessage
from slurm_manager.job_future import JobFuture, JobStatus, RegisteredCallback
from slurm_manager.redis_sub_client import RedisSubClient

logger = getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TopicInfo:
    """Helper for topic specification within Redis bec-slurm pub/sub interface."""

    job_id: str
    prefix: str = "info"

    ################################################
    #  Public Properties                           #
    ################################################

    @property
    def log(self) -> str:
        """Return the channel carrying stdout and stderr log messages.

        Returns:
            str: Job log channel.
        """
        return self.topic("log")

    @property
    def event(self) -> str:
        """Return the channel carrying lifecycle status messages.

        Returns:
            str: Job status channel.
        """
        return self.topic("status")

    @property
    def heartbeat(self) -> str:
        """Return the channel carrying worker heartbeats.

        Returns:
            str: Job heartbeat channel.
        """
        return self.topic("heartbeat")

    ################################################
    #  Public Topic Lookup                         #
    ################################################

    def topic(self, v: str) -> str:
        """Build a Redis topic name for a job event.

        Args:
            v (str): Event suffix appended to this job's Redis topic prefix.

        Returns:
            str: Redis channel in prefix/job_id/event form.
        """
        return f"{self.prefix}/{self.job_id}/{v}"


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
    last_scheduler_check: float | None = None


HEARTBEAT_TIMEOUT = 10  # seconds


class JobTracker:
    """Coordinate job state and callback ownership under one lock."""

    def __init__(self, sub_client: RedisSubClient) -> None:
        """Create tracking state and the user callback executor.

        Args:
            sub_client (RedisSubClient): Transport used for job subscriptions; the
                tracker closes it during shutdown.
        """
        self._sub_client = sub_client
        self._future_registry: dict[str, JobFuture] = {}
        self._global_callbacks: dict[str, RegisteredCallback] = {}
        self._subscriptions_by_topic: dict[str, dict[str, SubscriptionInfo]] = defaultdict(dict)
        self._topic_by_subscription_id: dict[str, str] = {}
        self._active_subscriptions: dict[str, set[str]] = defaultdict(set)
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(
            max_workers=8, thread_name_prefix="SlurmManagerCallback"
        )

    ################################################
    #  Public Job Tracking                         #
    ################################################

    def track(self, future: JobFuture) -> None:
        """Register a submitted job and attach internal monitoring callbacks.

        Args:
            future (JobFuture): Submitted job whose state and subscriptions are owned.

        Notes:
            The caller must remove tracking and cancel the held job if setup fails.
        """
        with self._lock:
            self._future_registry[future._job_id] = future
        self._subscribe_to_internal_topics(future, TopicInfo(future._job_id))

    def get_future(self, job_id: str) -> JobFuture | None:
        """Look up a tracked future under the state lock.

        Args:
            job_id (str): Opaque manager job identifier.

        Returns:
            JobFuture | None: Tracked future, or None after cleanup.
        """
        with self._lock:
            return self._future_registry.get(job_id)

    def wait_until_ready(self, job_id: str) -> None:
        """Wait for Redis to acknowledge all subscriptions installed for a job.

        Args:
            job_id (str): Opaque manager job identifier.

        Raises:
            TimeoutError: Redis does not acknowledge the subscriptions in time.
        """
        with self._lock:
            topics = list(self._active_subscriptions.get(job_id, set()))
        self._sub_client.wait_until_subscribed(topics)

    def begin_scheduler_check(self, job_id: str) -> JobFuture | None:
        """Record a scheduler check for an active job before performing network I/O.

        Args:
            job_id (str): Opaque manager job identifier.

        Returns:
            JobFuture | None: Active future, or None if it has completed or was removed.
        """
        with self._lock:
            future = self._future_registry.get(job_id)
            if future is None or future.done():
                return None
            topic = TopicInfo(job_id).heartbeat
            for info in self._subscriptions_by_topic.get(topic, {}).values():
                if info.last_heartbeat is not None:
                    info.last_scheduler_check = time.time()
            return future

    def apply_status(self, message: StatusMessage, future: JobFuture) -> None:
        """Apply a worker status and clean up terminal job subscriptions.

        Args:
            message (StatusMessage): Raw Redis payload or parsed worker message, as
                specified by the annotation.
            future (JobFuture): Tracked future whose lifecycle or subscriptions are
                being managed.

        Notes:
            Statuses use status[:detail]. Unknown statuses fail the job and follow the
            same cleanup path as an explicit error.
        """
        info = message.status.split(":", 1)
        try:
            status = JobStatus(info[0])
        except ValueError:
            logger.warning("Received unknown job status '%s' for job %s", info[0], future.name)
            detail = f"Received unknown job status '{info[0]}' from message: {message.model_dump()}"
            message = StatusMessage(status=f"error:{detail}", metadata=message.metadata.copy())
            status = JobStatus.ERROR
            info = [status.value, detail]

        with self._lock:
            if future.done():
                return
            topic = TopicInfo(future._job_id).event
            callbacks = list(self._subscriptions_by_topic.get(topic, {}).values())
            exc_info = info[1] if len(info) > 1 else None
            future._update_status(status, exc_info)
            # Queue notifications before cleanup or concurrent shutdown can discard them.
            # User code runs on the executor, never on this thread under the state lock.
            for subscription in callbacks:
                if subscription.callback is not None:
                    subscription.callback(message)
            self._dispatch_global(message, future._job_id, future.slurm_job_id)
            if status in (JobStatus.ERROR, JobStatus.FINISHED, JobStatus.CANCELLED):
                self.remove_subscriptions_for_job(future._job_id, skip_event_callback=True)

    def get_missing_heartbeats(self) -> list[SubscriptionInfo]:
        """Select stale heartbeat subscriptions whose scheduler check is due.

        Returns:
            list[SubscriptionInfo]: Copies of internal heartbeat subscriptions needing
                a Slurm query. Mutating a copy does not change tracking state.
        """
        missing = []
        now = time.time()
        with self._lock:
            for job_id, topics in self._active_subscriptions.items():
                topic = TopicInfo(job_id=job_id).heartbeat
                if topic not in topics:
                    continue
                for sub_info in self._subscriptions_by_topic.get(topic, {}).values():
                    if not sub_info.last_heartbeat:
                        continue
                    if now - sub_info.last_heartbeat <= HEARTBEAT_TIMEOUT:
                        continue
                    last_check = sub_info.last_scheduler_check
                    if last_check is not None and now - last_check <= HEARTBEAT_TIMEOUT:
                        continue
                    missing.append(sub_info.model_copy())
        return missing

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
        if event_type not in ("heartbeat", "log", "status"):
            raise ValueError("Event type must be log, status, or heartbeat")
        with self._lock:
            if job_id is None:
                callback_id = str(uuid4())
                self._global_callbacks[callback_id] = RegisteredCallback(
                    id=callback_id, event=event_type, callback=callback
                )
                return [callback_id]
            future = self._future_registry[job_id]
        return [future.listen(event_type, callback)]

    def add_subscription(
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

        topic_str = TopicInfo(job_id=job_id).topic(key)

        @wraps(callback)
        def wrapped_callback(
            message: SlurmMessage,
            executor: ThreadPoolExecutor,
            job_id: str,
            slurm_job_id: str,
            callback_kwargs: dict[str, Any] | None = None,
        ) -> None:
            """Enrich a parsed worker message and dispatch its registered handler.

            Args:
                message (SlurmMessage): Raw Redis payload or parsed worker message, as
                    specified by the annotation.
                executor (ThreadPoolExecutor): Callback executor that runs user code
                    away from the listener thread.
                job_id (str): Opaque manager identifier used in Redis topic names.
                slurm_job_id (str): Numeric scheduler identifier returned by Slurm.
                callback_kwargs (dict[str, Any] | None): Additional keyword arguments
                    passed to the user callback.
            """
            message.metadata.update(
                {
                    "msg_received": datetime.now().isoformat(),
                    "job_id": job_id,
                    "slurm_job_id": slurm_job_id,
                }
            )
            executor.submit(self._run_callback, callback, message, callback_kwargs or {})

        with self._lock:
            future = self._future_registry.get(job_id)
            if future is None or future.done():
                raise RuntimeError(f"Job {job_id} is no longer active")
        dispatch = partial(
            wrapped_callback,
            executor=self._executor,
            callback_kwargs=callback_kwargs,
            job_id=job_id,
            slurm_job_id=future.slurm_job_id,
        )
        # Registration may block. Let lifecycle updates proceed, then validate ownership
        # under the state lock and undo the Redis subscription if the job finished.
        redis_callback = self._ignore_status_message if key == "status" else dispatch
        cb_id = self._sub_client.subscribe(topic_str, redis_callback)
        with self._lock:
            if self._future_registry.get(job_id) is not future or future.done():
                self._sub_client.unsubscribe(cb_id)
                raise RuntimeError(f"Job {job_id} finished during subscription registration")

            sub_info = SubscriptionInfo(
                job_id=job_id,
                topic=topic_str,
                slurm_job_id=future.slurm_job_id,
                callback=dispatch,
                last_heartbeat=None,
                callback_id=cb_id,
                heartbeat_received=False,
            )
            self._register_subscription_for_job(sub_info=sub_info)
            return cb_id

    def remove_subscription_by_id(self, callback_id: str | list[str]) -> None:
        """Remove the selected callbacks while preserving other topic listeners.

        Args:
            callback_id (str | list[str]): Subscription ID, or IDs where accepted,
                returned during registration.
        """
        if isinstance(callback_id, str):
            callback_id = [callback_id]
        with self._lock:
            for cb_id in callback_id:
                if self._global_callbacks.pop(cb_id, None) is not None:
                    continue
                topic = self._topic_by_subscription_id.pop(cb_id, None)
                if topic is None:
                    continue

                callbacks = self._subscriptions_by_topic.get(topic, {})
                sub_info = callbacks.pop(cb_id, None)
                self._sub_client.unsubscribe(cb_id)

                # Other callbacks, including heartbeat monitoring, still own this topic.
                if callbacks:
                    continue

                self._subscriptions_by_topic.pop(topic, None)
                if sub_info is None:
                    continue

                topics = self._active_subscriptions.get(sub_info.job_id)
                if topics is not None:
                    topics.discard(topic)

    def remove_subscription_by_topic(self, topic: str) -> None:
        """Remove all callbacks for one topic and unsubscribe it from Redis.

        Args:
            topic (str): Redis channel whose callback registrations are selected.
        """
        with self._lock:
            callbacks = self._subscriptions_by_topic.pop(topic, {})
            for callback_id, sub_info in callbacks.items():
                self._topic_by_subscription_id.pop(callback_id, None)
                job_id = sub_info.job_id
                self._active_subscriptions[job_id].discard(topic)

            # Remove the topic subscription from Redis
            self._sub_client.unsubscribe_topic(topic)

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
        with self._lock:
            future = self._future_registry.get(job_id)
            if future is not None and not future.done() and not skip_event_callback:
                self.apply_status(
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
                return
            topics = self._active_subscriptions.pop(job_id, set())
            for topic in topics:
                callbacks = self._subscriptions_by_topic.pop(topic, {})
                for callback_id in callbacks:
                    self._topic_by_subscription_id.pop(callback_id, None)
                self._sub_client.unsubscribe_topic(topic)
            self._remove_job_future(job_id)

    ################################################
    #  Public Lifecycle                            #
    ################################################

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Stop Redis delivery and drain accepted user callbacks.

        Args:
            timeout (float | None): Redis listener join timeout in seconds; None waits
                indefinitely. Callback draining has no timeout.

        Notes:
            Stop scheduler monitoring before calling this method. Remote jobs are
            not cancelled and tracked futures are not marked complete.
        """
        with self._lock:
            self._global_callbacks.clear()
            self._subscriptions_by_topic.clear()
            self._topic_by_subscription_id.clear()
        self._sub_client.shutdown(timeout=timeout)
        self._executor.shutdown(wait=True, cancel_futures=False)

    ################################################
    #  Internal Subscriptions                      #
    ################################################

    @staticmethod
    def _run_callback(
        callback: Callable[[SlurmMessage], None],
        message: SlurmMessage,
        callback_kwargs: dict[str, Any],
    ) -> None:
        """Execute a user callback and log asynchronous failures.

        Args:
            callback (Callable[[SlurmMessage], None]): User callback to execute.
            message (SlurmMessage): Parsed job message delivered to the callback.
            callback_kwargs (dict[str, Any]): Additional callback keyword arguments.
        """
        try:
            callback(message, **callback_kwargs)
        except Exception:
            logger.exception(
                "Error in %s callback for job %s", message.msg_type, message.metadata.get("job_id")
            )

    @staticmethod
    def _ignore_status_message(message: SlurmMessage) -> None:
        """Leave status dispatch to the internal lifecycle subscription.

        Args:
            message (SlurmMessage): Redis status already handled by apply_status.
        """

    def _dispatch_global(
        self, message: SlurmMessage, job_id: str, slurm_job_id: str | None
    ) -> None:
        """Queue manager-wide callbacks for an event from any tracked job.

        Args:
            message (SlurmMessage): Parsed job event to dispatch.
            job_id (str): Opaque manager job identifier.
            slurm_job_id (str | None): Scheduler ID added to metadata when available.
        """
        with self._lock:
            callbacks = [
                item for item in self._global_callbacks.values() if item.event == message.msg_type
            ]
            if not callbacks:
                return
            metadata = {
                **message.metadata,
                "msg_received": datetime.now().isoformat(),
                "job_id": job_id,
            }
            if slurm_job_id is not None:
                metadata["slurm_job_id"] = slurm_job_id
            for item in callbacks:
                event = message.model_copy(update={"metadata": metadata.copy()})
                self._executor.submit(self._run_callback, item.callback, event, {})

    def _subscribe_to_log(self, topic_info: TopicInfo, future: JobFuture) -> None:
        """Install one log subscription for current and future global listeners.

        Args:
            topic_info (TopicInfo): Redis topic names for the tracked job.
            future (JobFuture): Tracked job supplying event metadata.
        """
        callback = partial(
            self._dispatch_global, job_id=future._job_id, slurm_job_id=future.slurm_job_id
        )
        cb_id = self._sub_client.subscribe(topic_info.log, callback)
        self._register_subscription_for_job(
            SubscriptionInfo(job_id=future._job_id, topic=topic_info.log, callback_id=cb_id)
        )

    def _subscribe_to_internal_topics(self, future: JobFuture, topic_info: TopicInfo) -> None:
        """Attach the manager status, heartbeat, and log callbacks before release.

        Args:
            future (JobFuture): Tracked future whose lifecycle or subscriptions are
                being managed.
            topic_info (TopicInfo): Redis topic names for the tracked job.
        """
        self._subscribe_to_heartbeat(topic_info, future)
        self._subscribe_to_event(topic_info, future)
        self._subscribe_to_log(topic_info, future)

    def _subscribe_to_heartbeat(self, topic_info: TopicInfo, future: JobFuture) -> None:
        """Register the internal heartbeat callback and its initial timestamp.

        Args:
            topic_info (TopicInfo): Redis topic names for the tracked job.
            future (JobFuture): Tracked future whose lifecycle or subscriptions are
                being managed.
        """
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
            """Enrich a parsed worker message and dispatch its registered handler.

            Args:
                message (HeartBeatMessage): Raw Redis payload or parsed worker message,
                    as specified by the annotation.
                sub_info (SubscriptionInfo): Subscription ownership and heartbeat
                    bookkeeping.
            """
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
                logger.error(
                    "Error in callback for topic %s: %s", message.msg_type, message.timestamp
                )

        cb_id = self._sub_client.subscribe(topic, partial(wrapped_callback, sub_info=sub_info))
        sub_info.callback_id = cb_id
        sub_info.last_heartbeat = time.time()
        self._register_subscription_for_job(sub_info=sub_info)

    def _subscribe_to_event(self, topic_info: TopicInfo, future: JobFuture) -> None:
        """Register the internal callback for terminal and running statuses.

        Args:
            topic_info (TopicInfo): Redis topic names for the tracked job.
            future (JobFuture): Tracked future whose lifecycle or subscriptions are
                being managed.
        """
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
            """Enrich a parsed worker message and dispatch its registered handler.

            Args:
                message (StatusMessage): Raw Redis payload or parsed worker message, as
                    specified by the annotation.
                future (JobFuture): Tracked future whose lifecycle or subscriptions are
                    being managed.
            """
            try:
                message.metadata.update(
                    {
                        "msg_received": datetime.now().isoformat(),
                        "job_id": future._job_id,
                        "slurm_job_id": future.slurm_job_id,
                    }
                )
                self.apply_status(message, future=future)
            except Exception:
                logger.error("Error in callback for topic %s: %s", message.msg_type, message.status)

        cb_id = self._sub_client.subscribe(event_topic, partial(wrapped_callback, future=future))
        sub_info.callback_id = cb_id
        self._register_subscription_for_job(sub_info=sub_info)

    def _register_subscription_for_job(self, sub_info: SubscriptionInfo) -> None:
        """Index a subscription by callback ID, topic, and job.

        Args:
            sub_info (SubscriptionInfo): Subscription ownership and heartbeat
                bookkeeping.

        Raises:
            ValueError: The subscription does not contain a callback ID.
        """
        with self._lock:
            if sub_info.callback_id is None:
                raise ValueError("SubscriptionInfo.callback_id must be set before registration")
            self._subscriptions_by_topic[sub_info.topic][sub_info.callback_id] = sub_info
            self._topic_by_subscription_id[sub_info.callback_id] = sub_info.topic
            self._active_subscriptions[sub_info.job_id].add(sub_info.topic)

    def _heartbeat_callback(self, message: HeartBeatMessage, sub_info: SubscriptionInfo) -> None:
        """Record heartbeat receipt for the monitored subscription.

        Args:
            message (HeartBeatMessage): Raw Redis payload or parsed worker message, as
                specified by the annotation.
            sub_info (SubscriptionInfo): Subscription ownership and heartbeat
                bookkeeping.
        """
        with self._lock:
            sub_info.heartbeat_received = True
            sub_info.last_heartbeat = time.time()
            self._dispatch_global(message, sub_info.job_id, sub_info.slurm_job_id)

    def _remove_job_future(self, job_id: str) -> None:
        """Remove a future from the manager registry under its lock.

        Args:
            job_id (str): Opaque manager identifier used in Redis topic names.
        """
        with self._lock:
            self._future_registry.pop(job_id, None)
