"""
Module defining a JobFuture class that represents a Slurm job and allows users to check
its status, register callbacks for events and wait for its completion.

The JobFuture class provides a high-level interface for interacting with Slurm jobs.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Callable, Literal

from slurm_manager.event_messages import SlurmMessage

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma no cover
    from slurm_manager.slurm_manager import SlurmManager


class JobStatus(StrEnum):
    """High-level job status for future-like interface."""

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class RegisteredCallback:
    """Represents a registered callback for a specific event type on a JobFuture."""

    id: str
    event: Literal["status", "log", "heartbeat"]
    callback: Callable[[SlurmMessage], None]


class JobFuture:
    """
    A future-like object that represents a Slurm job. It allows users to check the status of the job,
    wait for completion, and register callbacks for status, log, and heartbeat events.
    """

    def __init__(
        self, job_id: str, manager: SlurmManager, slurm_job_id: str, name: str | None = None
    ) -> None:
        """Create a future for one job tracked by its submitting manager.

        Args:
            job_id (str): Opaque manager identifier used in Redis topic names.
            manager (SlurmManager): Manager that owns the job and handles subscriptions
                and cancellation.
            slurm_job_id (str): Numeric scheduler identifier returned by Slurm.
            name (str | None): Display-name prefix; None uses Job, followed by the
                scheduler ID.
        """
        self._job_id = job_id
        self._manager = manager
        self._slurm_job_id = slurm_job_id.strip()
        self._future: Future[None] = Future()
        if name is None:
            name = "Job"
        self.name = f"{name}-{self._slurm_job_id}"
        self._status: str = JobStatus.PENDING.value
        self._lock = threading.RLock()
        self._callback_registry: dict[str, list[RegisteredCallback]] = {}

    ################################################
    #  Public Properties                           #
    ################################################

    @property
    def slurm_job_id(self) -> str:
        """Return the numeric scheduler job ID on the configured REST endpoint.

        Returns:
            str: Scheduler job ID.
        """
        return self._slurm_job_id

    @property
    def status(self) -> str | None:
        """Return the most recently accepted lifecycle status.

        Returns:
            str | None: Pending, running, finished, error, or cancelled status.
        """
        return self._status

    ################################################
    #  Public Job Lifecycle                        #
    ################################################

    def done(self) -> bool:
        """Check whether the job reached any terminal state.

        Returns:
            bool: True for a finished, failed, or cancelled job.
        """
        return self._future.done()

    def success(self) -> bool:
        """Check whether the job reached a successful terminal state.

        Returns:
            bool: True only when the job finished without an exception.
        """
        return self._future.done() and self._future.exception() is None

    def exception(self) -> Exception | None:
        """Return the recorded terminal exception without waiting.

        Returns:
            Exception | None: Failure or cancellation exception, or None if pending or
                successful.
        """
        if self._future.done():
            return self._future.exception()
        return None

    def wait(self, timeout: float | None = None) -> None:
        """Wait for the job and propagate its failure or cancellation.

        Args:
            timeout (float | None): Maximum wait in seconds; None waits indefinitely
                without cancelling the job.

        Raises:
            TimeoutError: The job does not reach a terminal state before the timeout.
            Exception: The job fails or is cancelled.

        Notes:
            A timeout does not cancel the job. Callback execution may still be pending
            when this method returns; manager shutdown drains accepted callbacks.
        """
        self._future.result(timeout=timeout)

    def cancel(self) -> None:
        """Request cancellation through the owning manager.

        Raises:
            SlurmRestError: Slurm rejects cancellation or the request fails.
        """
        self._manager.cancel_job(slurm_job_id=self.slurm_job_id, job_id=self._job_id)

    ################################################
    #  Public Event Subscriptions                  #
    ################################################

    def listen(
        self,
        event_type: Literal["status", "log", "heartbeat"],
        callback: Callable[[SlurmMessage], None],
    ) -> str:
        """Register an asynchronous callback for a job event.

        Args:
            event_type (Literal['status', 'log', 'heartbeat']): Event to receive: log,
                status, or heartbeat.
            callback (Callable[[SlurmMessage], None]): Callable receiving the parsed
                Slurm message.

        Returns:
            str: Callback ID accepted by unlisten().

        Raises:
            RuntimeError: The job has finished or is no longer tracked.
        """
        cb_id = self._manager._add_subscription(
            job_id=self._job_id, key=event_type, callback=callback
        )
        cb = RegisteredCallback(id=cb_id, event=event_type, callback=callback)
        with self._lock:
            if event_type not in self._callback_registry:
                self._callback_registry[event_type] = [cb]
            else:
                self._callback_registry[event_type].append(cb)
        return cb.id

    def unlisten(self, callback_id: str) -> None:
        """Remove a previously registered callback by ID.

        Args:
            callback_id (str): Subscription ID, or IDs where accepted, returned during
                registration.
        """
        # Manager cleanup may acquire this future's lock while holding its own.
        # Remove local ownership before acquiring the manager lock.
        if self._remove_callback(callback_id):
            self._manager.remove_subscription_by_id(callback_id=callback_id)

    ################################################
    #  Internal Callback Management                #
    ################################################

    def _remove_callback(self, callback_id: str) -> bool:
        """Remove local callback ownership without acquiring the manager lock.

        Args:
            callback_id (str): ID of the callback to remove.

        Returns:
            bool: True if a registered callback was removed.
        """
        with self._lock:
            for event, callbacks in self._callback_registry.items():
                for cb in callbacks:
                    if cb.id != callback_id:
                        continue
                    callbacks.remove(cb)
                    if not callbacks:
                        self._callback_registry.pop(event)
                    return True
        return False

    ################################################
    #  Internal State Updates                      #
    ################################################

    def _update_status(self, status: str, exc_info: str | None = None) -> None:
        """Apply a lifecycle update under the future lock, ignoring late updates.

        Args:
            status (str): Lifecycle status; a terminal state completes the underlying
                future.
            exc_info (str | None): Optional worker-provided failure details.
        """
        with self._lock:
            if self._future.done():
                return
            self._status = status
            if status == JobStatus.FINISHED:
                self._set_finished()
            elif status in (JobStatus.CANCELLED, JobStatus.ERROR):
                self._set_exception(status, exc_info)

    def _set_finished(self) -> None:
        """Complete the underlying future successfully unless it is already done."""
        if self._future.done():
            logger.warning(
                "Job %s is already marked as done. Ignoring _set_finished call.", self.name
            )
            return
        self._future.set_result(None)

    def _set_exception(self, status: str, exc_info: str | None = None) -> None:
        """Record a terminal job failure with optional worker details.

        Args:
            status (str): Lifecycle status; a terminal state completes the underlying
                future.
            exc_info (str | None): Optional worker-provided failure details.
        """
        if self._future.done():
            logger.warning("%s is already marked as done. Ignoring _set_exception call.", self.name)
            return
        if exc_info:
            info = f"{self.name} failed with status {status}. Exception info: {exc_info}"
        else:
            info = f"{self.name} failed with status {status}."
        self._future.set_exception(Exception(info))
