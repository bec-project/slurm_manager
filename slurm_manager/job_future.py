"""
Module defining a JobFuture class that represents a Slurm job and allows users to check
its status, register callbacks for events and wait for its completion.

The JobFuture class provides a high-level interface for interacting with Slurm jobs.
"""

from __future__ import annotations

import logging
import threading
import time
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
    retrieve the job's result, and register callbacks for events (e.g., 'event', 'logs', 'heartbeat').
    """

    def __init__(
        self,
        job_id: str,
        manager: SlurmManager,
        slurm_job_id: str,
        registry: dict | None = None,
        name: str | None = None,
    ):
        self._job_id = job_id
        self._manager = manager
        self._slurm_job_id = slurm_job_id
        self._registry = registry if registry is not None else {}
        self._future = Future()
        if name is None:
            name = "Job"
        self.name = f"{name}-{slurm_job_id}"
        self._status: str = JobStatus.PENDING.value
        self._shutdown_event = threading.Event()
        self._lock = threading.RLock()
        self._callback_registry: dict[str, list[RegisteredCallback]] = {}

    #####################
    ## Public API
    #####################

    @property
    def slurm_job_id(self) -> str:
        """The SLURM ID of the job, as returned by sbatch."""
        return self._slurm_job_id

    @property
    def status(self) -> str | None:
        """The current status of the job."""
        return self._status

    def cancel(self):
        """Cancel the job."""
        self._manager.cancel_job(slurm_job_id=self.slurm_job_id, job_id=self._job_id)

    def exception(self) -> Exception | None:
        """If the job has finished with an error, return the exception. Otherwise, return None."""
        if self._future.done():
            return self._future.exception()
        return None

    def success(self) -> bool:
        """Return True if the job finished successfully, False if it finished with an error or is not done yet."""
        return self._future.done() and self._future.exception() is None

    def done(self) -> bool:
        """Return True if the job has finished (either successfully or with an error), False otherwise."""
        return self._future.done()

    def wait(self, timeout: float | None = None) -> None:
        """
        Wait for the job to finish, with an optional timeout in seconds. Raises TimeoutError if the job
        does not finish within the specified timeout.

        Args:
            timeout: Maximum time to wait for the job to finish, in seconds. If None, wait indefinitely.
        """
        self._raise_if_failed()

        start_time = time.time()
        poll_interval = 0.05  # seconds
        while not self.done():
            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError(f"{self.name} did not complete within {timeout} seconds.")
            if self._shutdown_event.wait(timeout=poll_interval):
                break

    def listen(
        self,
        event_type: Literal["status", "log", "heartbeat"],
        callback: Callable[[SlurmMessage], None],
    ) -> str:
        """
        Register a callback to listen for events of the specified type ('status', 'log', or 'heartbeat').
        Returns a callback ID that can be used to unregister.

        Args:
            event_type: The type of event to listen for ('status', 'log', or 'heartbeat').
            callback: A function that takes a SlurmMessage object as input. The exact message object
                      depends on the event type. Please check event_messages.py for the specific
                      message class based on the event.

        Returns:
            A unique callback ID that can be used to unregister the callback later.
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
        """
        Unregister a previously registered callback using its callback ID.

        Args:
            callback_id: The unique ID of the callback to unregister, as returned by listen()
        """
        with self._lock:
            for event, callbacks in self._callback_registry.items():
                for cb in callbacks:
                    if cb.id == callback_id:
                        callbacks.remove(cb)
                        self._manager.remove_subscription_by_id(callback_id=callback_id)
                        if not callbacks:  # if no more callbacks for event, remove the event
                            self._callback_registry.pop(event, None)
                        return

    ###################
    ## Internal Methods
    ## Not meant to be called by users
    ###################

    def _set_finished(self):
        self._shutdown_event.set()
        if self._future.done():
            logger.warning(
                "Job %s is already marked as done. Ignoring _set_finished call.", self.name
            )
            return
        self._future.set_result(None)

    def _set_exception(self, status, exc_info=None):
        if self._future.done():
            logger.warning("%s is already marked as done. Ignoring _set_exception call.", self.name)
            return
        self._shutdown_event.set()
        # Here you could log the exception info or store it for later retrieval
        if exc_info:
            info = f"{self.name} failed with status {status}. Exception info: {exc_info}"
        else:
            info = f"{self.name} failed with status {status}."
        # You could log the info here, e.g., using logging.error(info)
        self._future.set_exception(Exception(info))

    def _raise_if_failed(self) -> None:
        if self._future.done() and self._future.exception() is not None:
            raise self._future.exception()

    def _update_status(self, status: str, exc_info: str | None = None) -> None:
        self._status = status
        if status == JobStatus.FINISHED:
            self._set_finished()
        elif status in (JobStatus.CANCELLED, JobStatus.ERROR):
            self._set_exception(status, exc_info)
