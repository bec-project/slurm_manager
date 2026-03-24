"""
Module defining a JobFuture class that represents a Slurm job and allows users to check
its status, register callbacks for events and wait for its completion.

The JobFuture class provides a high-level interface for interacting with Slurm jobs.
"""

import logging
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable
from uuid import uuid4

from slurm_manager.utils.utils import SLURM_JOB_STATE_CODES

logger = logging.getLogger(__name__)


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class RegisteredCallback:
    id: str
    event: str
    callback: Callable[..., None]


class JobFuture:
    """
    A future-like object that represents a Slurm job. It allows users to check the status of the job,
    retrieve the job's result, and register callbacks for events (e.g., 'status', 'stdout', 'stderr').
    """

    def __init__(self, job_id: str, manager, registry: dict | None = None, name: str | None = None):
        self._job_id = job_id
        self._manager = manager
        self._registry = registry if registry is not None else {}
        self._pubsub_client = manager.pubsub_client
        self._future = Future()
        self.name = name if name else f"Job-{job_id}"

        self._status: str = JobStatus.PENDING.value
        self._slurm_state: str | None = None

        self._shutdown_event = threading.Event()
        self._callback_registry: dict[str, list[RegisteredCallback]] = {"status": []}
        self._subscribe_to_event("status", self._update_status)

    #####################
    ## Public API
    #####################

    @property
    def status(self) -> str | None:
        return self._status

    # TBD - do we want to expose the raw slurm state in addition?
    # Maybe this can be more hidden.
    @property
    def slurm_state(self) -> str | None:
        return self._slurm_state

    def cancel(self):
        self._manager.cancel_job(self.job_id)

    def exception(self) -> Exception | None:
        if self._future.done():
            return self._future.exception()
        return None

    def success(self) -> bool:
        return self._future.done() and self._future.exception() is None

    def done(self) -> bool:
        return self._future.done()

    def set_finished(self):
        self._shutdown_event.set()
        if self._future.done():
            # TBD - have we decided
            print(
                f"Warning: Job {self.name} is already marked as done. Ignoring set_finished call."
            )
            logger.warning(
                f"Job {self.name} is already marked as done. Ignoring set_finished call."
            )
            return
        self._future.set_result(None)

    def set_exception(self, status, exc_info=None):
        if self._future.done():
            print(
                f"Warning: Job {self.name} is already marked as done. Ignoring set_exception call."
            )
            logger.warning(
                f"Job {self.name} is already marked as done. Ignoring set_exception call."
            )
            return
        self._shutdown_event.set()
        # Here you could log the exception info or store it for later retrieval
        if exc_info:
            info = f"Job {self.name} failed with status {status}. Exception info: {exc_info}"
        else:
            info = f"Job {self.name} failed with status {status}."
        # You could log the info here, e.g., using logging.error(info)
        self._future.set_exception(Exception(info))

    def wait(self, timeout: float | None = None):
        if self.done():
            self._raise_if_failed()
            return self

        start_time = time.time()
        poll_interval = 0.05  # seconds
        while not self.done():
            if timeout is not None and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Job {self.job_id} did not complete within {timeout} seconds.")
            if self._shutdown_event.wait(timeout=poll_interval):
                break

    def add_callback(self, event: str, callback: Callable[..., None]) -> str:
        cb = RegisteredCallback(id=str(uuid4()), event=event, callback=callback)
        if event not in self._callback_registry:
            self._callback_registry[event] = []
            self._subscribe_to_event(event)
        self._callback_registry[event].append(cb)
        return cb.id

    def remove_callback(self, callback_id: str):
        for event, callbacks in self._callback_registry.items():
            for cb in callbacks:
                if cb.id == callback_id:
                    callbacks.remove(cb)
                    return

    ###################
    ## Internal Methods
    ###################

    def _update_status(self, status: str, exc_info=None):
        self._status = JobStatus(status).value
        if status == JobStatus.COMPLETED:
            self.set_finished()
        elif status in (JobStatus.FAILED, JobStatus.CANCELLED):
            # Consider using exception info here to provide more context about the failure
            self.set_exception(status, exc_info)

        for cb in self._callback_registry["status"]:
            cb.callback(status)

    def _update_slurm_state(self, slurm_state: str):
        self._slurm_state = SLURM_JOB_STATE_CODES.get(slurm_state, slurm_state)
        # Depending on which event, we may like to trigger in addition here that the status changes
        # at least for events when the SBatch job is terminated, and we don't expect any more updates. TBD.

    def _subscribe_to_event(self, event: str):
        # Subscribe to the relevant Redis channel for the event
        channel = f"job/{self.job_id}/{event}"
        self._pubsub_client.subscribe(channel, self._handle_event)
