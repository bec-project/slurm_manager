"""Exercise tracking independently of REST submission and scheduler monitoring."""

from functools import partial
from unittest import mock

import fakeredis
import pytest

from slurm_manager.event_messages import StatusMessage
from slurm_manager.job_future import JobFuture
from slurm_manager.job_tracker import JobTracker, TopicInfo
from slurm_manager.redis_sub_client import RedisSubClient


@pytest.fixture
def tracker():
    client = RedisSubClient(redis_cls=partial(fakeredis.FakeRedis, decode_responses=True))
    tracker = JobTracker(client)
    try:
        yield tracker
    finally:
        tracker.shutdown(timeout=3)


def test_heartbeat_snapshots_cannot_mutate_tracking_state(tracker):
    future = JobFuture("first", manager=None, slurm_job_id="123")
    with mock.patch("slurm_manager.job_tracker.time.time", return_value=80):
        tracker.track(future)

    with mock.patch("slurm_manager.job_tracker.time.time", return_value=100):
        snapshot = tracker.get_missing_heartbeats()[0]
        snapshot.last_heartbeat = 0
        assert tracker.get_missing_heartbeats()[0].last_heartbeat == 80
        assert tracker.begin_scheduler_check("first") is future
        assert tracker.get_missing_heartbeats() == []


def test_completion_preserves_other_jobs_and_their_redis_subscriptions(tracker):
    first = JobFuture("first", manager=None, slurm_job_id="123")
    second = JobFuture("second", manager=None, slurm_job_id="124")
    tracker.track(first)
    tracker.track(second)
    tracker.wait_until_ready("second")

    tracker.apply_status(StatusMessage(status="finished"), first)

    assert first.success()
    assert tracker.get_future("first") is None
    assert tracker.get_future("second") is second
    assert not second.done()
    assert tracker._sub_client.registered_topics == {
        TopicInfo("second").heartbeat,
        TopicInfo("second").event,
        TopicInfo("second").log,
    }
    tracker._sub_client._redis.publish(
        TopicInfo("second").event, '{"msg_type":"status","status":"finished"}'
    )
    second.wait(timeout=1)
    assert second.success()


def test_scheduler_check_skips_a_job_removed_after_snapshot(tracker):
    future = JobFuture("first", manager=None, slurm_job_id="123")
    tracker.track(future)
    tracker.apply_status(StatusMessage(status="finished"), future)

    assert tracker.begin_scheduler_check("first") is None
    assert tracker.begin_scheduler_check("unknown") is None


@pytest.mark.parametrize("status", ["finished", "cancelled", "error:workload failed", "unknown"])
def test_synthesized_terminal_status_notifies_listeners_once(tracker, status):
    future = JobFuture("first", manager=None, slurm_job_id="123")
    tracker.track(future)
    received = []
    tracker.add_subscription("first", "status", received.append)
    topic = TopicInfo("first").event
    queued_redis_callbacks = list(tracker._sub_client._subscriptions_by_topic[topic].values())
    message = StatusMessage(status=status)

    tracker.apply_status(message, future)
    # Simulate a Redis message whose callbacks were captured before local cleanup.
    for subscription in queued_redis_callbacks:
        subscription.callback(message)
    tracker.shutdown(timeout=3)

    assert len(received) == 1
    assert received[0].metadata["job_id"] == "first"
    assert received[0].metadata["slurm_job_id"] == "123"
    assert received[0].status == status or received[0].status.startswith("error:")
    assert future.done()


def test_redis_status_is_dispatched_once_and_shutdown_drains_it(tracker):
    future = JobFuture("first", manager=None, slurm_job_id="123")
    tracker.track(future)
    received = []
    tracker.add_subscription("first", "status", received.append)
    tracker.wait_until_ready("first")
    tracker._sub_client._redis.publish(
        TopicInfo("first").event, '{"msg_type":"status","status":"finished"}'
    )
    future.wait(timeout=1)
    tracker.shutdown(timeout=3)

    assert [message.status for message in received] == ["finished"]


def test_removing_tracking_reports_failure_to_status_listeners(tracker):
    future = JobFuture("first", manager=None, slurm_job_id="123")
    tracker.track(future)
    received = []
    tracker.add_subscription("first", "status", received.append)

    tracker.remove_subscriptions_for_job("first")
    tracker.shutdown(timeout=3)

    assert len(received) == 1
    assert received[0].status.startswith("error:Removal requested by user")
    assert tracker.get_future("first") is None
    assert tracker._sub_client.registered_topics == set()


def test_callback_errors_are_logged_without_stopping_other_callbacks(tracker, caplog):
    import threading

    future = JobFuture("first", manager=None, slurm_job_id="123")
    tracker.track(future)
    received = threading.Event()

    def fail(message):
        raise RuntimeError("callback failed")

    tracker.add_subscription("first", "status", fail)
    tracker.add_subscription("first", "status", lambda message: received.set())
    tracker.apply_status(StatusMessage(status="finished"), future)
    tracker.shutdown(timeout=3)

    assert received.is_set()
    assert "Error in status callback for job first" in caplog.text
    assert "RuntimeError: callback failed" in caplog.text
