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
