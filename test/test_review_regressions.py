"""Regressions for job submission, waiting, and subscription ownership."""

from __future__ import annotations

import threading
import time
from functools import partial
from pathlib import Path
from unittest import mock

import fakeredis
import pytest

from slurm_manager.event_messages import HeartBeatMessage, StatusMessage
from slurm_manager.job_future import JobFuture
from slurm_manager.job_options import JobOptions
from slurm_manager.job_tracker import TopicInfo
from slurm_manager.redis_sub_client import RedisSubClient
from slurm_manager.slurm_manager import SlurmManager
from slurm_manager.slurm_rest_client import SlurmRestClient, SlurmRestError


@pytest.fixture
def manager():
    client = RedisSubClient(redis_cls=partial(fakeredis.FakeRedis, decode_responses=True))
    rest = mock.Mock(spec=SlurmRestClient)
    rest.submit.return_value = {"job_id": 123}
    rest.get_job.return_value = {"job_state": ["PENDING"]}
    with mock.patch("slurm_manager.slurm_manager.RedisSubClient", return_value=client):
        manager = SlurmManager(rest_client=rest)

    try:
        yield manager
    finally:
        manager.shutdown(timeout=3)
        assert not manager._heartbeat_monitoring_thread.is_alive()
        listener = client._listener_thread
        assert listener is None or not listener.is_alive()


def submit(manager):
    return manager.submit_job(
        Path(__file__).parent.parent / "examples/print_for_10s.sh",
        working_directory="/remote/node/work",
    )


def heartbeat_info(manager, future):
    return next(
        iter(
            manager._job_tracker._subscriptions_by_topic[
                TopicInfo(future._job_id).heartbeat
            ].values()
        )
    )


def test_wait_propagates_failure_arriving_during_wait():
    future = JobFuture("job", manager=None, slurm_job_id="123")
    timer = threading.Timer(0.05, lambda: future._update_status("error", "exit code 7"))
    timer.start()
    try:
        with pytest.raises(Exception, match="exit code 7"):
            future.wait(timeout=1)
    finally:
        timer.join()


def test_wait_timeout_does_not_complete_future():
    future = JobFuture("job", manager=None, slurm_job_id="123")
    with pytest.raises(TimeoutError):
        future.wait(timeout=0)
    assert not future.done()
    future._update_status("finished")
    future.wait(timeout=0)
    assert future.success()


def test_terminal_state_ignores_late_status_updates():
    future = JobFuture("job", manager=None, slurm_job_id="123")
    future._update_status("finished")
    future._update_status("running")
    assert future.status == "finished"
    assert future.success()


def test_job_is_held_until_redis_acknowledges_subscriptions(manager):
    def release(job_id):
        assert job_id == "123"
        script, job = manager.rest_client.submit.call_args.args
        assert job["hold"] is True
        assert job["current_working_directory"] == "/remote/node/work"
        assert '"activation": ""' in script
        assert "stdout-test %02d/10" in script
        assert "/usr/bin/bec-slurm-worker" in script
        future = next(iter(manager._job_tracker._future_registry.values()))
        topics = TopicInfo(future._job_id)
        assert manager.sub_client._subscription_ready[topics.event].is_set()
        assert manager.sub_client._subscription_ready[topics.heartbeat].is_set()
        manager.sub_client._redis.publish(topics.event, '{"msg_type":"status","status":"finished"}')
        future.wait(timeout=1)

    manager.rest_client.release.side_effect = release
    future = submit(manager)
    assert future.success()
    assert not manager.sub_client.registered_topics


@pytest.mark.parametrize("failure", ["subscription", "acknowledgment", "release"])
def test_setup_failure_cancels_held_job_and_cleans_subscriptions(manager, failure):
    if failure == "release":
        manager.rest_client.release.side_effect = SlurmRestError("release rejected")
    if failure == "subscription":
        patch = mock.patch.object(
            manager._job_tracker, "_subscribe_to_event", side_effect=RuntimeError("offline")
        )
    elif failure == "acknowledgment":
        patch = mock.patch.object(
            manager.sub_client, "wait_until_subscribed", side_effect=TimeoutError("no ack")
        )
    else:
        patch = mock.patch.object(
            manager.sub_client,
            "wait_until_subscribed",
            wraps=manager.sub_client.wait_until_subscribed,
        )
    with patch, pytest.raises((RuntimeError, TimeoutError)):
        submit(manager)
    manager.rest_client.cancel.assert_called_once_with("123")
    assert not manager._job_tracker._future_registry
    assert not manager._job_tracker._active_subscriptions
    assert not manager.sub_client.registered_topics


@pytest.mark.parametrize(
    "last_heartbeat,last_check,expected",
    [
        (None, None, False),
        (0, None, False),
        (95, None, False),
        (90, None, False),
        (89, None, True),
        (89, 95, False),
        (89, 90, False),
        (89, 89, True),
    ],
)
def test_heartbeat_selection_respects_timeout_and_scheduler_throttle(
    manager, last_heartbeat, last_check, expected
):
    manager._stop_heartbeat_thread.set()
    manager._heartbeat_monitoring_thread.join(timeout=1)
    future = submit(manager)
    future.listen("heartbeat", lambda message: None)
    future.listen("log", lambda message: None)
    info = heartbeat_info(manager, future)
    info.last_heartbeat = last_heartbeat
    info.last_scheduler_check = last_check

    with mock.patch("slurm_manager.job_tracker.time.time", return_value=100):
        missing = manager._job_tracker.get_missing_heartbeats()

    assert missing == ([info] if expected else [])
    manager.rest_client.get_job.assert_not_called()


@pytest.mark.parametrize("state", ["PENDING", "CONFIGURING", "RUNNING"])
def test_missing_heartbeat_does_not_fail_active_slurm_job(manager, state):
    future = submit(manager)
    info = heartbeat_info(manager, future)
    info.last_heartbeat = time.time() - 100
    manager.rest_client.get_job.return_value = {"job_state": [state]}
    manager._check_slurm_job(info)
    assert not future.done()
    assert future._job_id in manager._job_tracker._future_registry
    assert (
        TopicInfo(future._job_id).heartbeat
        in manager._job_tracker._active_subscriptions[future._job_id]
    )
    manager.rest_client.get_job.assert_called_once_with("123")


@pytest.mark.parametrize("accounting", [None, {"state": {"current": ["PENDING"]}}])
def test_unknown_or_delayed_accounting_is_not_failure(manager, accounting):
    future = submit(manager)
    manager.rest_client.get_job.side_effect = [None, accounting]
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert not future.done()


@pytest.mark.parametrize(
    "accounting,status,success",
    [
        ({"job_state": ["COMPLETED"], "exit_code": 0}, "finished", True),
        ({"job_state": ["FAILED"], "exit_code": 7}, "error", False),
        ({"job_state": ["CANCELLED"], "exit_code": 15}, "cancelled", False),
    ],
)
def test_terminal_accounting_resolves_missed_remote_status(manager, accounting, status, success):
    future = submit(manager)
    manager.rest_client.get_job.side_effect = [None, accounting]
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert future.done()
    assert future.status == status
    assert future.success() == success
    assert not manager._job_tracker._future_registry
    assert not manager.sub_client.registered_topics


def test_scheduler_error_keeps_job_unresolved(manager):
    future = submit(manager)
    manager.rest_client.get_job.side_effect = SlurmRestError("unavailable")
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert not future.done()


def test_unlisten_removes_only_selected_callback_and_preserves_monitoring(manager):
    future = submit(manager)
    removed = threading.Event()
    received = threading.Event()
    callback_id = future.listen("heartbeat", lambda _: removed.set())
    future.listen("heartbeat", lambda _: received.set())
    future.unlisten(callback_id)
    topic = TopicInfo(future._job_id).heartbeat
    assert callback_id not in manager.sub_client._topic_by_subscription_id
    assert topic in manager._job_tracker._active_subscriptions[future._job_id]
    manager.sub_client._redis.publish(
        topic, HeartBeatMessage(timestamp=time.time()).model_dump_json()
    )
    assert received.wait(1)
    assert not removed.is_set()
    assert heartbeat_info(manager, future).heartbeat_received
    manager.remove_subscriptions_for_job(future._job_id)
    assert not manager.sub_client.registered_topics


def test_removing_last_log_callback_unsubscribes_topic(manager):
    future = submit(manager)
    callback_id = future.listen("log", lambda _: None)
    future.unlisten(callback_id)
    topic = TopicInfo(future._job_id).log
    assert topic not in manager._job_tracker._active_subscriptions[future._job_id]
    assert topic not in manager.sub_client.registered_topics
    assert TopicInfo(future._job_id).heartbeat in manager.sub_client.registered_topics


@pytest.mark.parametrize("code,success", [(0, True), (7, False)])
def test_accounting_structured_exit_code(manager, code, success):
    future = submit(manager)
    manager.rest_client.get_job.side_effect = [
        None,
        {
            "state": {"current": ["COMPLETED"]},
            "exit_code": {
                "return_code": {"set": True, "number": code},
                "signal": {"id": {"set": False, "number": 0}},
            },
        },
    ]
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert future.done()
    assert future.success() == success
    assert manager.rest_client.get_job.call_args == mock.call("123", accounting=True)


def test_cancel_uses_rest_and_cleans_subscriptions(manager):
    future = submit(manager)
    future.cancel()
    manager.rest_client.cancel.assert_called_once_with("123")
    assert future.status == "cancelled"
    assert not manager.sub_client.registered_topics


def test_completion_during_subscription_registration_leaves_no_orphan(manager):
    future = submit(manager)
    subscribed = threading.Event()
    resume = threading.Event()
    cleaned = threading.Event()
    errors = []
    original_subscribe = manager.sub_client.subscribe
    original_event = manager._job_tracker.apply_status

    def subscribe(topic, callback):
        callback_id = original_subscribe(topic, callback)
        subscribed.set()
        assert resume.wait(2)
        return callback_id

    def event(message, future):
        original_event(message, future)
        cleaned.set()

    def listen():
        try:
            future.listen("log", lambda _: None)
        except RuntimeError as exc:
            errors.append(exc)

    with (
        mock.patch.object(manager.sub_client, "subscribe", side_effect=subscribe),
        mock.patch.object(manager._job_tracker, "apply_status", side_effect=event),
    ):
        thread = threading.Thread(target=listen)
        thread.start()
        try:
            assert subscribed.wait(1)
            manager.sub_client._redis.publish(
                TopicInfo(future._job_id).event, '{"msg_type":"status","status":"finished"}'
            )
            future.wait(timeout=1)
        finally:
            resume.set()
            thread.join(timeout=2)
        assert not thread.is_alive()
        assert cleaned.wait(1)
    assert len(errors) == 1
    assert not manager._job_tracker._future_registry
    assert not manager._job_tracker._active_subscriptions
    assert not manager._job_tracker._topic_by_subscription_id
    assert not manager.sub_client.registered_topics
    assert not manager.sub_client._topic_by_subscription_id


def test_listen_after_completion_does_not_register_callback(manager):
    future = submit(manager)
    manager._job_tracker.apply_status(StatusMessage(status="finished"), future)
    with pytest.raises(RuntimeError, match="no longer active"):
        future.listen("log", lambda _: None)
    assert not manager.sub_client.registered_topics


def test_unlisten_allows_concurrent_job_cleanup(manager):
    future = submit(manager)
    callback_id = future.listen("log", lambda _: None)
    cleaned = threading.Event()
    errors = []
    completed_during_removal = []
    original_remove = manager.remove_subscription_by_id

    def cleanup():
        try:
            manager.remove_subscriptions_for_job(future._job_id)
        except Exception as exc:
            errors.append(exc)
        finally:
            cleaned.set()

    thread = threading.Thread(target=cleanup)

    def remove(callback_id):
        # Cleanup takes the manager lock and then updates the future. It must
        # finish even while unlisten is paused at the manager call boundary.
        thread.start()
        completed = cleaned.wait(1)
        completed_during_removal.append(completed)
        if completed:
            original_remove(callback_id)
        # On regression, return without taking the manager lock so unlisten
        # releases its lock and the test can join cleanup instead of hanging.

    with mock.patch.object(manager, "remove_subscription_by_id", side_effect=remove):
        try:
            future.unlisten(callback_id)
        finally:
            thread.join(timeout=2)
    assert not thread.is_alive()
    assert not errors
    assert completed_during_removal == [True]
    assert future.done()
    assert not future._callback_registry
    assert not manager._job_tracker._future_registry
    assert not manager.sub_client.registered_topics


@pytest.mark.parametrize("code", [0, 7])
def test_controller_structured_exit_code(manager, code):
    future = submit(manager)
    manager.rest_client.get_job.return_value = {
        "job_state": ["COMPLETED"],
        "exit_code": {
            "status": ["SUCCESS" if code == 0 else "ERROR"],
            "return_code": {"set": True, "number": code},
            "signal": {"id": {"set": False, "number": 0}},
        },
    }
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert future.done()
    assert future.success() == (code == 0)


def test_local_files_are_uploaded_without_mutating_options(manager, tmp_path):
    workload = tmp_path / "work load $(touch should_not_exist).sh"
    activation = tmp_path / "it's my activation.sh"
    workload.write_text("#!/bin/bash\nprintf 'hello'\n")
    activation.write_text("module load redis\n")
    options = JobOptions(partition="hour", tasks=2, time_limit_minutes=5)
    manager.submit_job(workload, activation, working_directory="/remote/work", job_options=options)
    script, job = manager.rest_client.submit.call_args.args
    assert "hello" in script
    assert "module load redis" in script
    assert str(tmp_path) not in script
    assert job["hold"] is True
    assert options == JobOptions(partition="hour", tasks=2, time_limit_minutes=5)
    assert job["tasks"] == 2
    assert job["time_limit"] == {"set": True, "number": 5}


def test_submission_failure_does_not_register_job(manager):
    manager.rest_client.submit.side_effect = SlurmRestError("submission failed")
    with pytest.raises(SlurmRestError, match="submission failed"):
        submit(manager)
    assert not manager._job_tracker._future_registry
    assert not manager.sub_client.registered_topics
    manager.rest_client.release.assert_not_called()
    manager.rest_client.cancel.assert_not_called()


def test_cancellation_failure_keeps_monitoring(manager):
    future = submit(manager)
    manager.rest_client.cancel.side_effect = SlurmRestError("cancellation failed")
    with pytest.raises(SlurmRestError, match="cancellation failed"):
        future.cancel()
    assert not future.done()
    assert future._job_id in manager._job_tracker._future_registry


def test_relative_remote_paths_are_rejected_before_submission(manager):
    with pytest.raises(ValueError, match="absolute"):
        manager.submit_job("workload.sh", "activate.sh", working_directory="relative")
    manager.rest_client.submit.assert_not_called()


def test_public_callbacks_are_ready_before_release(manager):
    received = threading.Event()

    def release(_job_id):
        future = next(iter(manager._job_tracker._future_registry.values()))
        topic = TopicInfo(future._job_id).log
        assert manager.sub_client._subscription_ready[topic].is_set()
        manager.sub_client._redis.publish(topic, '{"msg_type":"log","log":"first line"}')
        assert received.wait(1)

    manager.rest_client.release.side_effect = release
    manager.submit_job(
        "examples/print_for_10s.sh",
        working_directory="/remote/work",
        callbacks={"log": lambda _: received.set()},
    )


def test_unknown_status_cleans_up_the_terminal_future(manager):
    future = submit(manager)
    future.listen("log", lambda _: None)
    manager._job_tracker.apply_status(StatusMessage(status="unexpected-status"), future)
    assert future.done()
    assert future.status == "error"
    assert not manager._job_tracker._future_registry
    assert not manager._job_tracker._active_subscriptions
    assert not manager._job_tracker._subscriptions_by_topic
    assert not manager._job_tracker._topic_by_subscription_id
    assert not manager.sub_client.registered_topics


def test_shutdown_drains_all_received_log_callbacks(manager):
    future = submit(manager)
    release = threading.Event()
    received = []
    blocking = [manager._job_tracker._executor.submit(release.wait) for _ in range(8)]
    future.listen("log", lambda message: received.append(message.log))
    topic = TopicInfo(future._job_id)
    for number in range(25):
        manager.sub_client._redis.publish(topic.log, f'{{"msg_type":"log","log":"{number}"}}')
    manager.sub_client._redis.publish(topic.event, '{"msg_type":"status","status":"finished"}')
    future.wait(timeout=2)
    # Release blocked workers only once shutdown reaches the executor. With the
    # old cancel_futures=True, all 25 accepted callbacks would be discarded here.
    original = manager._job_tracker._executor.shutdown

    def shutdown(*, wait=True, cancel_futures=False):
        original(wait=False, cancel_futures=cancel_futures)
        release.set()
        if wait:
            original(wait=True)

    try:
        with mock.patch.object(manager._job_tracker._executor, "shutdown", side_effect=shutdown):
            manager.shutdown(timeout=3)
    finally:
        release.set()
        for task in blocking:
            task.result(timeout=1)
    assert sorted(received, key=int) == [str(number) for number in range(25)]
