"""Regressions for job submission, waiting, and subscription ownership."""

from __future__ import annotations

import subprocess
import threading
import time
from functools import partial
from unittest import mock

import fakeredis
import pytest

from slurm_manager.event_messages import HeartBeatMessage, StatusMessage
from slurm_manager.job_future import JobFuture
from slurm_manager.redis_sub_client import RedisSubClient
from slurm_manager.slurm_manager import SlurmManager, TopicInfo


@pytest.fixture
def manager():
    client = RedisSubClient(redis_cls=partial(fakeredis.FakeRedis, decode_responses=True))
    with mock.patch("slurm_manager.slurm_manager.RedisSubClient", return_value=client):
        manager = SlurmManager()

    def slurm(command):
        output = (
            "123\n" if command[0] == "sbatch" else "PENDING\n" if command[0] == "squeue" else ""
        )
        return subprocess.CompletedProcess(command, 0, output, "")

    manager._run_subprocess = mock.Mock(side_effect=slurm)
    try:
        yield manager
    finally:
        manager.shutdown(timeout=3)
        assert not manager._heartbeat_monitoring_thread.is_alive()
        listener = client._listener_thread
        assert listener is None or not listener.is_alive()


def submit(manager):
    # The workload and activation environment belong to the remote node. They
    # deliberately do not exist in the submitter's Python environment/filesystem.
    return manager.submit_job("/remote/node/workload.sh", "/remote/node/activate.sh")


def heartbeat_info(manager, future):
    return next(iter(manager._subscriptions_by_topic[TopicInfo(future._job_id).heartbeat].values()))


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
    commands = []

    def slurm(command):
        commands.append(command)
        if command[0] == "sbatch":
            assert "--hold" in command
            assert "/remote/node/activate.sh" in command
            assert "/remote/node/workload.sh" in command
            assert command[-1].endswith("redis_worker.sh")
            return subprocess.CompletedProcess(command, 0, "123\n", "")
        assert command == ["scontrol", "release", "123"]
        future = next(iter(manager._future_registry.values()))
        topics = TopicInfo(future._job_id)
        assert manager.sub_client._subscription_ready[topics.event].is_set()
        assert manager.sub_client._subscription_ready[topics.heartbeat].is_set()
        # Simulate a remote job completing before scontrol returns.
        manager.sub_client._redis.publish(topics.event, '{"msg_type":"status","status":"finished"}')
        future.wait(timeout=1)
        return subprocess.CompletedProcess(command, 0, "", "")

    manager._run_subprocess.side_effect = slurm
    future = submit(manager)
    assert future.success()
    assert [command[0] for command in commands] == ["sbatch", "scontrol"]
    assert not manager.sub_client.registered_topics


@pytest.mark.parametrize("failure", ["subscription", "acknowledgment", "release"])
def test_setup_failure_cancels_held_job_and_cleans_subscriptions(manager, failure):
    original = manager._run_subprocess.side_effect

    def slurm(command):
        if command[0] == "scontrol":
            return subprocess.CompletedProcess(command, 1, "", "release rejected")
        return original(command)

    manager._run_subprocess.side_effect = slurm
    if failure == "subscription":
        patch = mock.patch.object(
            manager, "_subscribe_to_event", side_effect=RuntimeError("offline")
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
    assert mock.call(["scancel", "123"]) in manager._run_subprocess.call_args_list
    assert not manager._future_registry
    assert not manager._active_subscriptions
    assert not manager.sub_client.registered_topics


@pytest.mark.parametrize("state", ["PENDING", "CONFIGURING", "RUNNING"])
def test_missing_heartbeat_does_not_fail_active_slurm_job(manager, state):
    future = submit(manager)
    info = heartbeat_info(manager, future)
    info.last_heartbeat = time.time() - 100
    manager._run_subprocess.return_value = subprocess.CompletedProcess([], 0, f"{state}\n", "")
    manager._run_subprocess.side_effect = None
    manager._check_slurm_job(info)
    assert not future.done()
    assert future._job_id in manager._future_registry
    assert TopicInfo(future._job_id).heartbeat in manager._active_subscriptions[future._job_id]
    assert manager._run_subprocess.call_args.args[0][0] == "squeue"


@pytest.mark.parametrize("accounting", ["", "PENDING|0:0\n"])
def test_unknown_or_delayed_accounting_is_not_failure(manager, accounting):
    future = submit(manager)
    manager._run_subprocess.side_effect = [
        subprocess.CompletedProcess([], 0, "", ""),
        subprocess.CompletedProcess([], 0, accounting, ""),
    ]
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert not future.done()


@pytest.mark.parametrize(
    "accounting,status,success",
    [
        ("COMPLETED|0:0\n", "finished", True),
        ("FAILED|7:0\n", "error", False),
        ("CANCELLED by 1000|0:15\n", "cancelled", False),
    ],
)
def test_terminal_accounting_resolves_missed_remote_status(manager, accounting, status, success):
    future = submit(manager)
    manager._run_subprocess.side_effect = [
        subprocess.CompletedProcess([], 0, "", ""),
        subprocess.CompletedProcess([], 0, accounting, ""),
    ]
    manager._check_slurm_job(heartbeat_info(manager, future))
    assert future.done()
    assert future.status == status
    assert future.success() == success
    assert not manager._future_registry
    assert not manager.sub_client.registered_topics


def test_scheduler_error_keeps_job_unresolved(manager):
    future = submit(manager)
    manager._run_subprocess.side_effect = FileNotFoundError("squeue unavailable")
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
    assert topic in manager._active_subscriptions[future._job_id]
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
    assert topic not in manager._active_subscriptions[future._job_id]
    assert topic not in manager.sub_client.registered_topics
    assert TopicInfo(future._job_id).heartbeat in manager.sub_client.registered_topics


def cluster_submission(manager):
    original = manager._run_subprocess.side_effect

    def slurm(command):
        if command[0] == "sbatch":
            return subprocess.CompletedProcess(command, 0, "123;cluster_a\n", "")
        return original(command)

    manager._run_subprocess.side_effect = slurm


def test_cluster_qualified_job_is_released_and_cancelled_on_its_cluster(manager):
    cluster_submission(manager)
    future = submit(manager)
    assert future.slurm_job_id == "123"
    assert future.slurm_cluster == "cluster_a"
    assert mock.call(["scontrol", "--clusters", "cluster_a", "release", "123"]) in (
        manager._run_subprocess.call_args_list
    )
    future.cancel()
    assert manager._run_subprocess.call_args == mock.call(
        ["scancel", "--clusters", "cluster_a", "123"]
    )


def test_cluster_qualified_setup_failure_cancels_on_its_cluster(manager):
    cluster_submission(manager)
    with (
        mock.patch.object(
            manager.sub_client, "wait_until_subscribed", side_effect=TimeoutError("no ack")
        ),
        pytest.raises(TimeoutError),
    ):
        submit(manager)
    assert manager._run_subprocess.call_args == mock.call(
        ["scancel", "--clusters", "cluster_a", "123"]
    )
    assert not manager._future_registry
    assert not manager.sub_client.registered_topics


def test_cluster_qualified_job_monitoring_queries_its_cluster(manager):
    cluster_submission(manager)
    future = submit(manager)
    manager._run_subprocess.reset_mock()
    manager._run_subprocess.side_effect = [
        subprocess.CompletedProcess([], 0, "", ""),
        subprocess.CompletedProcess([], 0, "COMPLETED|0:0\n", ""),
    ]
    manager._check_slurm_job(heartbeat_info(manager, future))
    commands = [call.args[0] for call in manager._run_subprocess.call_args_list]
    assert [command[0] for command in commands] == ["squeue", "sacct"]
    for command in commands:
        assert command[1:3] == ["--clusters", "cluster_a"]
        assert command[command.index("--jobs") + 1] == "123"
    assert future.success()


def test_completion_during_subscription_registration_leaves_no_orphan(manager):
    future = submit(manager)
    subscribed = threading.Event()
    resume = threading.Event()
    cleaned = threading.Event()
    errors = []
    original_subscribe = manager.sub_client.subscribe
    original_event = manager._event_callback

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
        mock.patch.object(manager, "_event_callback", side_effect=event),
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
    assert not manager._future_registry
    assert not manager._active_subscriptions
    assert not manager._topic_by_subscription_id
    assert not manager.sub_client.registered_topics
    assert not manager.sub_client._topic_by_subscription_id


def test_listen_after_completion_does_not_register_callback(manager):
    future = submit(manager)
    manager._event_callback(StatusMessage(status="finished"), future)
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
    assert not manager._future_registry
    assert not manager.sub_client.registered_topics
