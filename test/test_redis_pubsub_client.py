from __future__ import annotations

import threading
import time
from functools import partial
from unittest import mock

import fakeredis
import pytest

from slurm_manager.event_messages import HeartBeatMessage, LogMessage, SlurmMessage, StatusMessage
from slurm_manager.redis_sub_client import RedisSubClient


@pytest.fixture
def client():
    """FakeRedis-based RedisServer fixture for testing the RedisSubClient."""
    server = fakeredis.FakeServer()
    redis_cls = partial(fakeredis.FakeRedis, server=server, decode_responses=True)
    return RedisSubClient(bootstrap="localhost:6379", redis_cls=redis_cls)


def test_redis_pubsub_listener(client):
    """Test listening for messages and executing callbacks."""
    received: list[SlurmMessage] = []
    received_event = threading.Event()

    def callback(message: SlurmMessage):
        received.append(message)
        received_event.set()

    sub_id = client.subscribe("job/1/status", callback)
    assert "job/1/status" in client.registered_topics

    client._redis.publish("job/1/status", '{"msg_type":"status", "status": "running"}')
    assert received_event.wait(timeout=1.0)
    assert isinstance(received[0], StatusMessage)
    assert received[0].status == "running"

    removed = client.unsubscribe(sub_id)
    assert removed is True
    assert "job/1/status" not in client.registered_topics
    client.shutdown()


def test_redis_pubsub_unsubscribe_topic(client):
    got: list[str] = []

    client.subscribe("a", lambda msg: got.append("cb1"))
    client.subscribe("a", lambda msg: got.append("cb2"))

    assert client.unsubscribe_topic("a") == 2
    client._redis.publish("a", "x")
    time.sleep(0.05)
    assert not got, "Callbacks should not be called after unsubscribing from topic"
    client.shutdown()


def test_redis_pubsub_shutdown(client):
    """Test that the listener thread is properly shut down."""
    with (
        mock.patch.object(client._pubsub, "close") as mock_close,
        mock.patch.object(client._redis, "close") as mock_redis_close,
    ):
        client._ensure_threads_started()
        client.shutdown()
        assert (
            not client._listener_thread.is_alive()
        ), "Listener thread should be stopped after shutdown"
        assert mock_close.called, "PubSub close method should be called on shutdown"
        assert mock_redis_close.called, "Redis close method should be called on shutdown"


# "type": message_type,
#                 "pattern": response[1],
#                 "channel": response[2],
#                 "data": response[3],
@pytest.mark.parametrize(
    "payload, expected",
    [
        (
            {
                "type": "Message",
                "pattern": "",
                "channel": "job/1",
                "data": '{"msg_type": "status", "status": "running"}',
            },
            StatusMessage(msg_type="status", status="running"),
        ),
        (
            {
                "type": "Message",
                "pattern": "",
                "channel": "job/1",
                "data": '{"msg_type": "heartbeat", "timestamp": 1234567890.0}',
            },
            HeartBeatMessage(msg_type="heartbeat", timestamp=1234567890.0),
        ),
        (
            {
                "type": "Message",
                "pattern": "",
                "channel": "job/1",
                "data": '{"msg_type": "log", "log": "This is a log message."}',
            },
            LogMessage(msg_type="log", log="This is a log message."),
        ),
    ],
)
def test_redis_pubsub_normalize_message(client, payload, expected):
    topic, message = client._normalize_message(payload)
    assert topic == "job/1"
    assert message == expected
