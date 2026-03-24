from __future__ import annotations

import threading
import time
from functools import partial

import fakeredis

from slurm_manager.redis_pubsub_client import Message
from slurm_manager.redis_pubsub_client import RedisPubSubClient


def _make_client() -> RedisPubSubClient:
    server = fakeredis.FakeServer()
    redis_cls = partial(fakeredis.FakeRedis, server=server, decode_responses=True)
    return RedisPubSubClient(bootstrap="localhost:6379", redis_cls=redis_cls)


def test_subscribe_dispatch_unsubscribe_cycle():
    client = _make_client()
    received: list[Message] = []
    received_event = threading.Event()

    def callback(message: Message):
        received.append(message)
        received_event.set()

    sub_id = client.subscribe("job/1/status", callback)
    assert "job/1/status" in client.registered_topics

    client._redis.publish("job/1/status", '{"status": "RUNNING"}')
    assert received_event.wait(timeout=1.0)
    assert received[0].topic == "job/1/status"
    assert received[0].payload == {"status": "RUNNING"}

    removed = client.unsubscribe(sub_id)
    assert removed is True
    assert "job/1/status" not in client.registered_topics
    client.shutdown()


def test_unsubscribe_topic_removes_all_callbacks():
    client = _make_client()
    got: list[str] = []

    client.subscribe("a", lambda msg: got.append("cb1"))
    client.subscribe("a", lambda msg: got.append("cb2"))

    assert client.unsubscribe_topic("a") == 2
    client._redis.publish("a", "x")
    time.sleep(0.05)
    assert got == []
    client.shutdown()
