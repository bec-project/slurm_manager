"""
Thin Redis Client to register callbacks to a pub/sub interface.
The client is not intended to publish messages itself, but only
subscribe to topics and dispatch received messages to registered callbacks.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4
from redis.client import Redis

logger = logging.getLogger(__name__)

MessageCallback = Callable[[dict[str, Any]], None]
_STOP = object()


@dataclass(frozen=True, slots=True)
class Subscription:
    """Represents one registered callback for one topic."""

    id: str
    topic: str
    callback: MessageCallback


@dataclass
class Message:
    topic: str
    payload: Any
    raw: dict[str, Any]


class RedisPubSubClient:
    """Simple pub/sub wrapper around redis-py.

    The client maintains a single pub/sub connection and two background threads:
    one listener (I/O polling) and one dispatcher (callback execution).
    """

    def __init__(
        self,
        bootstrap: str | list[str] = "localhost:6379",
        redis_cls: type[Redis] = Redis,
        **redis_kwargs: Any,
    ) -> None:
        if isinstance(bootstrap, list):
            if not bootstrap:
                raise ValueError("bootstrap list must not be empty")
            host, port = bootstrap[0].split(":", maxsplit=1)
        else:
            host, port = bootstrap.split(":", maxsplit=1)

        self._redis = redis_cls(host=host, port=int(port), **redis_kwargs)
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)

        self._lock = threading.RLock()
        self._subscriptions_by_topic: dict[str, dict[str, Subscription]] = {}
        self._topic_by_subscription_id: dict[str, str] = {}

        self._message_queue: queue.Queue[Message | object] = queue.Queue()
        self._stop_listener = threading.Event()
        self._stop_dispatcher = threading.Event()

        self._listener_thread: threading.Thread | None = None
        self._dispatcher_thread: threading.Thread | None = None

    @property
    def registered_topics(self) -> set[str]:
        """Returns all currently subscribed topics."""
        with self._lock:
            return set(self._subscriptions_by_topic.keys())

    def subscribe(self, topic: str, callback: MessageCallback) -> str:
        """Subscribes callback to a topic and returns a subscription id."""
        sub_id = str(uuid4())
        subscription = Subscription(id=sub_id, topic=topic, callback=callback)

        with self._lock:
            callbacks = self._subscriptions_by_topic.get(topic)
            if callbacks is None:
                self._subscriptions_by_topic[topic] = {sub_id: subscription}
                self._pubsub.subscribe(topic)
            else:
                callbacks[sub_id] = subscription
            self._topic_by_subscription_id[sub_id] = topic
            self._ensure_threads_started()

        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unregisters one callback by subscription id.

        Returns True if a callback was removed, False otherwise.
        """
        with self._lock:
            topic = self._topic_by_subscription_id.pop(subscription_id, None)
            if topic is None:
                return False

            callbacks = self._subscriptions_by_topic.get(topic)
            if callbacks is None:
                return False

            callbacks.pop(subscription_id, None)
            if callbacks:
                return True

            self._subscriptions_by_topic.pop(topic, None)
            self._pubsub.unsubscribe(topic)
            return True

    def unsubscribe_topic(self, topic: str) -> int:
        """Unregisters all callbacks for a topic and returns removed count."""
        with self._lock:
            callbacks = self._subscriptions_by_topic.pop(topic, None)
            if not callbacks:
                return 0
            for sub_id in callbacks:
                self._topic_by_subscription_id.pop(sub_id, None)
            self._pubsub.unsubscribe(topic)
            return len(callbacks)

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Stops worker threads and closes Redis resources."""
        self._stop_listener.set()
        self._stop_dispatcher.set()
        self._message_queue.put(_STOP)

        if self._listener_thread is not None:
            self._listener_thread.join(timeout=timeout)
            self._listener_thread = None

        if self._dispatcher_thread is not None:
            self._dispatcher_thread.join(timeout=timeout)
            self._dispatcher_thread = None

        with self._lock:
            self._subscriptions_by_topic.clear()
            self._topic_by_subscription_id.clear()

        self._pubsub.close()
        self._redis.close()

    def _ensure_threads_started(self) -> None:
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._stop_listener.clear()
            self._listener_thread = threading.Thread(
                target=self._listener_loop, name="redis-pubsub-listener", daemon=True
            )
            self._listener_thread.start()

        if self._dispatcher_thread is None or not self._dispatcher_thread.is_alive():
            self._stop_dispatcher.clear()
            self._dispatcher_thread = threading.Thread(
                target=self._dispatcher_loop, name="redis-pubsub-dispatcher", daemon=True
            )
            self._dispatcher_thread.start()

    def _listener_loop(self) -> None:
        while not self._stop_listener.is_set():
            try:
                message = self._pubsub.get_message(timeout=0.2)
            except redis.exceptions.RedisError:
                logger.exception("Error while polling redis pub/sub")
                self._stop_listener.wait(0.5)
                continue

            if message is None:
                continue

            normalized = self._normalize_message(message)
            self._message_queue.put(normalized)

    def _dispatcher_loop(self) -> None:
        while not self._stop_dispatcher.is_set():
            item = self._message_queue.get()
            if item is _STOP:
                break

            message = item
            if not isinstance(message, Message):
                continue

            topic = message.topic
            with self._lock:
                callbacks = list(self._subscriptions_by_topic.get(topic, {}).values())

            for subscription in callbacks:
                try:
                    subscription.callback(message)
                except Exception:
                    logger.exception(
                        "Error in callback for topic '%s' (subscription %s)", topic, subscription.id
                    )

    @staticmethod
    def _normalize_message(message: dict[str, Any]) -> Message:
        channel = message.get("channel")
        topic = channel.decode() if isinstance(channel, bytes) else str(channel)

        data = message.get("data")
        if isinstance(data, bytes):
            data = data.decode()

        payload: Any
        if isinstance(data, str):
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                payload = data
        else:
            payload = data

        return Message(topic=topic, payload=payload, raw=message)
