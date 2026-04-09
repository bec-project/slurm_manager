"""
Thin Redis Client to register callbacks to a pub/sub interface.
The client is not intended to publish messages itself, but only
subscribe to topics and dispatch received messages to registered callbacks.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

from redis.client import Redis
from redis.exceptions import RedisError

from slurm_manager.event_messages import SlurmMessage, parse_slurm_message

logger = logging.getLogger(__name__)


Message = SlurmMessage
MessageCallback = Callable[[SlurmMessage], None]


@dataclass(frozen=True, slots=True)
class Subscription:
    """Represents one registered callback for one topic."""

    id: str
    topic: str
    callback: MessageCallback


class RedisSubClient:
    """

    Simple subscribe client to a Redis instance.

    The client maintains a single listener thread that polls for new messages
    and executes callbacks registered for the topic of each message.
    Callbacks are executed in the listener thread, so they should be fast and
    non-blocking to avoid any delays in message processing.
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

        self._stop_listener = threading.Event()
        self._listener_thread: threading.Thread | None = None

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
        """
        Unregisters one callback by subscription id.

        Returns True if a callback was removed, False otherwise.
        """
        with self._lock:
            topic = self._topic_by_subscription_id.pop(subscription_id, None)
            if topic is None:  # Topic not found
                return False

            callbacks = self._subscriptions_by_topic.get(topic)
            if callbacks is None:  # No callbacks for topic, should technically be impossible
                return False

            callbacks.pop(subscription_id, None)
            if callbacks:  # There are still callbacks for this topic, so we keep the subscription
                return True

            # No more callbacks for this topic, we can unsubscribe from Redis and clean up
            self._subscriptions_by_topic.pop(topic, None)
            self._pubsub.unsubscribe(topic)
            return True

    def unsubscribe_topic(self, topic: str) -> int:
        """Unregisters all callbacks for a topic and returns removed count."""
        with self._lock:
            callbacks = self._subscriptions_by_topic.pop(topic, None)
            if not callbacks:  # No callbacks for this topic, nothing happens
                return 0
            for sub_id in callbacks:  # Clean up subscription id mapping
                self._topic_by_subscription_id.pop(sub_id, None)
            self._pubsub.unsubscribe(topic)  # Unsubscribe from Redis
            return len(callbacks)

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Stops worker threads and closes Redis resources."""
        self._stop_listener.set()

        if self._listener_thread is not None:
            self._listener_thread.join(timeout=timeout)
            if self._listener_thread.is_alive():
                logger.warning("Listener thread did not stop within timeout")

        with self._lock:
            self._subscriptions_by_topic.clear()
            self._topic_by_subscription_id.clear()

        self._pubsub.close()
        self._redis.close()

    def _ensure_threads_started(self) -> None:
        """Makes sure the listener thread is running to receive messages from Redis."""
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._stop_listener.clear()
            self._listener_thread = threading.Thread(
                target=self._listener_loop, name="redis-pubsub-listener", daemon=True
            )
            self._listener_thread.start()

    def _listener_loop(self) -> None:
        """Listener thread loop that polls for messages and dispatches them to callbacks."""
        while not self._stop_listener.is_set():
            try:
                raw_message = self._pubsub.get_message(timeout=0.2)
            except RedisError:
                logger.error("Error while polling redis pub/sub")
                self._stop_listener.wait(0.5)
                continue

            if raw_message is None:
                continue

            try:
                topic, message = self._normalize_message(raw_message)
            except (ValueError, TypeError) as exc:
                logger.error("Failed to normalize redis message: %s", exc)
                continue

            with self._lock:
                callbacks = list(self._subscriptions_by_topic.get(topic, {}).values())
            for subscription in callbacks:
                try:
                    subscription.callback(message)
                except Exception:
                    logger.error(
                        "Error in callback for topic %s (subscription %s)", topic, subscription.id
                    )

    @staticmethod
    def _normalize_message(message: dict[str, Any]) -> tuple[str, SlurmMessage]:
        """Converts raw Redis message dict to normalized Message object."""
        channel = message.get("channel")
        topic = channel.decode() if isinstance(channel, bytes) else str(channel)

        data = message.get("data")
        if isinstance(data, bytes):
            data = data.decode()

        if not isinstance(data, (str, dict)):
            raise TypeError(f"Unsupported redis message payload type: {type(data).__name__}")

        payload = parse_slurm_message(data)

        return topic, payload
