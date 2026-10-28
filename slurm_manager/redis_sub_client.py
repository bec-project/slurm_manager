"""
Thin Redis Client to register callbacks to a pub/sub interface.
The client is not intended to publish messages itself, but only
subscribe to topics and dispatch received messages to registered callbacks.
"""

from __future__ import annotations

import logging
import threading
import time
import traceback
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
        """Create Redis resources without starting the listener until subscribed.

        Args:
            bootstrap (str | list[str]): Redis host:port or a nonempty address list;
                only the first entry is used.
            redis_cls (type[Redis]): Redis client implementation, injectable for tests.
            **redis_kwargs (Any): Additional Redis constructor options such as socket
                timeouts.

        Raises:
            ValueError: The bootstrap list is empty or its address cannot be parsed.
        """
        if isinstance(bootstrap, list):
            if not bootstrap:
                raise ValueError("bootstrap list must not be empty")
            host, port = bootstrap[0].split(":", maxsplit=1)
        else:
            host, port = bootstrap.split(":", maxsplit=1)

        self._redis = redis_cls(host=host, port=int(port), **redis_kwargs)
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=False)

        self._lock = threading.RLock()
        self._subscriptions_by_topic: dict[str, dict[str, Subscription]] = {}
        self._topic_by_subscription_id: dict[str, str] = {}

        self._subscription_ready: dict[str, threading.Event] = {}
        self._stop_listener = threading.Event()
        self._listener_thread: threading.Thread | None = None

    ################################################
    #  Public Subscriptions                        #
    ################################################

    @property
    def registered_topics(self) -> set[str]:
        """Return a snapshot of topics with active callbacks.

        Returns:
            set[str]: Currently registered Redis channel names.
        """
        with self._lock:
            return set(self._subscriptions_by_topic.keys())

    def subscribe(self, topic: str, callback: MessageCallback) -> str:
        """Register a callback and start the listener if needed.

        Args:
            topic (str): Redis channel whose callback registrations are selected.
            callback (MessageCallback): Callable receiving the parsed Slurm message.

        Returns:
            str: Unique subscription ID.

        Notes:
            The callback runs on the Redis listener thread and must remain fast and
            nonblocking.
        """
        sub_id = str(uuid4())
        subscription = Subscription(id=sub_id, topic=topic, callback=callback)

        with self._lock:
            callbacks = self._subscriptions_by_topic.get(topic)
            if callbacks is None:
                self._subscription_ready[topic] = threading.Event()
                self._pubsub.subscribe(topic)
                self._subscriptions_by_topic[topic] = {sub_id: subscription}
            else:
                callbacks[sub_id] = subscription
            self._topic_by_subscription_id[sub_id] = topic
            self._ensure_threads_started()

        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove one callback and unsubscribe its topic if it has no owners.

        Args:
            subscription_id (str): Unique callback ID returned by subscribe().

        Returns:
            bool: True if the callback was registered; False if it was unknown.
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
            self._subscription_ready.pop(topic, None)
            self._pubsub.unsubscribe(topic)
            return True

    def unsubscribe_topic(self, topic: str) -> int:
        """Remove every callback registered for one topic.

        Args:
            topic (str): Redis channel whose callback registrations are selected.

        Returns:
            int: Number of removed callbacks.
        """
        with self._lock:
            callbacks = self._subscriptions_by_topic.pop(topic, None)
            if not callbacks:  # No callbacks for this topic, nothing happens
                return 0
            self._subscription_ready.pop(topic, None)
            for sub_id in callbacks:  # Clean up subscription id mapping
                self._topic_by_subscription_id.pop(sub_id, None)
            self._pubsub.unsubscribe(topic)  # Unsubscribe from Redis
            return len(callbacks)

    def wait_until_subscribed(self, topics: list[str], timeout: float = 5.0) -> None:
        """Wait for Redis subscription acknowledgements before releasing a job.

        Args:
            topics (list[str]): Already registered channels whose acknowledgements must
                be received.
            timeout (float): Shared acknowledgement deadline in seconds across all
                requested topics.

        Raises:
            KeyError: A requested topic is not registered.
            TimeoutError: The shared deadline expires before all topics are
                acknowledged.
        """
        deadline = time.monotonic() + timeout
        with self._lock:
            ready = [(topic, self._subscription_ready[topic]) for topic in topics]
        for topic, event in ready:
            if not event.wait(max(0, deadline - time.monotonic())):
                raise TimeoutError(f"Redis did not acknowledge subscription to {topic}")

    ################################################
    #  Public Lifecycle                            #
    ################################################

    def shutdown(self, timeout: float | None = 1.0) -> None:
        """Stop the listener and close Redis resources.

        Args:
            timeout (float | None): Listener join timeout in seconds; None waits
                indefinitely.

        Notes:
            A listener that misses the join timeout is logged before resources are
            closed.
        """
        self._stop_listener.set()

        if self._listener_thread is not None:
            self._listener_thread.join(timeout=timeout)
            if self._listener_thread.is_alive():
                logger.warning("Listener thread did not stop within timeout")

        with self._lock:
            self._subscriptions_by_topic.clear()
            self._topic_by_subscription_id.clear()
            self._subscription_ready.clear()

        self._pubsub.close()
        self._redis.close()

    ################################################
    #  Internal Listener                           #
    ################################################

    def _ensure_threads_started(self) -> None:
        """Start a listener if no listener thread is currently alive."""
        if self._listener_thread is None or not self._listener_thread.is_alive():
            self._stop_listener.clear()
            self._listener_thread = threading.Thread(
                target=self._listener_loop, name="redis-pubsub-listener", daemon=True
            )
            self._listener_thread.start()

    def _listener_loop(self) -> None:
        """Poll Redis, acknowledge subscriptions, and dispatch parsed messages.

        Notes:
            Redis errors are logged and polling resumes after a short delay. Callback
            exceptions are logged without stopping other callbacks.
        """
        while not self._stop_listener.is_set():
            try:
                with self._lock:
                    raw_message = self._pubsub.get_message(timeout=0.05)
                self._stop_listener.wait(0.001)
            except RedisError:
                logger.error("Error while polling redis pub/sub")
                self._stop_listener.wait(0.5)
                continue

            if raw_message is None:
                continue
            if raw_message["type"] == "subscribe":
                channel = raw_message["channel"]
                topic = channel.decode() if isinstance(channel, bytes) else channel
                with self._lock:
                    event = self._subscription_ready.get(topic)
                    if event is not None:
                        event.set()
                continue
            if raw_message["type"] == "unsubscribe":
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
                    format_exc = traceback.format_exc()
                    logger.error(
                        "Error in callback for topic %s (subscription %s) for callback %s. Traceback: %s",
                        topic,
                        subscription.id,
                        subscription.callback,
                        format_exc,
                    )

    ################################################
    #  Internal Message Parsing                    #
    ################################################

    @staticmethod
    def _normalize_message(message: dict[str, Any]) -> tuple[str, SlurmMessage]:
        """Decode a Redis pub/sub payload into the corresponding message model.

        Args:
            message (dict[str, Any]): Raw Redis payload or parsed worker message, as
                specified by the annotation.

        Returns:
            tuple[str, SlurmMessage]: Pair containing the channel name and validated
                Slurm message.

        Raises:
            TypeError: The payload is neither JSON text nor a dictionary.
            ValueError: The JSON or message schema is invalid.
        """
        channel = message.get("channel")
        topic = channel.decode() if isinstance(channel, bytes) else str(channel)

        data = message.get("data")
        if isinstance(data, bytes):
            data = data.decode()

        if not isinstance(data, (str, dict)):
            raise TypeError(f"Unsupported redis message payload type: {type(data).__name__}")

        payload = parse_slurm_message(data)

        return topic, payload
