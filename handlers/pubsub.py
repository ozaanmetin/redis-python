import json
import redis
from typing import Any, Callable


class RedisPubSub:
    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0, password: str = None):
        """
        Initialize the Redis Pub/Sub handler.

        Args:
            host (str, optional): The Redis server host. Defaults to 'localhost'.
            port (int, optional): The Redis server port. Defaults to 6379.
            db (int, optional): The Redis database number. Defaults to 0.
            password (str, optional): The Redis password. Defaults to None.
        """
        self._conn = redis.StrictRedis(host=host, port=port, db=db, password=password)
        self._pubsub = self._conn.pubsub()

    def subscribe(self, channel: str, callback: Callable[[Any], None]) -> None:
        """Subscribe to a channel."""
        self._pubsub.subscribe(**{channel: callback})
        self._pubsub.run_in_thread(sleep_time=0.001)

    def publish(self, channel: str, message: Any) -> None:
        """Publish a message to a channel."""
        self._conn.publish(channel, json.dumps(message))

    def __str__(self) -> str:
        return f"RedisPubSub()"