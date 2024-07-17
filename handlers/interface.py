import logging
import redis
from redis import exceptions
from abc import ABCMeta, abstractmethod
from typing import Any, Optional


logger = logging.getLogger(__name__)

class IRedis(metaclass=ABCMeta):

    def __init__(
        self, 
        name: str, 
        host: str = 'localhost', 
        port: int = 6379, 
        db: int = 0, 
        password: str = None
    ):
        """
        Initialize the Redis Connection.

        Args:
            name (str): The name of the queue.
            host (str, optional): The Redis server host. Defaults to 'localhost'.
            port (int, optional): The Redis server port. Defaults to 6379.
            db (int, optional): The Redis database number. Defaults to 0.
            password (str, optional): The Redis password. Defaults to None.
        """

        self._name = name
        try:
            self._conn = redis.StrictRedis(
                host=host, 
                port=port, 
                db=db, 
                password=password
            )
        except exceptions.ConnectionError as err:
            logger.error(f'Error connecting to Redis: {err}')
            raise
        except exceptions.AuthenticationError as err:
            logger.error(f'Error authenticating to Redis: {err}')
            raise
        except exceptions.TimeoutError as err:
            logger.error(f'Timeout while connecting to Redis: {err}')
            raise


    @abstractmethod
    def get(self, key: str) -> Any:
        """Retrieve the value associated with the given key."""
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set the value for the given key, with an optional TTL."""
        raise NotImplementedError
    