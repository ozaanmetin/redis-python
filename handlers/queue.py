import json
from typing import Any, List
from handlers.interface import IRedis


class RedisQueue(IRedis):
    def set(self, value: Any) -> None:
        """Push a value onto the queue."""
        self._conn.rpush(self._name, json.dumps(value))

    def get(self) -> Any:
        """Pop a value from the queue."""
        byte_data = self._conn.lpop(self._name)
        return json.loads(byte_data) if byte_data else None

    def get_many(self, count: int) -> List[Any]:
        """Pop multiple values from the queue."""
        data = []
        for _ in range(count):
            byte_data = self._conn.lpop(self._name)
            if byte_data:
                data.append(json.loads(byte_data))
            else:
                break
        return data

    def get_all(self) -> List[Any]:
        """Retrieve all values from the queue."""
        data = self._conn.lrange(self._name, 0, -1)
        return [json.loads(value.decode('utf-8')) for value in data]
    
    def clear(self) -> None:
        """Clear the queue."""
        self._conn.delete(self._name)

    def size(self) -> int:
        """Get the size of the queue."""
        return self._conn.llen(self._name)

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self.size() == 0

    def __str__(self) -> str:
        return f"RedisQueue(name={self._name})"
    