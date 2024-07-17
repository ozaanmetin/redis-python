import json
from typing import Any, List
from handlers.interface import IRedis


class RedisSet(IRedis):
    def set(self, value: Any) -> None:
        """Add a value to the set."""
        self._conn.sadd(self._name, json.dumps(value))

    def get(self,) -> List[Any]:
        """Retrieve all values from the set."""
        data = self._conn.smembers(self._name)
        return [json.loads(value.decode('utf-8')) for value in data]

    def delete(self, value: Any) -> None:
        """Remove a value from the set."""
        self._conn.srem(self._name, json.dumps(value))

    def clear(self) -> None:
        """Clear the set."""
        self._conn.delete(self._name)

    def exists(self, value: Any) -> bool:
        """Check if a value exists in the set."""
        return self._conn.sismember(self._name, json.dumps(value))
    
    def size(self) -> int:
        """Get the size of the set."""
        return self._conn.scard(self._name)
    
    def is_empty(self) -> bool:
        """Check if the set is empty."""
        return self.size() == 0
    
    def __str__(self) -> str:
        return f"RedisSet(name={self._name})"