import json
from typing import Any, Dict
from handlers.interface import IRedis


class RedisHashSet(IRedis):
    def set(self, key: str, value: Any) -> None:
        """Set a value in the hash set."""
        self._conn.hset(self._name, key, json.dumps(value))

    def get(self, key: str) -> Any:
        """Get a value from the hash set."""
        byte_data = self._conn.hget(self._name, key)
        return json.loads(byte_data) if byte_data else None
    
    def get_all(self) -> Dict[str, Any]:
        """Retrieve all key-value pairs from the hash set."""
        data = self._conn.hgetall(self._name)
        return {key.decode('utf-8'): json.loads(value.decode('utf-8')) for key, value in data.items()}

    def delete(self, key: str) -> None:
        """Delete a key-value pair from the hash set."""
        self._conn.hdel(self._name, key)

    def clear(self) -> None:
        """Clear the hash set."""
        return self._conn.delete(self._name)

    def exists(self, key: str) -> bool:
        """Check if a key exists in the hash set."""
        return self._conn.hexists(self._name, key)

    def __str__(self) -> str:
        return f"RedisHashSet(name={self._name})"
    