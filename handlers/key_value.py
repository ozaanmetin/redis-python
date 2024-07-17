import json
from typing import Any, Dict, Optional
from handlers.interface import IRedis


class RedisKeyValue(IRedis):
    def _key_name(self, key: str) -> str:
        return f'{self._name}:{key}'

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set a key-value pair with an optional TTL."""
        key_name = self._key_name(key)
        self._conn.set(key_name, json.dumps(value))
        if ttl:
            self._conn.expire(key_name, ttl)

    def get(self, key: str) -> Any:
        """Get a value by key."""
        key_name = self._key_name(key)
        byte_data = self._conn.get(key_name)
        return json.loads(byte_data) if byte_data else None

    def get_all(self) -> Dict[str, Any]:
        """Retrieve all key-value pairs."""
        keys = self._conn.keys(f'{self._name}:*')
        return {key.decode('utf-8').split(':')[-1]: self.get(key.decode('utf-8').split(':')[-1]) for key in keys}

    def delete(self, key: str) -> None:
        """Delete a key-value pair."""
        key_name = self._key_name(key)
        self._conn.delete(key_name)

    def clear(self) -> None:
        """Clear the key-value store."""
        keys = self._conn.keys(f'{self._name}:*')
        for key in keys:
            self._conn.delete(key)

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        key_name = self._key_name(key)
        return self._conn.exists(key_name)

    def __str__(self) -> str:
        return f"RedisKeyValue(name={self._name})"
