import json
from typing import Any, List
from handlers.interface import IRedis


class RedisList(IRedis):
    def set(self, value: Any) -> None:
        """Append a value to the list."""
        self._conn.rpush(self._name, json.dumps(value))

    def get(self, index: int) -> Any:
        """Get a value by index."""
        byte_data = self._conn.lindex(self._name, index)
        return json.loads(byte_data) if byte_data else None
    
    def delete(self, value: Any) -> None:
        """Remove a value from the list."""
        self._conn.lrem(self._name, 0, json.dumps(value))

    def clear(self) -> None:
        """Clear the list."""
        self._conn.delete(self._name)

    def get_all(self) -> List[Any]:
        """Retrieve all values from the list."""
        data = self._conn.lrange(self._name, 0, -1)
        return [json.loads(value.decode('utf-8')) for value in data]

    def exists(self) -> bool:
        """Check if the list exists."""
        return self._conn.exists(self._name)

    def size(self) -> int:
        """Get the size of the list."""
        return self._conn.llen(self._name)
        
    def contains(self, value: Any) -> bool:
        """Check if the list contains a value."""
        return self.count(value) > 0
    
    def count(self, value: Any) -> int:
        """Count occurrences of a value in the list."""
        elements = self.get_all()
        return elements.count(value)

    def __str__(self) -> str:
        return f"RedisList(name={self._name})"