import json
from typing import Any, List
from .interface import IRedis


class RedisStack(IRedis):
    def set(self, value: Any) -> None:
        """Push a value onto the stack."""
        self._conn.rpush(self._name, json.dumps(value))

    def get(self) -> Any:
        """Pop a value from the stack."""
        byte_data = self._conn.rpop(self._name)
        return json.loads(byte_data) if byte_data else None

    def get_many(self, count: int) -> List[Any]:
        """Pop multiple values from the stack."""
        data = []
        for _ in range(count):
            byte_data = self._conn.rpop(self._name)
            if byte_data:
                data.append(json.loads(byte_data))
            else:
                break
        return data

    def get_all(self) -> List[Any]:
        """Retrieve all values from the stack."""
        data = self._conn.lrange(self._name, 0, -1)
        return [json.loads(value.decode('utf-8')) for value in data]
    
    def clear(self) -> None:
        """Clear the stack."""
        self._conn.delete(self._name)
    
    def size(self) -> int:
        """Get the size of the stack."""
        return self._conn.llen(self._name)

    def is_empty(self) -> bool:
        """Check if the stack is empty."""
        return self.size() == 0

    def __str__(self) -> str:
        return f"RedisStack(name={self._name})"