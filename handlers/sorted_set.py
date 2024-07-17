import json
from typing import Any, List
from handlers.interface import IRedis


class RedisSortedSet(IRedis):
    def set(self, value: Any, score: float) -> None:
        """Add a value with a score to the sorted set."""
        self._conn.zadd(self._name, {json.dumps(value): float(score)})

    def get(self, value: Any) -> Any:
        """Get the score of a value."""
        score = self._conn.zscore(self._name, json.dumps(value))
        return score
    
    def get_by_score(self, start: int = 0, end: int = -1) -> List[Any]:
        """Retrieve values within a score range."""
        data = self._conn.zrange(self._name, start, end)
        return [json.loads(value.decode('utf-8')) for value in data]
    
    def delete(self, value: Any) -> None:
        """Remove a value from the sorted set."""
        self._conn.zrem(self._name, json.dumps(value))

    def clear(self) -> None:
        """Clear the sorted set."""
        self._conn.delete(self._name)

    def exists(self, value: Any) -> bool:
        """Check if a value exists in the sorted set."""
        return self._conn.zscore(self._name, json.dumps(value)) is not None
    
    def get_all(self) -> Any:
        """Retrieve all values from the sorted set."""
        data = self._conn.zrange(self._name, 0, -1)
        return [json.loads(value.decode('utf-8')) for value in data]
    
    def __str__(self) -> str:
        return f"RedisSortedSet(name={self._name})"