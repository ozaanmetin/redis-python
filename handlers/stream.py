import redis
from typing import Any
from handlers.interface import IRedis


class RedisStream(IRedis):
    def __init__(
            self, 
            name: str, 
            group_name: str = '',
            consumer_name: str = 'default',
            host: str = 'localhost', 
            port: int = 6379, 
            db: int = 0, 
            password: str = None
        ):
        """
        Initialize the Redis stream handler.

        Args:
            name (str): The name of the stream.
            group_name (str, optional): The consumer group name. Defaults to ''.
            consumer_name (str, optional): The consumer name. Defaults to 'default'.
            host (str, optional): The Redis server host. Defaults to 'localhost'.
            port (int, optional): The Redis server port. Defaults to 6379.
            db (int, optional): The Redis database number. Defaults to 0.
            password (str, optional): The Redis password. Defaults to None.
        """
        super().__init__(name, host, port, db, password)
        self.consumer_name = consumer_name
        self.group_name = group_name
        self._create_consumer_group()
    
    def _create_consumer_group(self):
        try:
            self._conn.xgroup_create(self._name, self.group_name, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP Consumer Group name already exists' in str(e):
                pass
            else:
                raise e
        
    def set(self, value: Any) -> None:
        """Add a message to the stream."""
        message_id = self._conn.xadd(self._name, value)
        return message_id
    
    def get(self, count=1, block=1000) -> Any:
        """Retrieve messages from the stream."""
        messages = self._conn.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self._name: '>'},
            count=count,
            block=block
        )
        data = {
            'stream': self._name,
            'datas': []
        }
        if messages:
            messages = messages[0][1]
            for message_id, message in messages:
                data['datas'].append({
                    'id': message_id,
                    'data': message
                })
                self.ack_message(message_id)
        return data

    def delete(self, id: str) -> None:
        """Delete a message from the stream."""
        self._conn.xdel(self._name, id)

    def clear(self) -> None:
        """Clear the stream."""
        self._conn.delete(self._name)

    def ack_message(self, id: str) -> None:
        """Acknowledge a message."""
        self._conn.xack(self._name, self.group_name, id)

    def __str__(self) -> str:
        return f"RedisStream(name={self._name})"
